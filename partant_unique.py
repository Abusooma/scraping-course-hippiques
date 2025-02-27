import re
import sys
import csv
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
from unidecode import unidecode

from loguru import logger
from selectolax.parser import HTMLParser
import asyncio
import aiohttp


BASE_URL = "https://www.geny.com/"

URLS_UNIQUES_PARTANTS = [
    "https://www.geny.com/partants-pmu/2024-09-24-vincennes-pmu-prix-hekate_c1521138"
]


def configurer_logger():
    """Configure le logger avec les paramètres appropriés."""
    logger.remove()
    fichier_log = "log_partants.log"
    logger.add(fichier_log, rotation="500 KB",
               retention="3 days", level="WARNING")
    logger.add(sys.stderr, level="INFO")


async def recuperer_les_urls(url: str) -> List[str]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                texte_html = await response.text(encoding='utf-8')
                arbre = HTMLParser(texte_html)
                urls_node = arbre.css('a[accesskey]')
                urls = [url]
                urls += [urljoin(BASE_URL, url.attributes.get('href'))
                         for url in urls_node if 'href' in url.attributes]

                return urls

    except Exception as e:
        logger.error(f"Erreur HTTP survenue pour l'URL {url}: {e}")
        return []


async def traiter_liste_urls(liste_urls: List[str]) -> List[str]:
    resultats = []

    async def recuperer_url(url: str):
        urls_extraites = await recuperer_les_urls(url)
        resultats.extend(urls_extraites)

    taches = [recuperer_url(url) for url in liste_urls]
    await asyncio.gather(*taches)

    return resultats


async def contient_attele_ou_monte(url: str, session: aiohttp.ClientSession) -> bool:
    try:
        async with session.get(url) as response:
            text_html = await response.text(encoding='utf-8')

        arbre = HTMLParser(text_html)
        info_course = arbre.css_first('span.infoCourse')

        if info_course:
            result = info_course.text(deep=True, separator='\n').strip()
            pattern = r'\b(Mont[ée]|Attel[ée])\b'
            match = re.search(pattern, result, re.MULTILINE)

            if match:
                return True

            return False

        else:
            print(
                f"Span avec la classe 'infoCourse' non trouvé dans l'url: {url}")
            return False

    except Exception as e:
        logger.error(f"Erreur lors de la requete HTTP pour l'URL {url}: {e}")
        return False


def normaliser_nom_hippodrome(nom_hippodrome: str) -> str:
    """Normalise le nom de l'hippodrome pour faciliter la correspondance."""
    nom_normalise = unidecode(nom_hippodrome.upper().strip())
    nom_normalise = nom_normalise.replace("-", " ")
    nom_normalise = " ".join(nom_normalise.split())
    return nom_normalise


def extraire_date_de_url(url: str) -> str:
    """Extrait et formate la date à partir de l'URL donnée."""
    correspondance_date = re.search(r'/(\d{4}-\d{2}-\d{2})-', url)
    if correspondance_date:
        chaine_date = correspondance_date.group(1)
        objet_date = datetime.strptime(chaine_date, '%Y-%m-%d')
        return objet_date.strftime('%d/%m/%Y')
    logger.warning(f"Date non trouvée dans l'URL: {url}")
    return ""


def extraire_hippodrome(arbre: HTMLParser) -> Optional[str]:
    try:
        container_hippodrome = arbre.css('#yui-main a')
        if container_hippodrome is None:
            raise ValueError("Le div contenant l'hippodrome n'est pas trouvé")

        hippodrome = container_hippodrome[1].text(strip=True)
        return normaliser_nom_hippodrome(hippodrome)

    except IndexError as e:
        logger.error(f' Le container hippodrome ne contient pas de valeur')
        return None
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction de l'hippodrome : {e}")
        return None


def extraire_numero_course(arbre: HTMLParser) -> Optional[int]:
    """Extrait le numéro de course du HTML."""
    try:
        noeud_numero_course = arbre.css_first("span h1")
        if noeud_numero_course:
            numero_course_text = noeud_numero_course.text().strip()
        else:
            logger.error("Aucun noeud correpondant au Numero de course")
            return None

        return numero_course_text[0]

    except IndexError as e:
        logger.error("Erreur d'accès au numero de course dans le texte")
        return None
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction du numéro de course : {e}")
        return None


def extraire_prix_et_partants(arbre: HTMLParser) -> Tuple[Optional[int], Optional[int]]:
    """Extrait le prix et le nombre de partants du HTML."""
    try:
        noeud_info_course = arbre.css_first("span.infoCourse")
        if noeud_info_course:
            texte_info_course = noeud_info_course.text().replace("ï¿½", "")
        else:
            logger.error(
                "Aucun element avec l'attribut 'span.infoCourse' trouvé lors de l'extraction du 'prix' et 'partants'")
            return None, None

        correspondance_prix = re.search(
            r'(\d+(?:\s\d+)?)\s*(?:000)?€', texte_info_course)
        correspondance_partants = re.search(
            r'-\s*(\d+)\s*Partants', texte_info_course)

        if correspondance_prix and correspondance_partants:
            prix_str = correspondance_prix.group(1).replace(" ", "")
            prix_str_nettoye = re.sub(r'[^\x20-\x7E]', '', prix_str)
            prix = int(prix_str_nettoye)/1000
            if prix.is_integer():
                prix = int(prix)
            partants = correspondance_partants.group(1).replace(' ', '')
            return prix, partants
        else:
            logger.error(
                "Le prix ou le partant n'a pas été trouvé dans le texte lors de l'extraction")
            return None, None
    except Exception as e:
        logger.error(
            f"Erreur lors de l'extraction du prix et des partants : {e}")
        return None, None


def extraire_chevaux_et_gains(arbre: HTMLParser) -> List[Dict[str, str]]:
    donnees_chevaux = []
    try:
        tableau = arbre.css_first('table#tableau_partants')
        if not tableau:
            logger.error("Tableau des partants non trouvé")
            return donnees_chevaux

        headers = tableau.css('thead th')
        gains_index = next((i for i, header in enumerate(
            headers) if header.text().strip() == "Gains"), None)

        cotes_pmu_index = next((i for i, header in enumerate(
            headers) if "Cotes" in header.text().strip()), None)

        cotes_genybet_index = cotes_pmu_index + \
            1 if cotes_pmu_index is not None else None

        if gains_index is None or cotes_pmu_index is None or cotes_genybet_index is None:
            logger.error(
                "Colonne 'Gains', 'Cotes' ou 'Genybet' non trouvée dans le tableau")
            return donnees_chevaux

        lignes = tableau.css('tbody tr')
        for ligne in lignes:
            try:
                nom_cheval = ligne.css_first('span.leftWidth100 a.lienFiche')
                gain = ligne.css(f'td:nth-child({gains_index + 1})')
                cote_pmu = ligne.css(f'td:nth-child({cotes_pmu_index + 1})')
                cote_genybet = ligne.css(
                    f'td:nth-child({cotes_genybet_index + 1})')

                if nom_cheval and gain and cote_pmu and cote_genybet:
                    nom = nom_cheval.text().strip()
                    gain_texte = gain[0].text().strip() if gain else ""
                    cote_pmu_texte = cote_pmu[0].text().strip()
                    cote_genybet_texte = cote_genybet[0].text().strip()

                    cote_pmu_texte = re.sub(
                        r'[^\x20-\x7E]', '', cote_pmu_texte)
                    cote_pmu_texte = cote_pmu_texte.replace('-', "")
                    cote_pmu_texte = cote_pmu_texte.strip(
                        '"').replace(',', '.')

                    cote_genybet_texte = re.sub(
                        r'[^\x20-\x7E]', '', cote_genybet_texte)
                    cote_genybet_texte = cote_genybet_texte.replace('-', "")
                    cote_genybet_texte = cote_genybet_texte.strip(
                        '"').replace(',', '.')

                    if nom and (gain_texte or gain_texte == "") and (cote_pmu_texte or cote_pmu_texte == "") and (cote_genybet_texte or cote_genybet_texte == ""):
                        if gain_texte == "":
                            gain_texte = '0'
                        if cote_pmu_texte == "":
                            cote_pmu_texte = '0'
                        if cote_genybet_texte == "":
                            cote_genybet_texte = '0'
                        donnees_chevaux.append({
                            "nom": nom,
                            "gain": gain_texte,
                            "cote_pmu": cote_pmu_texte,
                            "cote_genybet": cote_genybet_texte
                        })
            except AttributeError as e:
                logger.error(
                    f"Erreur lors de l'extraction des données du cheval : {e}")

    except Exception as e:
        logger.error(f"Erreur générale lors de l'analyse : {e}")

    return donnees_chevaux


def charger_donnees_excel(chemin_fichier: str) -> pd.DataFrame:
    """Charge les données du fichier Excel (.xls ou .xlsx) et les prépare pour la correspondance."""
    try:
        engine = 'xlrd' if chemin_fichier.endswith('.xls') else None
        df = pd.read_excel(chemin_fichier, engine=engine)
        df['Hippodrome'] = df['Hippodrome'].apply(normaliser_nom_hippodrome)
        return df
    except Exception as e:
        logger.error(f"Erreur lors du chargement du fichier Excel : {e}")
        return pd.DataFrame()


async def extraire_donnees(url: str, session: aiohttp.ClientSession) -> Dict[str, any]:
    """Extrait les données de l'URL donnée de manière asynchrone."""
    try:
        async with session.get(url) as response:
            texte_html = await response.text(encoding='utf-8')
        arbre = HTMLParser(texte_html)

        date = extraire_date_de_url(url)
        hippodrome = extraire_hippodrome(arbre)
        numero_course = extraire_numero_course(arbre)
        prix, partants = extraire_prix_et_partants(arbre)
        donnees_chevaux = extraire_chevaux_et_gains(arbre)

        return {
            "date": date,
            "hippodrome": hippodrome,
            "numero_course": numero_course,
            "prix": prix,
            "partants": partants,
            "donnees_chevaux": donnees_chevaux
        }
    except Exception as e:
        logger.error(f"Erreur HTTP survenue pour l'URL {url}: {e}")
        return {}


def calculer_gains_min_max(donnees_chevaux: List[Dict[str, str]]) -> Tuple[int, int]:
    """Calcule les gains minimum et maximum parmi les chevaux."""
    gains = []
    for cheval in donnees_chevaux:
        gain_str = cheval['gain'].replace(' ', '').replace('€', '')
        try:
            gain = int(gain_str)
            gains.append(gain)
        except ValueError:
            logger.warning(
                f"Impossible de convertir le gain en entier: {cheval['gain']}")

    if gains:
        return min(gains), max(gains)
    return 0, 0


def sauvegarder_en_csv(toutes_donnees: List[Dict[str, any]], nom_fichier: str, donnees_excel: pd.DataFrame):
    noms_champs = ['DATE', 'Hippodrome', 'COURSE', 'NumChev', 'CHEVAL',
                   'PLACE', 'RAP-G', 'RAP-P', 'PARTANTS', 'I-Gains', 'I-Prix du jour',
                   'I-Moins-Riche', 'I-Plus-Riche', 'Cotes-Pmu', 'Statut',
                   'L1', 'L2', 'D-P', 'D-C', 'D-N', 'D-L', 'D-B', 'D-C2', 'A']

    try:
        with open(nom_fichier, 'w', newline='', encoding='utf-8-sig') as fichier_csv:
            ecrivain = csv.DictWriter(fichier_csv, fieldnames=noms_champs)
            ecrivain.writeheader()

            for donnees in toutes_donnees:
                moins_riche, plus_riche = calculer_gains_min_max(
                    donnees['donnees_chevaux'])

                hippodrome_norm = normaliser_nom_hippodrome(
                    donnees['hippodrome'])
                donnees_hippodrome = donnees_excel[donnees_excel['Hippodrome']
                                                   == hippodrome_norm]

                valeurs_excel = {col: '0' for col in [
                    'L1', 'L2', 'D-P', 'D-C', 'D-N', 'D-L', 'D-B', 'D-C2', 'A']}

                if not donnees_hippodrome.empty:
                    for col in valeurs_excel.keys():
                        valeur = donnees_hippodrome.iloc[0].get(col, '0')
                        valeurs_excel[col] = '0' if pd.isna(
                            valeur) else str(valeur)

                cotes_pmu_zero = sum(
                    1 for cheval in donnees['donnees_chevaux'] if cheval['cote_pmu'] == '0')

                utiliser_genybet = cotes_pmu_zero >= 5

                for i, cheval in enumerate(donnees['donnees_chevaux'], start=1):
                    cote = cheval['cote_genybet'] if utiliser_genybet else cheval['cote_pmu']

                    if utiliser_genybet:
                        cote = f"(G) {cote}"

                    ligne = {
                        'DATE': donnees['date'],
                        'Hippodrome': donnees['hippodrome'],
                        'COURSE': donnees['numero_course'],
                        'NumChev': i,
                        'CHEVAL': cheval['nom'],
                        'PLACE': '',
                        'RAP-G': '',
                        'RAP-P': '',
                        'PARTANTS': donnees['partants'],
                        'I-Gains': cheval['gain'],
                        'I-Prix du jour': donnees['prix'],
                        'I-Moins-Riche': moins_riche,
                        'I-Plus-Riche': plus_riche,
                        'Cotes-Pmu': cote,
                        'Statut': '',
                        **valeurs_excel
                    }
                    ecrivain.writerow(ligne)
        logger.info(
            f"Données enrichies sauvegardées avec succès dans {nom_fichier}")
    except Exception as e:
        logger.error(
            f"Erreur lors de la sauvegarde des données enrichies en CSV : {e}")


async def traiter_urls(urls: List[str]) -> List[Dict[str, any]]:
    """Traite une liste d'URLs de manière asynchrone."""
    async with aiohttp.ClientSession() as session:
        taches = []
        for url in urls:
            est_attele_ou_monte = await contient_attele_ou_monte(url, session)
            if est_attele_ou_monte:
                tache = asyncio.create_task(extraire_donnees(url, session))
                taches.append(tache)

        resultats = await asyncio.gather(*taches)

    return [resultat for resultat in resultats if resultat]


async def main():
    """Fonction principale pour exécuter l'extracteur et enrichir les données."""
    configurer_logger()

    if not URLS_UNIQUES_PARTANTS:
        logger.info(
            "Veuillez Entrer au moins une URL dans la liste 'URLS_UNIQUES_PARTANTS' ")
        return

    urls = await traiter_liste_urls(URLS_UNIQUES_PARTANTS)

    toutes_donnees = await traiter_urls(urls)

    if toutes_donnees:
        donnees_excel = charger_donnees_excel("FichierH.xls")
        sauvegarder_en_csv(
            toutes_donnees, "donnees_courses_partants.csv", donnees_excel)
    else:
        logger.error("Aucune donnée extraite, fichier CSV non créé")


if __name__ == '__main__':
    asyncio.run(main())
