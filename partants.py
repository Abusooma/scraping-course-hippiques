import re
import sys
import csv
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import pandas as pd
from unidecode import unidecode

from loguru import logger
from selectolax.parser import HTMLParser
import asyncio
import aiohttp


def configurer_logger():
    """Configure le logger avec les paramètres appropriés."""
    fichier_log = "log_partants.log"
    logger.add(fichier_log, rotation="500 KB",
               retention="3 days", level="WARNING")
    logger.add(sys.stderr, level="INFO")


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
    """Extrait le nom de l'hippodrome du HTML en préservant les accents."""
    try:
        noeud_hippodrome = arbre.css_first("div.nomReunion")
        if noeud_hippodrome is None:
            raise ValueError("Nœud 'div.nomReunion' non trouvé")

        texte_hippodrome = noeud_hippodrome.text().strip()
        if not texte_hippodrome:
            raise ValueError("Le texte de l'hippodrome est vide")

        match = re.search(r':\s*(.+?)\s*\(', texte_hippodrome)
        if match:
            hippodrome = match.group(1).strip()
        else:
            parts = texte_hippodrome.split(':')
            if len(parts) > 1:
                hippodrome = parts[1].split('(')[0].strip()
            else:
                hippodrome = texte_hippodrome

        hippodrome_nettoye = re.sub(r'[^A-Za-zÀ-ÿ0-9\s-]', '', hippodrome)
        hippodrome_nettoye = ' '.join(hippodrome_nettoye.split())

        if not hippodrome_nettoye:
            raise ValueError("Le nom de l'hippodrome est vide après nettoyage")

        return hippodrome_nettoye

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
            logger.error("Aucun element avec l'attribut 'span.infoCourse' trouvé lors de l'extraction du 'prix' et 'partants'")
            return None, None

        correspondance_prix = re.search(r'(\d+(?:\s\d+)?)\s*(?:000)?€', texte_info_course)
        correspondance_partants = re.search(r'-\s*(\d+)\s*Partants', texte_info_course)

        if correspondance_prix and correspondance_partants:
            prix_str = correspondance_prix.group(1).replace(" ", "")
            prix_str_nettoye = re.sub(r'[^\x20-\x7E]', '', prix_str)
            prix = int(prix_str_nettoye)/1000
            if prix.is_integer():
                prix = int(prix)
            partants = correspondance_partants.group(1).replace(' ', '')
            return prix, partants
        else:
            logger.error("Le prix ou le partant n'a pas été trouvé dans le texte lors de l'extraction")
            return None, None
    except Exception as e:
        logger.error(
            f"Erreur lors de l'extraction du prix et des partants : {e}")
        return None, None


def extraire_chevaux_et_gains(arbre: HTMLParser) -> List[Dict[str, str]]:
    """Extrait les chevaux, leurs gains et leurs cotes PMU du HTML."""
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

        if gains_index is None or cotes_pmu_index is None:
            logger.error(
                "Colonne 'Gains' ou colonne des cotes non trouvée dans le tableau")
            return donnees_chevaux

        lignes = tableau.css('tbody tr')
        for ligne in lignes:
            try:
                nom_cheval = ligne.css_first('span.leftWidth100 a.lienFiche')
                gain = ligne.css(f'td:nth-child({gains_index + 1})')
                cote_pmu = ligne.css(f'td:nth-child({cotes_pmu_index + 1})')

                if nom_cheval and gain and cote_pmu:
                    nom = nom_cheval.text().strip()
                    gain_texte = gain[0].text().strip() if gain else ""
                  
                    cote_pmu_texte = cote_pmu[0].text().strip()
                    cote_pmu_texte = re.sub(r'[^\x20-\x7E]', '', cote_pmu_texte)
                    cote_pmu_texte = cote_pmu_texte.replace('-', "")
                    cote_pmu_texte = cote_pmu_texte.strip('"').replace(',', '.')
                 
                    if nom and (gain_texte or gain_texte == "") and (cote_pmu_texte or cote_pmu_texte == ""):
                        if gain_texte == "":
                            gain_texte = '0'
                        if cote_pmu_texte == "":
                            cote_pmu_texte = '0'
                        donnees_chevaux.append({
                            "nom": nom,
                            "gain": gain_texte,
                            "cote_pmu": cote_pmu_texte
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
        df['Hippodrome'] = df['Hippodrome'].apply(lambda x: unidecode(str(x)).upper())
        return df
    except Exception as e:
        logger.error(f"Erreur lors du chargement du fichier Excel : {e}")
        return pd.DataFrame()


# ... (autres fonctions inchangées) ...

def extraire_identifiant_unique(donnees: Dict[str, any]) -> str:
    """Crée un identifiant unique pour la course à partir des données extraites."""
    return f"{donnees['date']}_{donnees['hippodrome']}_{donnees['numero_course']}"

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

        donnees = {
            "date": date,
            "hippodrome": hippodrome,
            "numero_course": numero_course,
            "prix": prix,
            "partants": partants,
            "donnees_chevaux": donnees_chevaux,
        }
        donnees["identifiant_unique"] = extraire_identifiant_unique(donnees)
        return donnees
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
            logger.warning(f"Impossible de convertir le gain en entier: {cheval['gain']}")

    if gains:
        return min(gains), max(gains)
    return 0, 0


def sauvegarder_en_csv(toutes_donnees: List[Dict[str, any]], nom_fichier: str, donnees_excel: pd.DataFrame):
    """Sauvegarde les données extraites dans un fichier CSV avec des champs enrichis."""
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

                hippodrome_norm = unidecode(donnees['hippodrome']).upper()
                donnees_hippodrome = donnees_excel[donnees_excel['Hippodrome']
                                                   == hippodrome_norm]

                valeurs_excel = {col: '0' for col in [
                    'L1', 'L2', 'D-P', 'D-C', 'D-N', 'D-L', 'D-B', 'D-C2', 'A']}

                if not donnees_hippodrome.empty:
                    for col in valeurs_excel.keys():
                        valeur = donnees_hippodrome.iloc[0].get(col, '0')
                        valeurs_excel[col] = '0' if pd.isna(
                            valeur) else str(valeur)

                for i, cheval in enumerate(donnees['donnees_chevaux'], start=1):
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
                        'Cotes-Pmu': cheval['cote_pmu'],
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
        taches = [extraire_donnees(url, session) for url in urls]
        resultats = await asyncio.gather(*taches)
    # Filtre les résultats vides
    return [resultat for resultat in resultats if resultat]


async def main():
    """Fonction principale pour exécuter l'extracteur et enrichir les données."""
    configurer_logger()

    urls = [
        "https://www.geny.com/partants-pmh/2024-09-02-craon-prix-v-and-b_c1515922",
        "https://www.geny.com/partants-pmh/2024-09-02-craon-pmu-prix-chaussee-aux-moines_c1515923",
        "https://www.geny.com/partants-pmh/2024-09-02-craon-pmu-prix-des-transports-gillois-prix-tenor-de-baune_c1515924",
        "https://www.geny.com/partants-pmh/2024-09-02-craon-pmu-prix-dirickx-prix-intermede_c1515925",
        "https://www.geny.com/partants-pmh/2024-09-02-craon-pmu-prix-groupe-gendry-prix-pmu-bar-de-l-etoile_c1515926",
    ]

    toutes_donnees = await traiter_urls(urls)

    if toutes_donnees:
        donnees_excel = charger_donnees_excel("FichierH.xls")
        sauvegarder_en_csv(
            toutes_donnees, "donnees_courses_partants.csv", donnees_excel)
    else:
        logger.error("Aucune donnée extraite, fichier CSV non créé")

if __name__ == '__main__':
    asyncio.run(main())
