# Bibliothèques standard de python
import requests
import re
import sys
import csv
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# Packages externes installés par pip
from loguru import logger
from selectolax.parser import HTMLParser
import asyncio
import aiohttp


def configurer_logger():
    """Configure le logger avec les paramètres appropriés."""
    fichier_log = "fichier.log"
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
    """Extrait le nom de l'hippodrome du HTML."""
    try:
        noeud_hippodrome = arbre.css_first("div.nomReunion")
        if noeud_hippodrome is None:
            raise ValueError("Nœud 'div.nomReunion' non trouvé")

        texte_hippodrome = noeud_hippodrome.text()
        if not texte_hippodrome:
            raise ValueError("Le texte de l'hippodrome est vide")

        hippodrome_nettoye = re.sub(r'[^A-Za-z0-9:]', '', texte_hippodrome)
        hippodrome_nettoye = hippodrome_nettoye.split(':')

        if len(hippodrome_nettoye) < 2:
            raise ValueError("Format de texte d'hippodrome incorrect")

        hippodrome = hippodrome_nettoye[1][:-2]
        if not hippodrome:
            raise ValueError("Le nom de l'hippodrome est vide après nettoyage")

        return hippodrome

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
                    if cote_pmu:
                        cote_pmu_texte = cote_pmu[0].text().strip()
                        cote_pmu_texte = re.sub(r'[^\x20-\x7E]', '', cote_pmu_texte)
                        cote_pmu_texte = cote_pmu_texte.strip('"').replace(',', '.')
                    else:
                        cote_pmu = ""

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


def sauvegarder_en_csv(toutes_donnees: List[Dict[str, any]], nom_fichier: str):
    """Sauvegarde les données extraites dans un fichier CSV."""
    noms_champs = ['DATE', 'Hippodrome', 'COURSE', 'NumChev', 'CHEVAL',
                   'PLACE', 'RAP-G', 'RAP-P', 'PARTANTS', 'I-Gains', 'I-Prix du jour',
                   'I-Moins-Riche', 'I-Plus-Riche', 'Cotes-Pmu']

    try:
        with open(nom_fichier, 'w', newline='', encoding='utf-8-sig') as fichier_csv:
            ecrivain = csv.DictWriter(fichier_csv, fieldnames=noms_champs)
            ecrivain.writeheader()

            for donnees in toutes_donnees:
                moins_riche, plus_riche = calculer_gains_min_max(
                    donnees['donnees_chevaux'])

                for i, cheval in enumerate(donnees['donnees_chevaux'], start=1):
                    ecrivain.writerow({
                        'DATE': donnees['date'],
                        'Hippodrome': donnees['hippodrome'],
                        'COURSE': donnees['numero_course'],
                        'NumChev': i,
                        'CHEVAL': cheval['nom'],
                        'PLACE': '',  # Non disponible dans les données actuelles
                        'RAP-G': '',  # Non disponible dans les données actuelles
                        'RAP-P': '',  # Non disponible dans les données actuelles
                        'PARTANTS': donnees['partants'],
                        'I-Gains': cheval['gain'],
                        'I-Prix du jour': donnees['prix'],
                        'I-Moins-Riche': moins_riche,
                        'I-Plus-Riche': plus_riche,
                        'Cotes-Pmu': cheval['cote_pmu']
                    })
        logger.info(f"Données sauvegardées avec succès dans {nom_fichier}")
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde des données en CSV : {e}")


async def traiter_urls(urls: List[str]) -> List[Dict[str, any]]:
    """Traite une liste d'URLs de manière asynchrone."""
    async with aiohttp.ClientSession() as session:
        taches = [extraire_donnees(url, session) for url in urls]
        resultats = await asyncio.gather(*taches)
    # Filtre les résultats vides
    return [resultat for resultat in resultats if resultat]


async def main():
    """Fonction principale pour exécuter l'extracteur."""
    configurer_logger()

    """Mettez toutes vos urls avec le prefixe "https://www.geny.com/partants-pmu/" Avant d'executer le programme"""
    urls = [
        "https://www.geny.com/partants-pmu/2024-09-01-salon-de-provence-pmu-prix-d-arles_c1515880"
        
    ]
    
    toutes_donnees = await traiter_urls(urls)

    if toutes_donnees:
        sauvegarder_en_csv(toutes_donnees, "donnees_courses.csv")
    else:
        logger.error("Aucune donnée extraite, fichier CSV non créé")

if __name__ == '__main__':
    asyncio.run(main())
   