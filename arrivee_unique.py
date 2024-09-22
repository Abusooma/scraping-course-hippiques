from urllib.parse import urljoin
import csv
import sys
import re
import asyncio
import aiohttp
from typing import Dict, List, Tuple, Set
from selectolax.parser import HTMLParser
from loguru import logger
from typing import Optional
from collections import defaultdict


BASE_URL = "https://www.geny.com/"

URLS_UNIQUES_ARRIVEES = [
    "https://www.geny.com/arrivee-et-rapports-pmu?id_course=1515246&info=2024-08-29-Strasbourg-pmu-Prix+de+Vesoul",
    "https://www.geny.com/arrivee-et-rapports-pmu?id_course=1518523&info=2024-09-11-Marseille-Bor%c3%a9ly-pmu-Prix+de+Ch%c3%a2telneuf",
    "https://www.geny.com/arrivee-et-rapports-pmu?id_course=1514560&info=2024-08-26-Vincennes-pmu-Prix+de+Barbizon"
]

# Configuration du logger
def configurer_logger():
    """Configure le logger avec les paramètres appropriés."""
    logger.remove()
    fichier_log = "log_partants.log"
    logger.add(fichier_log, rotation="500 KB",
               retention="3 days", level="WARNING")
    logger.add(sys.stderr, level="INFO")


async def lire_csv(nom_fichier: str) -> List[Dict[str, str]]:
    try:
        with open(nom_fichier, 'r', encoding='utf-8-sig') as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        logger.error(f"Le fichier CSV {nom_fichier} n'a pas été trouvé.")
        return []
    except Exception as e:
        logger.error(
            f"Erreur lors de la lecture du fichier CSV {nom_fichier}: {e}")
        return []


def extraire_numero_course(arbre: HTMLParser) -> Optional[str]:
    """Extrait le numéro de course du HTML."""
    try:
        noeud_numero_course = arbre.css_first("span h1")
        if noeud_numero_course:
            numero_course_text = noeud_numero_course.text().strip()
            return numero_course_text[0]
        else:
            logger.error("Aucun noeud correspondant au Numéro de course")
            return None
    except IndexError as e:
        logger.error("Erreur d'accès au numéro de course dans le texte")
        return None
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction du numéro de course : {e}")
        return None 


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
    

def extraire_numero_partant(arbre: HTMLParser) -> Optional[str]:
    try:
        noeud_partant = arbre.css_first("span.infoCourse")
        if noeud_partant:
            texte_partant = noeud_partant.text().replace("ï¿½", "")
        else:
            logger.error(
                "Aucun element avec l'attribut 'span.infoCourse' trouvé lors de l'extraction du 'partant'")
            return None

        match = re.search(r'-\s*(\d+)\s*Partants', texte_partant)
        if match:
            partant = match.group(1).strip()
        else:
            logger.error("Le partant n'a pas été trouvé lors de l'extraction")
            return None
        
    except Exception as e:
        logger.error("Une exception s'est produite lors de la récuperation du numero partant")
        return None
    
    return partant


def extraire_places(arbre: HTMLParser) -> Dict[str, int]:
    places = {}
    try:
        table_arrivees = arbre.css_first('table#arrivees')
        if not table_arrivees:
            logger.warning("Tableau des arrivées non trouvé dans le HTML.")
            return places

        for row in table_arrivees.css('tr'):
            cells = row.css('td')
            if len(cells) < 3:
                continue

            place = cells[0].text(strip=True)
            numero = cells[1].text(strip=True)

            if not numero.isdigit():
                continue

            if any(status in place.lower() for status in ['dai', 'dpj', 'd']):
                places[numero] = 15
            elif place.isdigit():
                place_int = int(place)
                places[numero] = min(place_int, 12)
            elif place.lower() in ['a', 't']:
                places[numero] = 12
            else:
                places[numero] = 12

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des places : {e}")

    return places


def extraire_non_partants(arbre: HTMLParser) -> Set[str]:
    non_partants = set()
    try:
        div_non_partant = arbre.css_first('div.nonPartant')
        if div_non_partant:
            texte_non_partant = div_non_partant.text().strip()
            match = re.search(r'Non-partant\s*:\s*(.*)', texte_non_partant)
            if match:
                numeros = match.group(1).split('-')
                non_partants = set(numero.strip()
                                   for numero in numeros if numero.strip().isdigit())
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des non partants : {e}")
    return non_partants


async def extraire_donnees_arrivee(html_content: str) -> Tuple[Dict[str, Tuple[str, str]], Dict[str, int], Optional[str], Optional[str], Set[str], Optional[str]]:
    resultats_pmu = {}
    places = {}
    numero_course = None
    hippodrome = None
    partant = None
    non_partants = set()
    try:
        parser = HTMLParser(html_content)

        numero_course = extraire_numero_course(parser)
        hippodrome = extraire_hippodrome(parser)
        partant = extraire_numero_partant(parser)

        if not numero_course or not hippodrome or not partant:
            logger.warning(
                "Numéro de course ou hippodrome ou partant non trouvé dans le HTML.")
            return resultats_pmu, places, numero_course, hippodrome, non_partants, partant

        places = extraire_places(parser)
        non_partants = extraire_non_partants(parser)

        # Trouver la div PMU
        pmu_div = None
        for div in parser.css('div'):
            if div.text().strip() == "PMU":
                pmu_div = div
                break

        if not pmu_div:
            logger.warning("Section PMU non trouvée dans le HTML.")
            return resultats_pmu, places, numero_course, hippodrome, non_partants, partant

        # Trouver le tableau qui suit la div PMU
        table = pmu_div.next
        while table and table.tag != 'table':
            table = table.next

        if not table:
            logger.warning("Tableau PMU non trouvé dans le HTML.")
            return resultats_pmu, places, numero_course, hippodrome, non_partants, partant

        for row in table.css('tr'):
            cells = row.css('td')
            if len(cells) < 2:
                continue

            numero = cells[0].css_first('b')
            type_pari = cells[0].css_first('div[style="float: right"]')
            montant = cells[1].text(strip=True)

            if numero and type_pari and montant:
                numero = numero.text(strip=True)
                type_pari = type_pari.text(strip=True)
                montant = montant.replace('€', '').replace(',', '.').strip()

                if numero not in resultats_pmu:
                    resultats_pmu[numero] = ['0', '0']

                if type_pari == 'Gagnant':
                    resultats_pmu[numero][0] = montant
                elif type_pari == 'Placé':
                    resultats_pmu[numero][1] = montant

    except Exception as e:
        logger.error(
            f"Erreur lors de l'extraction des données d'arrivée PMU: {e}")

    return resultats_pmu, places, numero_course, hippodrome, non_partants, partant


async def mettre_a_jour_csv(donnees_csv: List[Dict[str, str]], resultats_pmu: Dict[str, Tuple[str, str]], places: Dict[str, int], numero_course: str, hippodrome: str, non_partants: Set[str], partant: Optional[str]) -> List[Dict[str, str]]:
    try:
        if not numero_course or not hippodrome or not partant:
            logger.error(
                "Numéro de course ou hippodrome ou partant manquant dans les données d'arrivée.")
            return donnees_csv

        donnees_mises_a_jour = []
        for ligne in donnees_csv:
            if ligne['COURSE'] == numero_course and ligne['Hippodrome'] == hippodrome:
                numero_cheval = ligne['NumChev']
                if numero_cheval not in non_partants:
                    if numero_cheval in resultats_pmu:
                        ligne['RAP-G'], ligne['RAP-P'] = resultats_pmu[numero_cheval]
                    else:
                        ligne['RAP-G'], ligne['RAP-P'] = '0', '0'

                    ligne['PLACE'] = str(places.get(numero_cheval, 12))

                    
                    ligne['PARTANTS'] = partant if partant is not None else ligne.get('PARTANTS', '')

                    donnees_mises_a_jour.append(ligne)
            else:
                donnees_mises_a_jour.append(ligne)

        return donnees_mises_a_jour
    except Exception as e:
        logger.error(f"Erreur lors de la mise à jour des données CSV: {e}")
        return donnees_csv


def trier_chevaux_par_hippodrome_et_classement(donnees_csv: List[Dict[str, str]]) -> List[Dict[str, str]]:
    try:
        hippodromes = defaultdict(lambda: defaultdict(list))
        for ligne in donnees_csv:
            hippodrome = ligne['Hippodrome']
            course = ligne['COURSE']
            hippodromes[hippodrome][course].append(ligne)

        for hippodrome in hippodromes:
            for course in hippodromes[hippodrome]:
                hippodromes[hippodrome][course] = sorted(
                    hippodromes[hippodrome][course], key=lambda x: int(x['PLACE']))

        donnees_triees = []
        for hippodrome in sorted(hippodromes.keys()):
            for course in sorted(hippodromes[hippodrome].keys()):
                donnees_triees.extend(hippodromes[hippodrome][course])

        return donnees_triees
    except Exception as e:
        logger.error(
            f"Erreur lors du tri des chevaux par hippodrome et classement : {e}")
        return donnees_csv


async def sauvegarder_csv(donnees: List[Dict[str, str]], nom_fichier: str):
    try:
        with open(nom_fichier, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=donnees[0].keys())
            writer.writeheader()
            writer.writerows(donnees)
        logger.info(f"Sauvegarde du fichier CSV {nom_fichier} réussie.")
    except Exception as e:
        logger.error(
            f"Erreur lors de la sauvegarde du fichier CSV {nom_fichier}: {e}")


async def fetch_html(url: str, session: aiohttp.ClientSession) -> str:
    try:
        async with session.get(url) as response:
            return await response.text()
    except Exception as e:
        logger.error(
            f"Erreur lors de la récupération du contenu HTML pour {url}: {e}")
        return ""


async def traiter_url(url: str, session: aiohttp.ClientSession, donnees_csv: List[Dict[str, str]]) -> List[Dict[str, str]]:
    html_content = await fetch_html(url, session)
    if not html_content:
        logger.error(
            f"Impossible de continuer sans contenu HTML valide pour {url}")
        return donnees_csv

    resultats_pmu, places, numero_course, hippodrome, non_partants, partant = await extraire_donnees_arrivee(html_content)
    donnees_mises_a_jour = await mettre_a_jour_csv(donnees_csv, resultats_pmu, places, numero_course, hippodrome, non_partants, partant)
    return donnees_mises_a_jour


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


async def main():
    configurer_logger()

    logger.info("Début du traitement des arrivées")

    donnees_csv = await lire_csv('donnees_courses_partants.csv')
    if not donnees_csv:
        logger.error("Impossible de continuer sans données CSV valides.")
        return

    urls_resultats = await traiter_liste_urls(URLS_UNIQUES_ARRIVEES)

    async with aiohttp.ClientSession() as session:
        for url in urls_resultats:
            donnees_csv = await traiter_url(url, session, donnees_csv)

    donnees_triees = trier_chevaux_par_hippodrome_et_classement(donnees_csv)

    await sauvegarder_csv(donnees_triees, 'donnees_courses_arrivees.csv')

    logger.info("Fin du traitement des arrivées")

if __name__ == "__main__":
    asyncio.run(main())
