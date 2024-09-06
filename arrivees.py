import csv
import asyncio
import aiohttp
from typing import Dict, List, Tuple
from selectolax.parser import HTMLParser
from loguru import logger
from typing import Optional

# Configuration du logger
logger.add("log_arrivees.log", rotation="1 MB", level="INFO")


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

            # Vérifier si le numéro est valide
            if not numero.isdigit():
                continue

            # Traiter les cas spéciaux
            if any(status in place.lower() for status in ['dai', 'dpj', 'd']):
                places[numero] = 15
            elif place.isdigit():
                place_int = int(place)
                places[numero] = min(place_int, 12)
            elif place.lower() in ['a', 't']:
                places[numero] = 12
            else:
                places[numero] = 12

        logger.info(f"Places extraites : {places}")

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des places : {e}")

    return places


async def extraire_donnees_arrivee(html_content: str) -> Tuple[Dict[str, Tuple[str, str]], Dict[str, int], Optional[str]]:
    resultats_pmu = {}
    places = {}
    numero_course = None
    try:
        parser = HTMLParser(html_content)

        numero_course = extraire_numero_course(parser)
        if not numero_course:
            logger.warning("Numéro de course non trouvé dans le HTML.")
            return resultats_pmu, places, numero_course

        places = extraire_places(parser)

        # Trouver la div PMU
        pmu_div = None
        for div in parser.css('div'):
            if div.text().strip() == "PMU":
                pmu_div = div
                break

        if not pmu_div:
            logger.warning("Section PMU non trouvée dans le HTML.")
            return resultats_pmu, places, numero_course

        # Trouver le tableau qui suit la div PMU
        table = pmu_div.next
        while table and table.tag != 'table':
            table = table.next

        if not table:
            logger.warning("Tableau PMU non trouvé dans le HTML.")
            return resultats_pmu, places, numero_course

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

        logger.info(
            f"Extraction des données d'arrivée PMU réussie. {len(resultats_pmu)} chevaux trouvés.")

    except Exception as e:
        logger.error(
            f"Erreur lors de l'extraction des données d'arrivée PMU: {e}")

    return resultats_pmu, places, numero_course


async def mettre_a_jour_csv(donnees_csv: List[Dict[str, str]], resultats_pmu: Dict[str, Tuple[str, str]], places: Dict[str, int], numero_course: str) -> List[Dict[str, str]]:
    try:
        if not numero_course:
            logger.error(
                "Numéro de course manquant dans les données d'arrivée.")
            return donnees_csv

        for ligne in donnees_csv:
            if ligne['COURSE'] == numero_course:
                numero_cheval = ligne['NumChev']
                # Mise à jour des rapports
                if numero_cheval in resultats_pmu:
                    ligne['RAP-G'], ligne['RAP-P'] = resultats_pmu[numero_cheval]
                else:
                    ligne['RAP-G'], ligne['RAP-P'] = '0', '0'

                # Mise à jour de la place
                ligne['PLACE'] = str(places.get(numero_cheval, 12))

        logger.info(
            f"Mise à jour des données CSV réussie pour la course {numero_course}. {len(donnees_csv)} lignes traitées.")
    except Exception as e:
        logger.error(f"Erreur lors de la mise à jour des données CSV: {e}")

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
    logger.info(f"Traitement de l'URL: {url}")
    html_content = await fetch_html(url, session)
    if not html_content:
        logger.error(
            f"Impossible de continuer sans contenu HTML valide pour {url}")
        return donnees_csv

    resultats_pmu, places, numero_course = await extraire_donnees_arrivee(html_content)
    donnees_mises_a_jour = await mettre_a_jour_csv(donnees_csv, resultats_pmu, places, numero_course)
    return donnees_mises_a_jour


async def main():
    logger.info("Début du traitement des arrivées")

    donnees_csv = await lire_csv('donnees_courses_partants.csv')
    if not donnees_csv:
        logger.error("Impossible de continuer sans données CSV valides.")
        return

    urls_resultats = [
        "https://www.geny.com/arrivee-et-rapports-pmh?id_course=1515922&info=2024-09-02-Craon-Prix+V+And+B",
        "https://www.geny.com/arrivee-et-rapports-pmh/2024-09-02-craon-pmu-prix-chaussee-aux-moines_c1515923",
        "https://www.geny.com/arrivee-et-rapports-pmh/2024-09-02-craon-pmu-prix-des-transports-gillois-prix-tenor-de-baune_c1515924",
        "https://www.geny.com/arrivee-et-rapports-pmh/2024-09-02-craon-pmu-prix-dirickx-prix-intermede_c1515925",
        "https://www.geny.com/arrivee-et-rapports-pmh/2024-09-02-craon-pmu-prix-groupe-gendry-prix-pmu-bar-de-l-etoile_c1515926"
    ]

    async with aiohttp.ClientSession() as session:
        for url in urls_resultats:
            donnees_csv = await traiter_url(url, session, donnees_csv)

    # Sauvegarder le nouveau fichier CSV
    await sauvegarder_csv(donnees_csv, 'donnees_courses_arrivees.csv')

    logger.info("Fin du traitement des arrivées")

if __name__ == "__main__":
    asyncio.run(main())
