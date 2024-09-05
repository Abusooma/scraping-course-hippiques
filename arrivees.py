# import csv
# import asyncio
# import aiohttp
# from typing import Dict, List, Tuple
# from selectolax.parser import HTMLParser
# from loguru import logger
# from pprint import pprint

# # Configuration du logger
# logger.add("log_arrivees.log", rotation="1 MB", level="INFO")


# async def lire_csv(nom_fichier: str) -> List[Dict[str, str]]:
#     try:
#         with open(nom_fichier, 'r', encoding='utf-8-sig') as f:
#             return list(csv.DictReader(f))
#     except FileNotFoundError:
#         logger.error(f"Le fichier CSV {nom_fichier} n'a pas été trouvé.")
#         return []
#     except Exception as e:
#         logger.error(
#             f"Erreur lors de la lecture du fichier CSV {nom_fichier}: {e}")
#         return []


# async def extraire_donnees_arrivee(html_content: str) -> Dict[str, Tuple[str, str]]:
#     resultats = {}
#     try:
#         parser = HTMLParser(html_content)

#         # Trouver la div PMU
#         pmu_div = None
#         for div in parser.css('div'):
#             if div.text().strip() == "PMU":
#                 pmu_div = div
#                 break

#         if not pmu_div:
#             logger.warning("Section PMU non trouvée dans le HTML.")
#             return resultats

#         # Trouver le tableau qui suit la div PMU
#         table = pmu_div.next
#         while table and table.tag != 'table':
#             table = table.next

#         if not table:
#             logger.warning("Tableau PMU non trouvé dans le HTML.")
#             return resultats

#         for row in table.css('tr'):
#             cells = row.css('td')
#             if len(cells) < 2:
#                 continue

#             numero = cells[0].css_first('b')
#             type_pari = cells[0].css_first('div[style="float: right"]')
#             montant = cells[1].text(strip=True)

#             if numero and type_pari and montant:
#                 numero = numero.text(strip=True)
#                 type_pari = type_pari.text(strip=True)
#                 montant = montant.replace('€', '').replace(',', '.').strip()

#                 if numero not in resultats:
#                     resultats[numero] = ['0', '0']

#                 if type_pari == 'Gagnant':
#                     resultats[numero][0] = montant
#                 elif type_pari == 'Placé':
#                     resultats[numero][1] = montant

#         logger.info(
#             f"Extraction des données d'arrivée PMU réussie. {len(resultats)} chevaux trouvés.")
#     except Exception as e:
#         logger.error(
#             f"Erreur lors de l'extraction des données d'arrivée PMU: {e}")

#     return resultats


# async def mettre_a_jour_csv(donnees_csv: List[Dict[str, str]], donnees_arrivee: Dict[str, Tuple[str, str]]) -> List[Dict[str, str]]:
#     try:
#         for ligne in donnees_csv:
#             numero_cheval = ligne['NumChev']
#             if numero_cheval in donnees_arrivee:
#                 ligne['RAP-G'], ligne['RAP-P'] = donnees_arrivee[numero_cheval]
#             else:
#                 ligne['RAP-G'], ligne['RAP-P'] = '0', '0'
#         logger.info(
#             f"Mise à jour des données CSV réussie. {len(donnees_csv)} lignes traitées.")
#     except Exception as e:
#         logger.error(f"Erreur lors de la mise à jour des données CSV: {e}")
#     return donnees_csv


# async def sauvegarder_csv(donnees: List[Dict[str, str]], nom_fichier: str):
#     try:
#         with open(nom_fichier, 'w', newline='', encoding='utf-8-sig') as f:
#             writer = csv.DictWriter(f, fieldnames=donnees[0].keys())
#             writer.writeheader()
#             writer.writerows(donnees)
#         logger.info(f"Sauvegarde du fichier CSV {nom_fichier} réussie.")
#     except Exception as e:
#         logger.error(
#             f"Erreur lors de la sauvegarde du fichier CSV {nom_fichier}: {e}")


# async def fetch_html(url: str, session: aiohttp.ClientSession) -> str:
#     try:
#         async with session.get(url) as response:
#             return await response.text()
#     except Exception as e:
#         logger.error(f"Erreur lors de la récupération du contenu HTML: {e}")
#         return ""


# async def main():
#     logger.info("Début du traitement des arrivées")

#     donnees_csv = await lire_csv('donnees_courses_partants.csv')
#     if not donnees_csv:
#         logger.error("Impossible de continuer sans données CSV valides.")
#         return

#     url_resultats = "https://www.geny.com/arrivee-et-rapports-pmh/2024-09-02-craon-pmu-prix-dirickx-prix-intermede_c1515925"

#     async with aiohttp.ClientSession() as session:
#         # Récupérer le contenu HTML
#         html_content = await fetch_html(url_resultats, session)
#         if not html_content:
#             logger.error("Impossible de continuer sans contenu HTML valide.")
#             return

#         # Extraire les données d'arrivée
#         donnees_arrivee = await extraire_donnees_arrivee(html_content)

#         # Mettre à jour les données CSV
#         donnees_mises_a_jour = await mettre_a_jour_csv(donnees_csv, donnees_arrivee)

#         # Sauvegarder le nouveau fichier CSV
#         await sauvegarder_csv(donnees_mises_a_jour, 'donnees_courses_arrivees.csv')

#     logger.info("Fin du traitement des arrivées")

# if __name__ == "__main__":
#     asyncio.run(main())

import csv
import asyncio
import aiohttp
from typing import Dict, List, Tuple
from selectolax.parser import HTMLParser
from loguru import logger
from pprint import pprint

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


async def extraire_donnees_arrivee(html_content: str) -> Dict[str, Tuple[str, str]]:
    resultats = {}
    try:
        parser = HTMLParser(html_content)

        # Trouver la div PMU
        pmu_div = None
        for div in parser.css('div'):
            if div.text().strip() == "PMU":
                pmu_div = div
                break

        if not pmu_div:
            logger.warning("Section PMU non trouvée dans le HTML.")
            return resultats

        # Trouver le tableau qui suit la div PMU
        table = pmu_div.next
        while table and table.tag != 'table':
            table = table.next

        if not table:
            logger.warning("Tableau PMU non trouvé dans le HTML.")
            return resultats

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

                if numero not in resultats:
                    resultats[numero] = ['0', '0']

                if type_pari == 'Gagnant':
                    resultats[numero][0] = montant
                elif type_pari == 'Placé':
                    resultats[numero][1] = montant

        logger.info(
            f"Extraction des données d'arrivée PMU réussie. {len(resultats)} chevaux trouvés.")
    except Exception as e:
        logger.error(
            f"Erreur lors de l'extraction des données d'arrivée PMU: {e}")

    return resultats


async def mettre_a_jour_csv(donnees_csv: List[Dict[str, str]], donnees_arrivee: Dict[str, Tuple[str, str]]) -> List[Dict[str, str]]:
    try:
        for ligne in donnees_csv:
            numero_cheval = ligne['NumChev']
            if numero_cheval in donnees_arrivee:
                ligne['RAP-G'], ligne['RAP-P'] = donnees_arrivee[numero_cheval]
            else:
                ligne['RAP-G'], ligne['RAP-P'] = '0', '0'
        logger.info(
            f"Mise à jour des données CSV réussie. {len(donnees_csv)} lignes traitées.")
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

    donnees_arrivee = await extraire_donnees_arrivee(html_content)
    donnees_mises_a_jour = await mettre_a_jour_csv(donnees_csv, donnees_arrivee)
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
