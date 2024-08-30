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
            prix_str = correspondance_prix.group(1).replace(' ', '')
            prix = int(prix_str[:-3]) if len(prix_str) > 3 else int(prix_str)
            partants = correspondance_partants.group(1).replace(' ', '')
            return prix, partants
        else:
            logger.error("Le prix ou le partant n'a pas été trouvé dans le texte lors de l'extraction")
            return None, None
    except Exception as e:
        logger.error(
            f"Erreur lors de l'extraction du prix et des partants : {e}")
        return None, None


# def extraire_chevaux_et_gains(arbre: HTMLParser) -> List[Dict[str, str]]:
#     """Extrait les chevaux et leurs gains du HTML."""
#     donnees_chevaux = []
#     try:
#         noeuds_chevaux = arbre.css("span.leftWidth100 a.lienFiche")
#         # Recuperer tous les tableaux dans le HTML:
#         tous_tableaux = arbre.css('table')
#         # Recuperer le tableau qui contient les gains des chevaux
#         tableau_des_gains = None
#         for tableau in tous_tableaux:
#             if tableau.css_first('span.leftWidth100'):
#                 tableau_des_gains = tableau
#                 break

#         # Recuperer les Noeud des gains dans leur tableau trouvé
#         if tableau_des_gains:
#             noeuds_gains = tableau_des_gains.css('tbody tr td:nth-child(8)')
#         else:
#             logger.error("Aucun tableau contenant des gains n'a été trouvé")
#             return donnees_chevaux

#         if not noeuds_chevaux or not noeuds_gains:
#             logger.error("Aucun cheval ou gain trouvé dans le tableau")
#             return donnees_chevaux

#         for cheval, gain in zip(noeuds_chevaux, noeuds_gains):
#             try:
#                 nom_cheval = cheval.text().strip()
#                 texte_gain = gain.text().strip()
#                 if nom_cheval and texte_gain:
#                     donnees_chevaux.append(
#                         {"nom": nom_cheval, "gain": texte_gain})
#             except AttributeError as e:
#                 logger.error(
#                     f"Erreur lors de l'extraction des données du cheval : {e}")

#     except Exception as e:
#         logger.error(f"Erreur générale lors de l'analyse : {e}")

#     return donnees_chevaux

def extraire_chevaux_et_gains(arbre: HTMLParser) -> List[Dict[str, str]]:
    """Extrait les chevaux et leurs gains du HTML."""
    donnees_chevaux = []
    try:
        # Trouver le tableau contenant les données des chevaux
        tableau = arbre.css_first('table#tableau_partants')
        if not tableau:
            logger.error("Tableau des partants non trouvé")
            return donnees_chevaux

        # Trouver l'index de la colonne "Gains"
        headers = tableau.css('thead th')
        gains_index = next((i for i, header in enumerate(
            headers) if header.text().strip() == "Gains"), None)

        if gains_index is None:
            logger.error("Colonne 'Gains' non trouvée dans le tableau")
            return donnees_chevaux

        # Extraire les noms des chevaux et leurs gains
        lignes = tableau.css('tbody tr')
        for ligne in lignes:
            try:
                nom_cheval = ligne.css_first('span.leftWidth100 a.lienFiche')
                # +1 car les index CSS commencent à 1
                gain = ligne.css(f'td:nth-child({gains_index + 1})')

                if nom_cheval and gain:
                    nom = nom_cheval.text().strip()
                    gain_texte = gain[0].text().strip() if gain else ""
                    if nom and gain_texte:
                        donnees_chevaux.append(
                            {"nom": nom, "gain": gain_texte})
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
            texte_html = await response.text()
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


def sauvegarder_en_csv(toutes_donnees: List[Dict[str, any]], nom_fichier: str):
    """Sauvegarde les données extraites dans un fichier CSV."""
    noms_champs = ['DATE', 'Hippodrome', 'COURSE', 'NumChev', 'CHEVAL',
                   'PLACE', 'RAP-G', 'RAP-P', 'PARTANTS', 'I-Gains', 'I-Prix du jour']

    try:
        with open(nom_fichier, 'w', newline='', encoding='utf-8') as fichier_csv:
            ecrivain = csv.DictWriter(fichier_csv, fieldnames=noms_champs)
            ecrivain.writeheader()

            for donnees in toutes_donnees:
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
                        'I-Prix du jour': donnees['prix']
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
            "https://www.geny.com/partants-pmu/2024-08-30-vincennes-pmu-prix-lampetia_c1515327",
            "https://www.geny.com/partants-pmu/2024-08-30-vincennes-pmu-prix-amalia_c1515330",
            "https://www.geny.com/partants-pmu/2024-08-30-vincennes-pmu-prix-athamantis_c1515328",
            "https://www.geny.com/partants-pmu/2024-08-30-vincennes-pmu-prix-diotima_c1515323",
            "https://www.geny.com/partants-pmu/2024-08-30-vincennes-pmu-prix-algeiba_c1515325",
            "https://www.geny.com/partants-pmu/2024-08-30-vincennes-pmu-prix-gisella_c1515326",
            "https://www.geny.com/partants-pmu/2024-08-30-vincennes-pmu-prix-danae_c1515329",
            "https://www.geny.com/partants-pmu/2024-08-30-vincennes-pmu-prix-dorado_c1515324"
    ]

    toutes_donnees = await traiter_urls(urls)

    if toutes_donnees:
        sauvegarder_en_csv(toutes_donnees, "donnees_courses.csv")
    else:
        logger.error("Aucune donnée extraite, fichier CSV non créé")

if __name__ == '__main__':
    asyncio.run(main())
   