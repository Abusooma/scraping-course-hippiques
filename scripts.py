# Bibliothèques standard de python
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
    logger.warning("Date non trouvée dans l'URL")
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
        noeud_course = arbre.css_first("span.numeroCourse.fondVert")
        texte_course = noeud_course.text()
        return int(texte_course)
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction du numéro de course : {e}")
        return None


def extraire_prix_et_partants(arbre: HTMLParser) -> Tuple[Optional[int], Optional[int]]:
    """Extrait le prix et le nombre de partants du HTML."""
    try:
        noeud_info_course = arbre.css_first("span.infoCourse")
        texte_info_course = noeud_info_course.text().replace("ï¿½", "")

        correspondance_prix = re.search(
            r'-\s*(\d+)[\s\S]*?000€', texte_info_course)
        correspondance_partants = re.search(
            r'-\s*(\d+)\s*Partants', texte_info_course)

        prix = int(correspondance_prix.group(
            1)) if correspondance_prix else None
        partants = int(correspondance_partants.group(
            1)) if correspondance_partants else None

        return prix, partants
    except Exception as e:
        logger.error(
            f"Erreur lors de l'extraction du prix et des partants : {e}")
        return None, None


def extraire_chevaux_et_gains(arbre: HTMLParser) -> List[Dict[str, str]]:
    """Extrait les chevaux et leurs gains du HTML."""
    donnees_chevaux = []
    try:
        noeuds_chevaux = arbre.css("span.leftWidth100 a.lienFiche")
        # Recuperer toutes les tables dans le HTML:
        tables = arbre.css('table')
        # Recuperer le tableau qui contient les gains des chevaux
        tableau_des_gains = [table for table in tables if table.css_first('span.leftWidth100')]
        # Recuperer les Noeud des gains dans leur tableau trouvé
        noeuds_gains = tableau_des_gains[0].css('tbody tr td:nth-child(8)')

        if not noeuds_chevaux or not noeuds_gains:
            logger.error("Aucun cheval ou gain trouvé dans le tableau")
            return donnees_chevaux

        for cheval, gain in zip(noeuds_chevaux, noeuds_gains):
            try:
                nom_cheval = cheval.text().strip()
                texte_gain = gain.text().strip()
                if nom_cheval and texte_gain:
                    donnees_chevaux.append(
                        {"nom": nom_cheval, "gain": texte_gain})
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
        logger.error(f"Erreur HTTP survenue : {e}")
        return {}


def sauvegarder_en_csv(donnees: Dict[str, any], nom_fichier: str):
    """Sauvegarde les données extraites dans un fichier CSV."""
    noms_champs = ['DATE', 'Hippodrome', 'COURSE', 'NumChev', 'CHEVAL',
                   'PLACE', 'RAP-G', 'RAP-P', 'PARTANTS', 'I-Gains', 'I-Prix du jour']

    try:
        with open(nom_fichier, 'w', newline='', encoding='utf-8') as fichier_csv:
            ecrivain = csv.DictWriter(fichier_csv, fieldnames=noms_champs)
            ecrivain.writeheader()

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


async def main():
    """Fonction principale pour exécuter l'extracteur."""
    configurer_logger()
    url = "https://www.geny.com/partants-pmu/2024-08-26-vincennes-pmu-prix-de-barbizon_c1514560"

    async with aiohttp.ClientSession() as session:
        donnees = await extraire_donnees(url, session)

    if donnees:
        sauvegarder_en_csv(donnees, "donnees_courses.csv")
    else:
        logger.error("Aucune donnée extraite, fichier CSV non créé")

if __name__ == '__main__':
    asyncio.run(main())
