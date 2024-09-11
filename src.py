import requests
import re
from selectolax.parser import HTMLParser

url = "https://www.geny.com/partants-pmh/2024-09-02-craon-pmu-prix-lesieur-prix-courant-d-air_c1515849"

responses = requests.get(url)

with open("index.html", "w") as f:
    f.write(responses.text)

with open("index.html", "r") as f:
    fichier = f.read()

tree = HTMLParser(fichier)
info_course = tree.css_first("span.infoCourse")
info_course_nettoyee = info_course.text().replace("ï¿½", "").strip()
match = re.search(r'-\s*(\d+)\s*Partants', info_course_nettoyee)
print(match.group(0))
print(match.end())
# Regex parfait pour nettoyer ce texte: r'-\s*(\d+)\s*Partants'

