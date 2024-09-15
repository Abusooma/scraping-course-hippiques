import requests
import re
from selectolax.parser import HTMLParser

url = "https://www.geny.com/partants-pmu/2024-09-07-vincennes-pmu-prix-de-beziers_c1517317"

responses = requests.get(url)

with open("index2.html", "w") as f:
    f.write(responses.text)

with open("index2.html", "r") as f:
    fichier = f.read()

tree = HTMLParser(fichier)


