import requests
from selectolax.parser import HTMLParser

url = "https://www.geny.com/partants-pmu/2024-08-29-strasbourg-pmu-prix-de-vesoul_c1515246"

response = requests.get(url)

with open("index.html", "w") as f:
    f.write(response.text)

with open("index.html", "r") as f:
    fichier = f.read()

arbre = HTMLParser(fichier)

les_liens = arbre.css('#yui-main a')[1]
print(les_liens.text())
