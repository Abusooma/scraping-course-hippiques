import requests
from selectolax.parser import HTMLParser

# url = "https://www.geny.com/arrivee-et-rapports-pmh?id_course=1515922&info=2024-09-02-Craon-Prix+V+And+B"


# response = requests.get(url)

# with open("index2.html", 'w') as f:
#     f.write(response.text)

with open("index2.html", "r") as f:
    fichier = f.read()

tree = HTMLParser(fichier)
print(len(tree.css('[align="right"]')))
