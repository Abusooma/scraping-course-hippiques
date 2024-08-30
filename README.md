# Programme d'extraction de données

Salut ! Voici comment exécuter Ce programme.

## Installation et exécution

1. Ouvre le dossier du projet dans ton éditeur de code préferé.

2. Ouvre un terminal dans le dossier du projet.

3. Crée un environnement virtuel :
   ```
   python -m venv venv
   ```

4. Active l'environnement virtuel :
   ```
   .\venv\Scripts\activate
   ```

5. Installe les dépendances :
   ```
   pip install -r requirements.txt
   ```

6. Avant d'exécuter le programme, ouvre le fichier du script "scripts.py et cherche la fonction "async def main()" elle se trouve vers la fin du script

Tu devrais voir quelque chose comme ça :

   ```python
   """Mettez toutes vos urls avec le prefixe "https://www.geny.com/partants-pmu/" Avant d'executer le programme"""
   urls = []
   ```

   Ajoute les URLs ou une seule URL comme tu voudras dans cette liste, en t'assurant qu'elles commencent toutes par "https://www.geny.com/partants-pmu/".

7. Exécute le script :
   ```
   python scripts.py
   ```

## Problèmes ?

- Vérifie que vous tu as bien activé l'environnement virtuel avant d'installer les dépendances ou d'exécuter le script.


Si tu as des questions, n'hésite pas à me contacter !