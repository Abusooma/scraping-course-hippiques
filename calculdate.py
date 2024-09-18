import pandas as pd
from pathlib import Path


def calculer_jour_de_annee(date):
    return date.timetuple().tm_yday


def calculer_jours_entre(date1, date2):
     resultat = (date2 - date1)
     return abs(resultat.days)


def traiter_fichier_excel(fichier_entree):
    df = pd.read_excel(fichier_entree)

    colonnes_date = ['Date-du-Jour', 'Date-1', 'Date-2', 'Date-3', 'Date-4']
    for col in colonnes_date:
        df[col] = pd.to_datetime(df[col], format='%d/%m/%Y')

    df['Nieme jour'] = df['Date-du-Jour'].apply(calculer_jour_de_annee)

    for i in range(1, 5):
        df[f'Nbr-jours-{i}'] = df.apply(lambda ligne: calculer_jours_entre(
            ligne['Date-du-Jour'], ligne[f'Date-{i}']), axis=1)

    return df 


def main():
    chemin_fichier = r"CALCULDATE.xls"

    fichier = Path(chemin_fichier)

    try:
        df_resultat = traiter_fichier_excel(fichier)

        fichier_sortie = fichier.stem + '_resultat.csv'
        df_resultat.to_csv(fichier_sortie, index=False, date_format='%d/%m/%Y')
        print(f"Le fichier résultat a été sauvegardé sous : {fichier_sortie}")

    except FileNotFoundError:
        print(f"Le fichier {fichier} n'a pas été trouvé.")
    except Exception as e:
        print(f"Une erreur s'est produite : {str(e)}")


if __name__ == "__main__":
    main()
