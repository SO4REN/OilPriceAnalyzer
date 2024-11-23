import os
import glob
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

import warnings
warnings.filterwarnings("ignore")


def cleanPrezzi(df, anagrafica):
    df = df[df.isSelf == 1]
    df = df[df.descCarburante.isin(["Benzina", "Gasolio"])]
    df = df.merge(anagrafica, on="idImpianto", how="inner")
    df.descCarburante = df.descCarburante.map({"Benzina": 0, "Gasolio": 1})
    df = df.drop(["isSelf", "Provincia", "dtComu", "Tipo Impianto"], axis=1)
    return df


def removeFirstLine(file):
    with open(file, "r") as f:
        data = f.readlines()

        if not data[0].startswith("Estrazione"):
            return

        data = data[2:]
        with open(file, "w") as f:
            f.writelines(data)


def toMultipleFiles(inputPath, outputPath, anagraficaPath):
    def write_parquet(impianto, df):
        parquet_path = os.path.join(outputPath, f"{impianto}.parquet")
        df.to_parquet(parquet_path, index=False)

    anagrafica = pd.read_parquet(anagraficaPath)
    impianti = anagrafica.idImpianto.unique()
    files = sorted(glob.glob(os.path.join(inputPath, "*.csv")))

    with ThreadPoolExecutor() as executor:
        executor.map(removeFirstLine, files)

    impianti_data = {impianto: pd.DataFrame(columns=["carburante", "X_prezzo", "Y_prezzo", "insertOrder"]) for impianto in impianti}
    countImpianti = {impianto: len(impianti_data[impianto]) for impianto in impianti}

    for X, Y in zip(files, files[1:]):
        def load_csv(file):
            return pd.read_csv(
                file, sep=";", header=0,
                names=["idImpianto", "descCarburante", "prezzo", "isSelf", "dtComu"],
                on_bad_lines="skip"
            )

        with ThreadPoolExecutor() as executor:
            dfX, dfY = executor.map(load_csv, [X, Y])

        with ThreadPoolExecutor() as executor:
            dfX, dfY = executor.map(cleanPrezzi, [dfX, dfY], [anagrafica, anagrafica])

        for impianto in impianti:
            prezzoX = dfX[dfX.idImpianto == impianto][["descCarburante", "prezzo"]]
            prezzoY = dfY[dfY.idImpianto == impianto][["descCarburante", "prezzo"]]

            if prezzoX.empty or prezzoY.empty:
                continue

            rows = []
            for carb in (0, 1):
                xTarget = prezzoX[prezzoX.descCarburante == carb].prezzo.values[0] if not prezzoX[prezzoX.descCarburante == carb].empty else -1
                yTarget = prezzoY[prezzoY.descCarburante == carb].prezzo.values[0] if not prezzoY[prezzoY.descCarburante == carb].empty else -1

                rows.append({"carburante": carb, "X_prezzo": xTarget, "Y_prezzo": yTarget, "insertOrder": countImpianti[impianto]})
            impianti_data[impianto] = pd.concat([impianti_data[impianto], pd.DataFrame(rows)], ignore_index=True)
            countImpianti[impianto] += 1
        print(f"Processed: {X}")

    if not os.path.exists(outputPath):
        os.makedirs(outputPath)

    with ThreadPoolExecutor() as executor:
        executor.map(lambda item: write_parquet(*item), impianti_data.items())
    print("Processing complete.")
