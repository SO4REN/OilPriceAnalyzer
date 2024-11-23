import glob
import pandas as pd
import os

import warnings
warnings.filterwarnings("ignore")


def cleanPrezzi(df, anagrafica):
    df = df[df.isSelf == 1]
    df = df[df.descCarburante.isin(["Benzina", "Gasolio"])]
    df = df.merge(anagrafica, on="idImpianto", how="inner")
    df.descCarburante = df.descCarburante.map({"Benzina": 0, "Gasolio": 1})
    df = df.drop(["isSelf", "Provincia", "dtComu", "Tipo Impianto"], axis=1)
    return df

def removeFirstLine(files):
    for file in files:
        with open(file, "r") as f:
            data = f.readlines()
            
            if not data[0].startswith("Estrazione"):
                return
            
            data = data[2:]
            with open(file, "w") as f:
                f.writelines(data)


    
def toMultipleFiles(inputPath, outputPath, anagraficaPath):
    anagrafica = pd.read_parquet(anagraficaPath)
    impianti = anagrafica.idImpianto.unique()

    files = glob.glob(os.path.join(inputPath, "*.csv"))
    files.sort()

    removeFirstLine(files)

    countImpianti = dict.fromkeys(impianti, 0)
    for X, Y in zip(files, files[1:]):
        dfX = pd.read_csv(X, sep=";", header=0, names=["idImpianto", "descCarburante", "prezzo", "isSelf", "dtComu"], on_bad_lines="skip")
        dfY = pd.read_csv(Y, sep=";", header=0, names=["idImpianto", "descCarburante", "prezzo", "isSelf", "dtComu"], on_bad_lines="skip")

        dfX = cleanPrezzi(dfX, anagrafica)
        dfY = cleanPrezzi(dfY, anagrafica)

        for impianto in impianti:
            df = None
            if os.path.exists(os.path.join(outputPath, "{id}.parquet".format(id=impianto))):
                df = pd.read_parquet(os.path.join(outputPath, "{id}.parquet".format(id=impianto)))
            else:
                df = pd.DataFrame(columns=["carburante", "X_prezzo", "Y_prezzo", "insertOrder"])
            
            prezzoX = dfX[dfX.idImpianto == impianto][["descCarburante", "prezzo", "idImpianto"]]
            prezzoY = dfY[dfY.idImpianto == impianto][["descCarburante", "prezzo", "idImpianto"]]

            if prezzoX.empty or prezzoY.empty:
                    continue
            else:
                for carb in (0, 1):
                    xTarget = prezzoX[prezzoX.descCarburante == carb].prezzo.values[0] if not prezzoX[prezzoX.descCarburante == carb].empty else -1
                    yTarget = prezzoY[prezzoY.descCarburante == carb].prezzo.values[0] if not prezzoY[prezzoY.descCarburante == carb].empty else -1
                    
                    newRow = pd.DataFrame({
                                        "carburante": carb,
                                        "X_prezzo": xTarget,
                                        "Y_prezzo": yTarget,
                                        "insertOrder": countImpianti[impianto]},
                                        columns=["carburante", "X_prezzo", "Y_prezzo", "insertOrder"], index=[0])
                    df = pd.concat([df, newRow], ignore_index=True)
            countImpianti[impianto] += 1
            if not os.path.exists(outputPath):
                os.makedirs(outputPath)
            df.to_parquet(os.path.join(outputPath, "{id}.parquet".format(id=impianto)))
        print("Processed:", X)
