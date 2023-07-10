import glob
import pandas as pd



def cleanPrezzi(df, anagrafica):
    df = df[df.isSelf == 1]
    df = df[df.descCarburante.isin(["Benzina", "Gasolio"])]
    df = df.merge(anagrafica, on="idImpianto", how="inner")
    df.carburante = df.carburante.map({"Benzina": 0, "Gasolio": 1})
    df = df.drop(["isSelf", "Provincia", "dtComu", "Tipo Impianto"], axis=1)
    return df

def removeFirstLine(files):
    for file in files:
        with open(file, "r") as f:
            data = f.readlines()
            data = data[2:]
            
            with open(file, "w") as f:
                f.writelines(data)


def main():
    anagrafica = pd.read_parquet("anagrafica_impianti_CT.parquet")
    impianti = anagrafica.idImpianto.unique()

    df = pd.DataFrame(columns=["idImpianto", "carburante", "X_prezzo", "Y_prezzo"])

    files = glob.glob("Prezzi/*.csv")
    files.sort()

    removeFirstLine(files)

    for X, Y in zip(files, files[1:]):
        dfX = pd.read_csv(X, sep=";", header=0, names=["idImpianto", "descCarburante", "prezzo", "isSelf", "dtComu"], on_bad_lines="skip")
        dfY = pd.read_csv(Y, sep=";", header=0, names=["idImpianto", "descCarburante", "prezzo", "isSelf", "dtComu"], on_bad_lines="skip")

        dfX = cleanPrezzi(dfX, anagrafica)
        dfY = cleanPrezzi(dfY, anagrafica)

        for impianto in impianti:
            prezzoX = dfX[dfX.idImpianto == impianto][["descCarburante", "prezzo", "idImpianto"]]
            prezzoY = dfY[dfY.idImpianto == impianto][["descCarburante", "prezzo", "idImpianto"]]

            if prezzoX.empty or prezzoY.empty:
                    continue
            else:
                for carb in ("Benzina", "Gasolio"):
                    newRow = pd.DataFrame({
                                        "idImpianto": impianto,
                                        "carburante": carb,
                                        "X_prezzo": prezzoX[prezzoX.descCarburante == carb].prezzo.values[0],
                                        "Y_prezzo": prezzoY[prezzoY.descCarburante == carb].prezzo.values[0]},
                                        columns=["idImpianto", "carburante", "X_prezzo", "Y_prezzo"], index=[0])
                    df = pd.concat([df, newRow], ignore_index=True)
    df.to_parquet("Prezzi.parquet")


if __name__ == "__main__":
    main()
