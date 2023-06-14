import os
import glob
import pyspark.sql.types as tp
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression




def cleanCSV(df, anagrafica):
    df = df[df.isSelf == 1]
    df = df[df.descCarburante.isin(["Benzina", "Gasolio"])]
    df = df.withColumnRenamed("idImpianto", "idImpiantoPrezzo")
    df = df.join(anagrafica, df.idImpiantoPrezzo == anagrafica.idImpianto, how="inner")
    
    df = df.drop("__index_level_0__", "isSelf", "Provincia", "dtComu", "idImpiantoPrezzo")
    return df


def createAndTrainModel(df, mpath, type: str):
    path = os.path.join(os.path.join(mpath, type), "")

    assembler = VectorAssembler(inputCols=["prezzoX"], outputCol="features")
    train, _ = df.randomSplit([0.8, 0.2])

    if type not in ["Benzina", "Gasolio"]:
        type = "Benzina"
    if os.path.exists(path):
        model = LinearRegression.load(os.path.join(os.path.join(path, type), ""))
    else:
        model = LinearRegression(featuresCol="features", labelCol="prezzoY", predictionCol="prediction", regParam=0.3, elasticNetParam=0.8)

    lr = model.fit(assembler.transform(train))
    model.write().overwrite().save(os.path.join(os.path.join(path, type), ""))
    print("RMSE: %f" % lr.summary.rootMeanSquaredError) 
    return lr




#! ------------------------------ MAIN ------------------------------ *#

schema = tp.StructType([
    tp.StructField("idImpianto", tp.IntegerType(), True),
    tp.StructField("descCarburante", tp.StringType(), True),
    tp.StructField("prezzo", tp.DoubleType(), True),
    tp.StructField("isSelf", tp.IntegerType(), True),
    tp.StructField("dtComu", tp.StringType(), True)
])

sparkConf = SparkConf()
sc = SparkContext(appName = "Predict", conf = sparkConf)
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

modelPath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "model")
datasetFolder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dataset")
anagrafica = spark.read.parquet(os.path.join(datasetFolder, "anagrafica_impianti_CT.parquet"))

prices = os.path.join(os.path.join(datasetFolder, "Prezzi"), "")
files = glob.glob(os.path.join(prices, "*.csv"))
files.sort()


lrBenzina = None
lrGasolio = None
for X, Y in zip(files, files[1:]):
    dfX = spark.read.csv(X, schema=schema, sep=";")
    dfY = spark.read.csv(Y, schema=schema, sep=";")

    dfX = cleanCSV(dfX, anagrafica)
    dfY = cleanCSV(dfY, anagrafica)

    dfX = dfX.withColumnRenamed("prezzo", "prezzoX")
    dfY = dfY.withColumnRenamed("prezzo", "prezzoY")
    dfX = dfX.withColumnRenamed("Tipo Impianto", "Tipo Impianto X")

    df = dfX.join(dfY, ["idImpianto", "descCarburante"], how="inner")
    df = df[df["Tipo Impianto X"] == "Stradale"].select("descCarburante", "prezzoX", "prezzoY")

    dfBenzina = df[(df.descCarburante == "Benzina")].drop("descCarburante")
    dfGasolio = df[(df.descCarburante == "Gasolio")].drop("descCarburante")

    lrBenzina = createAndTrainModel(dfBenzina, modelPath, "Benzina")
    lrGasolio = createAndTrainModel(dfGasolio, modelPath, "Gasolio")
