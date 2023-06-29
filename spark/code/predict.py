import os
import pyspark.sql.types as tp
import pyspark.sql.functions as fun
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


def getTrainedRegressor(impiantiFiles, id, carburante: str):
    data = spark.read.parquet(os.path.join(impiantiFiles, str(id) + ".parquet"))
    assembler = VectorAssembler(inputCols=["X_" + carburante], outputCol="features")
    model = LinearRegression(featuresCol="features", labelCol="Y_" + carburante, predictionCol="prediction", regParam=0.3, elasticNetParam=0.8)
    regressor = model.fit(assembler.transform(data))
    return regressor


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
impiantiFiles = os.path.join(datasetFolder, "Impianti", "")


#! -------------------- PREDICTION ----------------------------------------
tempDF = spark.read.csv(os.path.join(datasetFolder, "prezzo_alle_8.csv"), header=False, schema=schema, sep=";")
tempDF = cleanCSV(tempDF, anagrafica)
tempDF = tempDF.withColumn("prediction", tempDF.prezzo * 0)

impianti = set(anagrafica.select("idImpianto").collect())
for i in impianti:    
    row = tempDF[tempDF.idImpianto == i.idImpianto].drop("prediction")
    carb = (row.select("descCarburante").collect()[0].descCarburante).capitalize()

    assembler = VectorAssembler(inputCols=["prezzo"], outputCol="features")
    regressor = getTrainedRegressor(impiantiFiles, i.idImpianto, carb)

    row = regressor.transform(assembler.transform(row))
    row = row.drop("features")

    pred = row.select("prediction").collect()[0].prediction
    tempDF = tempDF.withColumn("prediction", fun.when(tempDF.idImpianto == i.idImpianto, pred).otherwise(tempDF.prediction))
