import os
from pyspark import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.session import SparkSession

from clean import *
from predict import *


def saveAndUpdate(df, _):
    # global spark
    # global impianti
    global trainingDataset

    if(df.count() == 0):
        return

    # TODO SELECT NEWEST DATA FROM TRAINING
    # tr = trainingDataset.drop("__index_level_0__")
    # for impianto in impianti:
    #     for carb in (0, 1):
    #         row = tr.filter((tr.idImpianto == impianto) & (tr.carburante == carb)).orderBy("date", ascending=False).limit(1)
    #         tempDF = df.filter((df.idImpianto == impianto) & (df.carburante == carb))
            
    #         newRow = spark.createDataFrame([row], tr.schema)
    #         newRow = newRow.drop("X_prezzo")
    #         newRow = newRow.withColumnRenamed("Y_prezzo", "X_prezzo")
    #         newRow = newRow.withColumn("Y_prezzo", tempDF.prezzo)
    #         print(newRow.show())
    
    df.write.format("console").save()   #TODO save to ElasticSearch


def initSpark():
    sc = SparkContext(appName = "OilPricePrediction")
    spark = SparkSession(sc).builder.appName("OilPricePrediction").getOrCreate()
    sc.setLogLevel("ERROR")

    sc.addPyFile(os.path.join(os.path.dirname(os.path.realpath(__file__)), "clean.py"))
    sc.addPyFile(os.path.join(os.path.dirname(os.path.realpath(__file__)), "predict.py"))
    return sc, spark


def main(spark):
    #* GET STREAMING INPUT DATAFRAME
    inputDF = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", kafkaServer) \
            .option("subscribe", topic) \
            .load()

    df = cleanStreamingDF(inputDF, anagrafica)      #* DATA CLEANING AND DEDUPLICATION
    df = predictStreamingDF(df, trainingDataset)    #* DATA PREDICTION

    #* EXECUTE (leaving foreachBatch for future updates to dataset and retraining on the fly)
    df.writeStream \
        .foreachBatch(saveAndUpdate) \
        .start() \
        .awaitTermination()

    spark.stop()


if __name__ == "__main__":
    topic = "prices"
    kafkaServer = "kafkaServer:9092"
    sc, spark = initSpark()
    
    datasetFolder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dataset")
    anagrafica = spark.read.parquet(os.path.join(datasetFolder, "anagrafica_impianti_CT.parquet"))
    trainingDataset = spark.read.parquet(os.path.join(datasetFolder, "prezzi.parquet"))
    
    # impianti = anagrafica.select("idImpianto").distinct().collect()
    # impianti = [row.idImpianto for row in impianti]

    main(spark)

