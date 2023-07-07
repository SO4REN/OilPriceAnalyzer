import os
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

from clean import *
from predict import *



def initSpark():
    sc = SparkContext(appName = "OilPricePrediction")
    spark = SparkSession(sc).builder.appName("OilPricePrediction").getOrCreate()
    sc.setLogLevel("ERROR")

    sc.addPyFile(os.path.join(os.path.dirname(os.path.realpath(__file__)), "clean.py"))
    sc.addPyFile(os.path.join(os.path.dirname(os.path.realpath(__file__)), "predict.py"))
    return sc, spark


def main():
    topic = "prices"
    kafkaServer = "kafkaServer:9092"
    sc, spark = initSpark()
    
    datasetFolder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dataset")
    anagrafica = spark.read.parquet(os.path.join(datasetFolder, "anagrafica_impianti_CT.parquet"))
    trainingDataset = spark.read.parquet(os.path.join(datasetFolder, "prezzi.parquet"))
    #! ---------------------

    #* GET STREAMING INPUT DATAFRAME
    inputDF = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", kafkaServer) \
            .option("subscribe", topic) \
            .load()

    #* DATA CLEANING
    df = cleanStreamingDF(inputDF, anagrafica)

    #* DATA PREDICTION
    df = predictStreamingDF(df, trainingDataset)

    #* EXECUTE
    df.writeStream \
        .format("console") \
        .start() \
        .awaitTermination()  

    spark.stop()
    
    
if __name__ == "__main__":
    main()
