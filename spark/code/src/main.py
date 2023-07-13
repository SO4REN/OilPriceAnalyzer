import os
from pyspark import SparkContext
from elasticsearch import Elasticsearch
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



def createElasticIndex(host, index, mapping):
    es = Elasticsearch(hosts=host)

    response = es.indices.create(
        index=index,
        body=mapping,
        ignore=400
    )

    if 'acknowledged' in response:
        if response['acknowledged'] == True:
            print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])
    return es



def main(spark):    
    #* GET STREAMING INPUT DATAFRAME
    inputDF = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .option("subscribe", KAFKA_TOPIC) \
            .load()

    df = cleanStreamingDF(inputDF, anagrafica)      #* DATA CLEANING AND DEDUPLICATION
    df = predictStreamingDF(df, trainingDataset)    #* DATA PREDICTION

    #* EXECUTE
    df.writeStream \
        .option("checkpointLocation", "/save/location") \
        .option("es.nodes", "elasticsearch") \
        .format("es") \
        .start(ELASTIC_INDEX) \
        .awaitTermination()

    spark.stop()





if __name__ == "__main__":
    KAFKA_TOPIC = "prices"
    KAFKA_SERVER = "kafkaServer:9092"
    ELASTIC_HOST = "http://elasticsearch:9200"
    ELASTIC_INDEX = "prices"
    
    ES_MAPPING = {
        "mappings": {
            "properties": {
                "idImpianto": {"type": "keyword"},
                "carburante": {"type": "integer"},
                "prezzo": {"type": "float"},
                "@timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                "original_timestamp": {"type": "date", "format": "epoch_millis"},
                "prediction": {"type": "float"},
                "Gestore": {"type": "text"},
                "Bandiera": {"type": "text"},
                "Nome Impianto": {"type": "text"},
                "Indirizzo": {"type": "text"},
                "Comune": {"type": "text"},
                "Location": {"type": "geo_point"},
            },
        },
    }
    
    #*-----------------------------------------------------------------
    
    sc, spark = initSpark()
    es = createElasticIndex(ELASTIC_HOST, ELASTIC_INDEX, ES_MAPPING)
    
    datasetFolder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dataset")
    anagrafica = spark.read.parquet(os.path.join(datasetFolder, "anagrafica_impianti_CT.parquet"))
    trainingDataset = spark.read.parquet(os.path.join(datasetFolder, "prezzi.parquet"))

    main(spark)

