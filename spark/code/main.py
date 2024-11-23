import os
import tempfile
from pyspark import SparkContext
from elasticsearch import Elasticsearch
from pyspark.sql.session import SparkSession

from clean import *
from predict import *



def addPrediction(DF, batchID):
    finalSchema = tp.StructType([
        tp.StructField(name="carburante", dataType=tp.IntegerType(), nullable=False),
        tp.StructField(name="prezzo", dataType=tp.DoubleType(), nullable=False),
        tp.StructField(name="idImpianto", dataType=tp.IntegerType(), nullable=False),
        tp.StructField(name="Gestore", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="Bandiera", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="Nome Impianto", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="Indirizzo", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="Comune", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="Latitudine", dataType=tp.FloatType(), nullable=False),
        tp.StructField(name="Longitudine", dataType=tp.FloatType(), nullable=False),
        tp.StructField(name="prediction", dataType=tp.DoubleType(), nullable=False),
    ])

    if DF.count() > 0:
        print("New batch arrived. BatchID:", batchID)
        df = DF.toPandas()
        df["prediction"] = 0.0

        # Cache = True  -> Load regressors from disk
        # Cache = False -> Train regressors and save them to disk
        #* ----------------------------------------------------------------
        assembler = VectorAssembler(inputCols=["prezzo"], outputCol="features")
        regressors = getRegressors(impianti, (batchID == 1), modelFolder=modelFolder, spark=spark, datasetFolder=os.path.join(datasetFolder, "prezzi"))
        print("Regressors READY")

        for index, row in df.iterrows():
            impianto = row["idImpianto"]
            carb = row["carburante"]
            regressor = regressors[impianto][carb]

            tempDF = spark.createDataFrame([[row["prezzo"]]], schema=tp.StructType([tp.StructField(name="prezzo", dataType=tp.FloatType(), nullable=False)]))
            tempDF = tempDF.drop("prediction")
            tempDF = assembler.transform(tempDF)
            tempDF = regressor.transform(tempDF)
            tempDF = tempDF.drop("features")

            df.at[index, "prediction"] = tempDF.collect()[0]["prediction"]
        #* ----------------------------------------------------------------

        toRet = spark.createDataFrame(df, schema=finalSchema)
        toRet = toRet.withColumn("@timestamp", fun.date_format(fun.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        toRet = toRet.withColumn("carburante", fun.when(toRet.carburante == 0, "Benzina").otherwise("Gasolio"))

        toRet = toRet.withColumn("Location", fun.array(toRet.Longitudine, toRet.Latitudine))
        toRet = toRet.drop("Latitudine", "Longitudine")
        print("Prediction DONE")

        toRet.write \
            .option("checkpointLocation", "/save/location") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", ELASTIC_INDEX) \
            .mode("append") \
            .format("es") \
            .save()
        print("Prediction added to ElasticSearch.")

        updateDataset(df, impianti, spark, os.path.join(datasetFolder, "prezzi"))
        print("Dataset UPDATED")



def initSpark():
    sc = SparkContext(appName = "OilPricePrediction")
    spark = SparkSession(sc).builder.appName("OilPricePrediction").getOrCreate()
    sc.setLogLevel("ERROR")

    sc.addPyFile(os.path.join(mainFolder, "clean.py"))
    sc.addPyFile(os.path.join(mainFolder, "predict.py"))
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

    df = cleanStreamingDF(inputDF, anagrafica)   #* DATA CLEANING AND DEDUPLICATION

    #* EXECUTE
    df.writeStream \
        .foreachBatch(addPrediction) \
        .start() \
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
                "carburante": {"type": "keyword"},
                "prezzo": {"type": "float"},
                "@timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                "prediction": {"type": "float"},
                "Gestore": {"type": "text"},
                "Bandiera": {"type": "keyword"},
                "Nome Impianto": {"type": "text"},
                "Indirizzo": {"type": "text"},
                "Comune": {"type": "keyword"},
                "Location": {"type": "geo_point"},
            },
        },
    }

    #*-----------------------------------------------------------------

    mainFolder = os.path.dirname(os.path.realpath(__file__))

    tempdir = tempfile.TemporaryDirectory()
    modelFolder = os.path.join(tempdir.name, "model")
    datasetFolder = os.path.join(mainFolder, "dataset")

    sc, spark = initSpark()
    es = createElasticIndex(ELASTIC_HOST, ELASTIC_INDEX, ES_MAPPING)
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    anagrafica = spark.read.parquet(os.path.join(datasetFolder, "anagrafica_impianti_CT.parquet"))
    impianti = [x.idImpianto for x in anagrafica.select("idImpianto").distinct().collect()]

    main(spark)
