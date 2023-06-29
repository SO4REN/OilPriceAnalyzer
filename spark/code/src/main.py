import os
import pyspark.sql.types as tp
import pyspark.sql.functions as fun
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession




sparkConf = SparkConf()
sc = SparkContext(appName = "Clean", conf = sparkConf)
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

sc.addFile(os.path.join(os.path.dirname(os.path.realpath(__file__)), "clean.py"))
sc.addFile(os.path.join(os.path.dirname(os.path.realpath(__file__)), "predict.py"))

from clean import *
from predict import *



schemaJSON = tp.StructType([
        tp.StructField(name="@timestamp", dataType=tp.TimestampType(), nullable=False),
        tp.StructField(name="event", 
                       dataType=tp.StructType([tp.StructField(name="original", dataType=tp.StringType(), nullable=False)])
                       ),
        tp.StructField(name="hash", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="column1", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="@version", dataType=tp.IntegerType(), nullable=False)
    ])

topic = "prices"
kafkaServer = "kafkaServer:9092"

datasetFolder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dataset")
anagrafica = spark.read.parquet(os.path.join(datasetFolder, "anagrafica_impianti_CT.parquet"))
impiantiFiles = os.path.join(datasetFolder, "Impianti", "")



#* GET STREAMING INPUT DATAFRAME
inputDF = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafkaServer) \
        .option("subscribe", topic) \
        .load()

#* DATA CLEANING
df = cleanStreamingDF(inputDF, anagrafica, schemaJSON)

#* DATA PREDICTION
df = predictStreamingDF(df, spark, impiantiFiles, anagrafica)




df.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()
spark.stop()
