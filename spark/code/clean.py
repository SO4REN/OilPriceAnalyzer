import os
import pyspark.sql.types as tp
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json, col, udf


# def cleanCSV(data):
#     schema = tp.StructType([
#         tp.StructField(name= "idImpianto", dataType= tp.IntegerType(), nullable= False),
#         tp.StructField(name= "descCarburante", dataType= tp.StringType(), nullable= False),
#         tp.StructField(name= "prezzo", dataType= tp.FloatType(), nullable= False),
#         tp.StructField(name= "isSelf", dataType= tp.IntegerType(), nullable= False),
#         tp.StructField(name= "dtComu", dataType= tp.DateType(), nullable= False)
#     ])
    
#     datasetFolder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dataset")
#     anagrafica = spark.read.parquet(os.path.join(datasetFolder, "anagrafica_impianti_CT.parquet"))
    
#     prezziDF = spark.read.csv(data, schema= schema, sep=";")
#     prezziDF = prezziDF[prezziDF.isSelf == 1]
#     prezziDF = prezziDF[prezziDF.descCarburante.isin(["Benzina", "Gasolio"])]
#     prezziDF = prezziDF.withColumnRenamed("descCarburante", "carburante")
#     prezziDF = prezziDF.withColumnRenamed("idImpianto", "idImpiantoPrezzo")

#     df = prezziDF.join(anagrafica, prezziDF.idImpiantoPrezzo == anagrafica.idImpianto, how="inner")
#     df = df.drop("__index_level_0__")
#     df = df.drop("isSelf")
#     df = df.drop("idImpiantoPrezzo")
#     df = df.drop("dtComu")



#* DECLARE VARIABLES
topic = "prices"
kafkaServer = "kafkaServer:9092"


sparkConf = SparkConf()
sc = SparkContext(appName = "Clean", conf = sparkConf)
spark = SparkSession(sc)

schema = tp.StructType([
        tp.StructField(name= "@timestamp", dataType= tp.DateType(), nullable= False),
        tp.StructField(name = "event", dataType= tp.StructType([tp.StructField(name="original", dataType= tp.StringType(), nullable= False)])),
        tp.StructField(name= "hash", dataType= tp.StringType(), nullable= False),
        tp.StructField(name= "column1", dataType= tp.StringType(), nullable= False),
        tp.StructField(name= "@version", dataType= tp.IntegerType(), nullable= False)
    ])

inputDF = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafkaServer) \
        .option("subscribe", topic) \
        .load()

df = inputDF.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.event", "data.hash", "data.@timestamp")
df = df.withColumnRenamed("@timestamp", "timestamp")


df.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()









# i = 0
# hashes = dict.fromkeys(range(0, 101), "")

# files = glob.glob(os.path.join(datasetFolder, "*.csv"))
# files.sort()

# for csv in files:
#     hash = hashlib.sha256(open(csv, "rb").read()).hexdigest()
#     if hash not in hashes.values():
#         hashes[i % 101] = hash
#         cleanCSV(os.path.basename(csv))
#         i += 1

spark.stop()
