import os
import pyspark.sql.types as tp
import pyspark.sql.functions as fun
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession




def removeParenthesis(string):
    string = string[1:-1]
    return string.split("\n", 2)[2]



#* DECLARE VARIABLES
topic = "prices"
kafkaServer = "kafkaServer:9092"


sparkConf = SparkConf()
sc = SparkContext(appName = "Clean", conf = sparkConf)
spark = SparkSession(sc)

schemaJSON = tp.StructType([
        tp.StructField(name= "@timestamp", dataType= tp.DateType(), nullable= False),
        tp.StructField(name = "event", dataType= 
                       tp.StructType([tp.StructField(name="original", dataType= tp.StringType(), nullable= False)])
                       ),
        tp.StructField(name= "hash", dataType= tp.StringType(), nullable= False),
        tp.StructField(name= "column1", dataType= tp.StringType(), nullable= False),
        tp.StructField(name= "@version", dataType= tp.IntegerType(), nullable= False)
    ])


datasetFolder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dataset")
anagrafica = spark.read.parquet(os.path.join(datasetFolder, "anagrafica_impianti_CT.parquet"))


#! --------------- READ STREAM FROM KAFKA -------------------

inputDF = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafkaServer) \
        .option("subscribe", topic) \
        .load()

df = inputDF.selectExpr("CAST(value AS STRING)") \
        .select(fun.from_json(fun.col("value"), schemaJSON).alias("data")) \
        .select("data.event", "data.hash", "data.@timestamp")
df = df.withColumnRenamed("@timestamp", "timestamp")
df = df.drop_duplicates(["hash"])

preClean = fun.udf(removeParenthesis, tp.StringType())
df.event = df.event.cast("string")
df = df.withColumn("event", preClean(df.event))

csv = df.select(fun.explode(fun.split(df.event, "\n")).alias("csv"))
csv = csv.select(fun.from_csv(fun.col("csv"),
                schema="idImpianto INT, descCarburante STRING, prezzo DOUBLE, isSelf INT, dtComu STRING",
                options={"sep" : ';'}).alias("data")).select("data.*")

csv = csv[csv.isSelf == 1]
csv = csv[csv.descCarburante.isin(["Benzina", "Gasolio"])]
csv = csv.withColumnRenamed("descCarburante", "carburante")
csv = csv.withColumnRenamed("idImpianto", "idImpiantoPrezzo")
csv = csv.join(anagrafica, csv.idImpiantoPrezzo == anagrafica.idImpianto, how="inner")
csv = csv[csv["Tipo Impianto"] == "Stradale"]

csv = csv.drop("__index_level_0__", "isSelf", "idImpianto", "Tipo Impianto", "Provincia", "dtComu", "idImpiantoPrezzo")

df.event = csv.select(fun.to_csv(
        fun.struct("carburante", "prezzo", "Gestore", "Bandiera", "Tipo Impianto",
                   "Nome Impianto", "Indirizzo", "Comune", "Latitudine", "longitudine"), options={"sep" : ";"}).alias("data"))

df.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()


spark.stop()
