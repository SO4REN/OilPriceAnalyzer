import pyspark.sql.types as tp
import pyspark.sql.functions as fun




def removeBloat(string):
    string = string[1:-1]
    return string.split("\n", 2)[2]


def cleanDF(df, anagrafica):
    df = df[df.isSelf == 1]
    df = df[df.descCarburante.isin(["Benzina", "Gasolio"])]
    df = df.withColumnRenamed("descCarburante", "carburante")
    df = df.withColumn("carburante", fun.when(df.carburante == "Benzina", 0).otherwise(1))
    df = df.withColumnRenamed("idImpianto", "idImpiantoPrezzo")
    df = df.join(anagrafica, df.idImpiantoPrezzo == anagrafica.idImpianto, how="inner")
    df = df[df["Tipo Impianto"] == "Stradale"]
    
    df = df.withColumn("Location", fun.array(df.Longitudine, df.Latitudine))
    
    df = df.drop("Latitudine", "Longitudine")
    df = df.drop("__index_level_0__", "isSelf", "Tipo Impianto", "Provincia", "dtComu", "idImpiantoPrezzo")
    return df



def cleanStreamingDF(inputDF, anagrafica):
    schemaJSON = tp.StructType([
        tp.StructField(name="@timestamp", dataType=tp.TimestampType(), nullable=False),
        tp.StructField(name="event", 
                       dataType=tp.StructType([tp.StructField(name="original", dataType=tp.StringType(), nullable=False)])
                       ),
        tp.StructField(name="hash", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="column1", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="@version", dataType=tp.IntegerType(), nullable=False)
    ])
    
    df = inputDF.selectExpr("CAST(value AS STRING)") \
            .select(fun.from_json(fun.col("value"), schemaJSON).alias("data")) \
            .select("data.event", "data.hash", "data.@timestamp")
    df = df.withColumnRenamed("@timestamp", "timestamp")
    df = df.drop_duplicates(["hash"]) #! DEBUG

    preClean = fun.udf(removeBloat, tp.StringType())
    df.event = df.event.cast("string")
    df = df.withColumn("event", preClean(df.event))

    outputDF = df.select(fun.explode(fun.split(df.event, "\n")).alias("df"), "timestamp")
    outputDF = outputDF.select(fun.from_csv(fun.col("df"),
                    schema="idImpianto INT, descCarburante STRING, prezzo DOUBLE, isSelf INT, dtComu STRING",
                    options={"sep" : ';'}).alias("data"), "timestamp").select("data.*", "timestamp")

    outputDF = cleanDF(outputDF, anagrafica)
    outputDF = outputDF.withColumn("timestamp", fun.to_timestamp(outputDF.timestamp, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    outputDF = outputDF.withColumnRenamed("timestamp", "original_timestamp")
    
    return outputDF
