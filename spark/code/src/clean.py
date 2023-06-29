import pyspark.sql.types as tp
import pyspark.sql.functions as fun




def removeBloat(string):
    string = string[1:-1]
    return string.split("\n", 2)[2]


def cleanDF(df, anagrafica):
    df = df[df.isSelf == 1]
    df = df[df.descCarburante.isin(["Benzina", "Gasolio"])]
    df = df.withColumnRenamed("descCarburante", "carburante")
    df = df.withColumnRenamed("idImpianto", "idImpiantoPrezzo")
    df = df.join(anagrafica, df.idImpiantoPrezzo == anagrafica.idImpianto, how="inner")
    df = df[df["Tipo Impianto"] == "Stradale"]

    df = df.drop("__index_level_0__", "isSelf", "Tipo Impianto", "Provincia", "dtComu", "idImpiantoPrezzo")
    return df



def cleanStreamingDF(inputDF, anagrafica, schemaJSON):
    df = inputDF.selectExpr("CAST(value AS STRING)") \
            .select(fun.from_json(fun.col("value"), schemaJSON).alias("data")) \
            .select("data.event", "data.hash", "data.@timestamp")
    df = df.withColumnRenamed("@timestamp", "timestamp")
    df = df.drop_duplicates(["hash"])

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
