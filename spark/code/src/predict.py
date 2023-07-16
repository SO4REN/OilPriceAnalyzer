import pyspark.sql.functions as fun
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler, OneHotEncoder



def train(trainDF):
    encoder = OneHotEncoder(inputCols=["idImpianto"], outputCols=["X_idImpianto"])
    model = encoder.fit(trainDF)
    df = model.transform(trainDF)
    
    assembler = VectorAssembler(inputCols=["X_prezzo", "X_idImpianto"], outputCol="features")
    df = assembler.transform(df)

    lr = DecisionTreeRegressor(featuresCol="features", labelCol="Y_prezzo", predictionCol="prediction")
    regressor = lr.fit(df)
    return regressor


def predictStreamingDF(df, trainDF):    
    toRet = None
    assembler = VectorAssembler(inputCols=["prezzo", "X_idImpianto"], outputCol="features")
    
    for carb in (0, 1):
        tempTrainDF = trainDF.filter(trainDF.carburante == carb)
        regressor = train(tempTrainDF)

        tempDF = df.filter(df.carburante == carb)

        encoder = OneHotEncoder(inputCols=["idImpianto"], outputCols=["X_idImpianto"])
        model = encoder.fit(tempTrainDF)
        tempDF = model.transform(tempDF)

        tempDF = assembler.transform(tempDF)
        tempDF = regressor.transform(tempDF)

        tempDF = tempDF.drop("X_idImpianto", "features")

        toRet = tempDF if toRet is None else toRet.union(tempDF)

    toRet = toRet.withColumn("@timestamp", fun.date_format(fun.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    toRet = toRet.withColumn("carburante", fun.when(toRet.carburante == 0, "Benzina").otherwise("Gasolio"))
    return toRet
