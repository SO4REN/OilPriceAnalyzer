import pyspark.sql.functions as fun
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler, OneHotEncoder



def train(trainDF):
    encoder = OneHotEncoder(inputCols=["idImpianto", "carburante"], outputCols=["X_idImpianto", "X_carburante"])
    model = encoder.fit(trainDF)
    df = model.transform(trainDF)
    
    assembler = VectorAssembler(inputCols=["X_prezzo", "X_idImpianto", "X_carburante"], outputCol="features")
    df = assembler.transform(df)

    lr = DecisionTreeRegressor(featuresCol="features", labelCol="Y_prezzo", predictionCol="prediction")
    regressor = lr.fit(df)
    return regressor


def predictStreamingDF(df, trainDF):
    regressor = train(trainDF)

    encoder = OneHotEncoder(inputCols=["idImpianto", "carburante"], outputCols=["X_idImpianto", "X_carburante"])
    model = encoder.fit(trainDF)
    df = model.transform(df)

    assembler = VectorAssembler(inputCols=["prezzo", "X_idImpianto", "X_carburante"], outputCol="features")
    df = assembler.transform(df)

    df = regressor.transform(df)
    df = df.drop("features")
    df = df.drop("X_idImpianto", "X_carburante")
    df = df.withColumn("timestampPrediction", fun.current_timestamp())
    return df
