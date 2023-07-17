import os
import pyspark.sql.functions as fun
from pyspark.ml.regression import DecisionTreeRegressionModel
from pyspark.ml.feature import VectorAssembler, OneHotEncoderModel




def predictStreamingDF(df, modelFolder):
    toRet = None
    assembler = VectorAssembler(inputCols=["prezzo", "X_idImpianto"], outputCol="features")
    encoder = OneHotEncoderModel.load(os.path.join(modelFolder, "encoder"))
    
    for carb in (0, 1):
        regressor = DecisionTreeRegressionModel.load(os.path.join(modelFolder, "regressor_{carb}".format(carb=("benzina" if carb == 0 else "gasolio"))))
        
        tempDF = df.filter(df.carburante == carb)
        tempDF = encoder.transform(tempDF)
        tempDF = assembler.transform(tempDF)
        tempDF = regressor.transform(tempDF)

        tempDF = tempDF.drop("X_idImpianto", "features")
        toRet = tempDF if toRet is None else toRet.union(tempDF)

    toRet = toRet.withColumn("@timestamp", fun.date_format(fun.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    toRet = toRet.withColumn("carburante", fun.when(toRet.carburante == 0, "Benzina").otherwise("Gasolio"))
    return toRet
