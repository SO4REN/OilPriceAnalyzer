import os
import pyspark.sql.types as tp
import pyspark.sql.functions as fun
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel



def train(trainDF):
     assembler = VectorAssembler(inputCols=["X_prezzo"], outputCol="features")
     df = assembler.transform(trainDF)

     lr = LinearRegression(featuresCol="features", labelCol="Y_prezzo", maxIter=100, regParam=0.3, elasticNetParam=0.8)
     regressor = lr.fit(df)
     return regressor


def getRegressors(impianti, cache = False, **kwargs):
    spark = kwargs["spark"] if "spark" in kwargs else None
    modelFolder = kwargs["modelFolder"] if (cache and "modelFolder" in kwargs) else ""
    datasetFolder = kwargs["datasetFolder"] if (not cache and "datasetFolder" in kwargs) else ""

    regressors = dict.fromkeys(impianti)
    for key in regressors:
        regressors[key] = dict.fromkeys((0, 1))

    if cache and len(os.listdir(modelFolder)) > 0:
        for impianto in impianti:
            for carb in (0, 1):
                regressors[impianto][carb] = LinearRegressionModel.load(os.path.join(modelFolder, str(impianto) + "_" + str(carb)))
    else:
        print("Training regressors...")
        for impianto in impianti:
            trainingDataset = spark.read.parquet(os.path.join(datasetFolder, str(impianto)))
            for carb in (0, 1):
                trainDF = trainingDataset.filter(trainingDataset.carburante == carb)
                regressors[impianto][carb] = train(trainDF)
                regressors[impianto][carb].write().overwrite().save(os.path.join(modelFolder, str(impianto) + "_" + str(carb)))

    return regressors


def updateDataset(df, impianti, spark, datasetFolder):
    for impianto in impianti:
        trainingDataset = spark.read.parquet(os.path.join(datasetFolder, str(impianto)))
        
        for carb in (0, 1):
            lastRow = trainingDataset.filter(trainingDataset.carburante == carb).orderBy(fun.desc("insertOrder")).limit(1)
            lastRow = lastRow.withColumn("insertOrder", lastRow.insertOrder + 1)
            lastRow = lastRow.withColumn("X_prezzo", lastRow.Y_prezzo)

            data = df[(df.idImpianto == impianto) & (df.carburante == carb)].prezzo.values[0] if not df[(df.idImpianto == impianto) & (df.carburante == carb)].empty else 0.0
            lastRow = lastRow.withColumn("Y_prezzo", fun.lit(data).cast(tp.DoubleType()))
            lastRow.write.mode("append").parquet(os.path.join(datasetFolder, str(impianto)))
    
    print("Dataset updated!")
