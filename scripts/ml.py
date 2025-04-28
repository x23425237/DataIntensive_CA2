#%%
import loadSparkSession as load
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col


#%% load the dataset 

datafolder='C:/Users/prana/DataIntensive_CA2/Kaggle/driver_standings.csv'
data = load.spark.read.csv(datafolder, header=True, inferSchema=True)

data.printSchema()
data.show()

#%% extract the features 

feature_columns = ['points', 'position']
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

data = assembler.transform(data)


#%%

data = data.select("features", "wins")



#%%
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)




#%%
lr = LinearRegression(labelCol="wins", featuresCol="features")

model = lr.fit(train_data)




#%%
predictions = model.transform(test_data)

#%%
evaluator = RegressionEvaluator(labelCol='wins', predictionCol='prediction', metricName='rmse')
rmse = evaluator.evaluate(predictions)

print(f"Root Mean Squared Error (RMSE): {rmse}")
predictions.select('wins', 'prediction').show(300)


# %%
predictions.filter(predictions.prediction > 0.8).select('wins', 'prediction').show()




# %%
# Average prediction by actual wins
predictions.groupBy("wins").avg("prediction").show()



# %%

