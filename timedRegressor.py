import pyspark
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import time

def merge_list(predictors):
    output = [str(x) for x in predictors]
    result = ""
    for string in output:
        result += string + " "
    return result

#load data from DB
#db_connection = mysql.connector.connect(user="Group3", password="Group3", host="localhost")
#db_cursor = db_connection.cursor()
#db_cursor.execute("USE usedCarsDB")
#query = "SELECT year,price FROM usedCarsData;"
#db_cursor.execute(query)
#data = []
#for row in db_cursor:
#    data.append(row)
#    print(row)

config = pyspark.SparkConf().setAll([('spark.executor.instances', '1'), ('spark.executor.cores', '1')])

#load data from csv
spark = SparkSession.builder.config(conf=config).appName("ReadCSV").getOrCreate()

#spark.conf.set("spark.executor.instances", 4)
#spark.conf.set("spark.executor.cores", 4)

df = spark.read.option("header","true").csv("used_cars_data.csv")

#print(df.take(1))
df.printSchema()
df_2 = df.select(df['mileage'].cast("double"), df['city_fuel_economy'].cast("double"),\
        df['engine_displacement'].cast("double"), df['horsepower'].cast("double"),\
        df['year'].cast("double"), df['owner_count'].cast("double"),\
        df['daysonmarket'].cast("double"), df['price'].cast("double"))

df_2 = df_2.dropna()

print(df_2.take(5))

#print(data[5])
#assemble selected features into ['features','price']
#vectorAssembler = VectorAssembler(inputCols = ['','',''], outputCol = 'features')
#vec_car_df = vectorAssembler.transform(data)
#vec_car_df = vec_car_df.select(['features', 'price'])
#vec_car_df.show(3)

#train/test split(train_data, test_data) = df_2.randomSplit([0.7, 0.3])
predictors=['mileage','city_fuel_economy','engine_displacement','horsepower',\
        'year','owner_count','daysonmarket']
vec_assembler = VectorAssembler(inputCols=predictors, outputCol = 'features')
vec_train_df = vec_assembler.transform(df_2)
vec_train_df.select("features", "price").show(5)

(train_data, test_data) = vec_train_df.randomSplit([0.7, 0.3])

#print(train_data.count())
#print(test_data.count())

#vectorAssembler to turn list of column names into list??

#create regressor object
lr = LinearRegression(featuresCol = 'features', labelCol = 'price', maxIter=10, regParam=0.3, elasticNetParam=0.8)


tic = time.perf_counter()

#fit model to training data
lr_model = lr.fit(train_data)

toc = time.perf_counter()
print(f"trained LR model with 1  executor in {toc - tic:0.4f} seconds.")

lr_model.save("used_cars_lr.model")

#results
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

lr_predictions = lr_model.transform(test_data)
lr_predictions.select("prediction","price","features").show(5)

#store results in db
#db_connection = mysql.connector.connect(user="Group3", password="Group3", host="localhost")
#db_cursor = db_connection.cursor()
#db_cursor.execute("USE usedCarsDB;")

#db_cursor.execute("CREATE TABLE predNormalizedLR(predictions DOUBLE, price DOUBLE, predictors VARCHAR(200));")

#predicted_data = lr_predictions.collect()

#for row in predicted_data:
#    db_cursor.execute("INSERT INTO predNormalizedLR VALUES(" + str(row["prediction"])\
#            + "," + str(row["price"]) + ",\'" + merge_list(row["features"]) + "\');")
#    db_connection.commit()





