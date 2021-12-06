import pyspark
import psycopg2
from psycopg2 import Error
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.ml.regression import LinearRegression
import pyspark.sql.functions as func
import time

def merge_list(predictors):
    output = [str(x) for x in predictors]
    result = ""
    for string in output:
        result += string + " "
    return result

#load data from DB
db_connection = psycopg2.connect(host="localhost", database="usedCars", user="group3", password="group3")
db_cursor = db_connection.cursor()
#db_cursor.execute("CREATE TABLE Predicted_Prices_FINAL(VIN VARCHAR(17) UNIQUE, Model VARCHAR(50), prediction NUMERIC(1000,2), price NUMERIC, difference NUMERIC(5,2), predictors VARCHAR(200), make VARCHAR(50), Age INTEGER)")
#db_cursor.execute("COMMIT")
#load data from csv
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

df = spark.read.option("header","true").csv("used_cars_data.csv")

df.printSchema()
df_2 = df.select(df['vin'],df['make_name'], df['model_name'], df['mileage'].cast("double"), df['city_fuel_economy'].cast("double"),\
        df['engine_displacement'].cast("double"), df['horsepower'].cast("double"),\
        df['year'].cast("double"), df['owner_count'].cast("double"),\
        df['daysonmarket'].cast("double"), df['price'].cast("double"))

df_2 = df_2.dropna()
df_2 = df_2.withColumn('age', 2021-df_2.year)
print(df_2.take(5))


normCols=['mileage','city_fuel_economy','engine_displacement','horsepower'] # Columns to be scaled


unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())    # Convert columns from vector to double

for i in normCols:

    assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
    scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
    pipeline = Pipeline(stages=[assembler, scaler])
    df_2 = pipeline.fit(df_2).transform(df_2).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")
predictors=['mileage_Scaled','city_fuel_economy_Scaled','engine_displacement_Scaled','horsepower_Scaled',\
           'age','owner_count','daysonmarket']

vec_assembler = VectorAssembler(inputCols=predictors, outputCol = 'features')
vec_train_df = vec_assembler.transform(df_2)
vec_train_df.select("features", "price").show(5)

(train_data, test_data) = vec_train_df.randomSplit([0.7, 0.3])

lr = LinearRegression(featuresCol = 'features', labelCol = 'price', maxIter=10, regParam=0.3, elasticNetParam=0.8)


tic = time.perf_counter()

lr_model = lr.fit(train_data)

toc = time.perf_counter()
print(f"trained LR model with default executors in {toc - tic:0.4f} seconds.")

#results
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

lr_predictions = lr_model.transform(test_data)
lr_predictions = lr_predictions.withColumn('perc_diff', func.round((1 - func.abs((lr_predictions.prediction-lr_predictions.price)/lr_predictions.price))*100, 2))
lr_predictions.select("vin","model_name","prediction","price","perc_diff").show(5)
#lr_predictions.withColumn('perc_diff', 1 - (lr_predictions.prediction-lr_predictions.price)/lr_predictions.price)

#store results in db

predicted_data = lr_predictions.collect()
#predicted_data = predicted_data.withColumn('perc_diff', round((1 - abs((predicted_data.price-predicted_data.prediction)/predicted_data.price)) * 100, 2))
for row in predicted_data:
    try:
        print("INSERT INTO Predicted_Prices_FINAL VALUES(\'" + str(row["vin"]) + "\',\'" + str(row["model_name"]) + "\'," + str(row["prediction"]) + "," + str(row["price"]) +  "," + str(row["perc_diff"]) + ",\'" + merge_list(row["features"]) + "\',\'" + row["make_name"] + "\'," + str(row['age']) + ");")
        db_cursor.execute("INSERT INTO Predicted_Prices_FINAL VALUES(\'" + str(row["vin"]) + "\',\'" + str(row["model_name"]) + "\'," + str(row["prediction"]) + "," + str(row["price"]) +  "," + str(row["perc_diff"]) + ",\'" + merge_list(row["features"]) + "\',\'" + row["make_name"] + "\'," + str(row['age']) + ");")
        db_cursor.execute("COMMIT;")
    except Exception:
        pass




