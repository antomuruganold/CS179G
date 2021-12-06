import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col,lit

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

df = spark.read.option("header","true").csv("used_cars_data.csv")
df_2 = df.select(df['vin'],df['make_name'],df['model_name'],df['mileage'].cast("double"), df['city_fuel_economy'].cast("double"),\
        df['engine_displacement'].cast("double"), df['horsepower'].cast("double"),\
        df['year'].cast("double"), df['owner_count'].cast("double"),\
        df['daysonmarket'].cast("double"), df['price'].cast("double"))

df_2 = df_2.dropna()
df_2 = df_2.withColumn('age', 2021-df_2.year)

print("Before Scaling :")
df_2.show(5)

normCols=['mileage','city_fuel_economy','engine_displacement','horsepower'] # Columns to be scaled


unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())    # Convert columns from vector to double

for i in normCols:

    assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
    scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
    pipeline = Pipeline(stages=[assembler, scaler])
    df_2 = pipeline.fit(df_2).transform(df_2).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")

print("After Scaling :")
df_2.show(5)
