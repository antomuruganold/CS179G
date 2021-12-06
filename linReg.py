import pandas
import pyspark
import mysql.connector
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
df = spark.read.option("header","true").csv("final_used_cars.csv/part-00001-c6bff23b-34a7-4b55-9748-7a85f5af9fd2-c000.csv")

print((df.count(), len(df.columns)))
db_connection = mysql.connector.connect(user="Group3", password="Group3")
db_cursor = db_connection.cursor()
db_cursor.execute("USE usedCarsDB;")

var_names = []

#example code to add to a DB
#db_cursor.execute("CREATE TABLE cleanCarData (vin VARCHAR(50) NOT NULL UNIQUE);")

for i in range(1,len(var_names)):
    db_cursor.execute("ALTER TABLE cleanCarData ADD COLUMN " + var_names[i] + " VARCHAR(200);")

db_cursor.execute("ALTER TABLE cleanCarData DROP COLUMN description")
db_cursor.execute("ALTER TABLE cleanCarData DROP COLUMN major_options ")

dataCollect = df.collect()

for row in dataCollect:
    insert_command = "INSERT INTO cleanCarData "
    values_added = "VALUES("
    for name in var_names:
        values_added += "\'" + str(row[name]) + "\'" + ","
    values_added = values_added[:-1]+ ");"
    print(insert_command)
    print(values_added)
    command = insert_command + values_added;
    db_cursor.execute(command)

