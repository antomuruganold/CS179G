import pandas
import pyspark
import mysql.connector
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadCSV").getOrCreate() 
df = spark.read.option("header","true").csv("used_cars_data.csv")
drop_columns = ['bed','bed_height', 'bed_length','cabin','combine_fuel_economy', \
                'is_certified', 'fleet', 'frame_damaged','has_accidents','isCab',\
                'is_cpo','salvage','theft_title', 'vehicle_damage_category','description','major_options']
df = df.drop(*drop_columns)

db_connection = mysql.connector.connect(user="Group3", password="Group3", host="localhost")
db_cursor = db_connection.cursor()
db_cursor.execute("USE usedCarsDB;")

df.printSchema()

#df.write.csv('usedCarsDataCleaned.csv')

var_names = []

for f in df.schema.fields:
    var_names.append(f.name)
    print(var_names[-1])
#
#for name in var_names:
#   print(name+":")
#   df.select(name).show()

df.printSchema()
df_2 = df.select(df['mileage'].cast("double"), df['city_fuel_economy'].cast("double"),\
        df['engine_displacement'].cast("double"), df['horsepower'].cast("double"),\
        df['year'].cast("double"), df['owner_count'].cast("double"),\
        df['daysonmarket'].cast("double"), df['price'].cast("double"))
df_2.printSchema()


predictors=['mileage','city_fuel_economy','engine_displacement','horsepower',\
                'year','owner_count','daysonmarket']
vec_assembler = VectorAssembler(inputCols=predictors, outputCol = 'feature_vec')
vec_train_df = vec_assembler.transform(df_2)
vec_train_df.select(["feature_vec", "price"]).show(5)


#filepath = "final_used_cars.csv/part-00060-c6bff23b-34a7-4b55-9748-7a85f5af9fd2-c000.csv"
#df = spark.read.option("header","true").csv(filepath)
#example code to add to a DB
#db_cursor.execute("CREATE TABLE usedCarsData(vin VARCHAR(50) NOT NULL UNIQUE);")

#for i in range(1,len(var_names)):
#    db_cursor.execute("ALTER TABLE usedCarsData MODIFY COLUMN " + var_names[i] + " VARCHAR(300);")

#db_cursor.execute("ALTER TABLE usedCarsData DROP COLUMN description")
#db_cursor.execute("ALTER TABLE usedCarsData DROP COLUMN major_options ")

#for i in range (60,75):
#
#    df = spark.read.option("header","true").csv(filepath)
#    dataCollect = df.collect()
#
#    old_value = filepath[28:30]
#    new_value = str(int(old_value)+1)
#    filepath = filepath.replace(old_value,new_value,1)

#    for row in dataCollect:
        
#        insert_command = "INSERT INTO usedCarsData("
#        values_added = "VALUES("
        
#        for i in range(0,len(var_names)):
#            insert_command += var_names[i] + ","
#            values_added += "\'" + str(row[i]) + "\'" + ","
        
#        insert_command = insert_command[:-1] + ") "
#        values_added = values_added[:-1]+ ");"
#        print(insert_command)
#        print(values_added)
        
#        command = insert_command + values_added;
        
#        try:
#            db_cursor.execute(command)
#            db_connection.commit()
#        except:
#            db_connection.rollback()
