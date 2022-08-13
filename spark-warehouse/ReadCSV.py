from pyspark.sql import SparkSession

import pyspark.sql.functions as F
# Building the spark session to run SPARK
spark = SparkSession.builder.master("local").appName("csv data").config("spark.driver.extraClassPath","C:\\work\\Drivers\\ojdbc7.jar").getOrCreate()
data="C:\\work\\dataset\\us-500.csv" # input data
# Created dataframe with the data present in the file. read.format will read
df = spark.read.format("csv").option("header","true").load(data)
# Create a temp table for the dataframe "df".
df.createOrReplaceTempView("tab")
host = "jdbc:oracle:thin:@//paritoshoracle.czhyzqz8vleo.ap-south-1.rds.amazonaws.com:1521/orcl"
# run the spark sql.
res=spark.sql("select * from tab where email like '%gmail%'")
res.write.format("jdbc").option("user","ousername").option("password","opassword").option("driver","oracle.jdbc.OracleDriver").option("dbtable","dt_tabls").option("url",host).save()
df.printSchema()
res.show()
