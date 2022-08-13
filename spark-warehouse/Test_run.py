from pyspark.sql import SparkSession

import pyspark.sql.functions as F
# Building the spark session to run SPARK
spark = SparkSession.builder.master("local").appName("csv data").config("spark.driver.extraClassPath","C:\\work\\Drivers\\ojdbc7.jar").getOrCreate()
data="C:\\work\\dataset\\us-500.csv" # input data
df = spark.read.format("csv").option("header","true").load(data)
df.createOrReplaceTempView("tab")
res = spark.sql("select * from tab where state = 'NY'")
res.show()
