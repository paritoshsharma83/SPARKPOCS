from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

import pyspark.sql.functions as f

spark = SparkSession.builder.master("local").appName("Read_json").getOrCreate()

a = spark.read.option("multiline","true").json(r"C:\work\dataset\json\fwdjsondataresources\monthlySalesbyCategoryMultiple.json")
a.printSchema()