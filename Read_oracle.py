from pyspark.sql import SparkSession

import pyspark.sql.functions as F
#spark = SparkSession.builder.appName("Write_To_Oracle").config("spark.driver.extraClassPath","C:\\work\\Drivers\\ojdbc7.jar").config("spark.executor.extraClassPath","C:\\work\\Drivers\\ojdbc7.jar").getOrCreate()
spark = SparkSession.builder.appName("Write_To_Oracle").config("spark.driver.extraClassPath","C:\\work\\Drivers\\ojdbc7.jar").getOrCreate()
host = "jdbc:oracle:thin:@//sriorc.c2b8qdd13kl2.ap-south-1.rds.amazonaws.com:1521/ORCL"
#df=spark.read.format("jdbc").option("user","ousername").option("password","opassword").option("driver","oracle.jdbc.OracleDriver").option("dbtable","EMP").option("url",host).load()
op="C:\\work\\dataset\\test_drop_tab"
#query = "select * from EMP where sal>2500 and ename in (select ename from emp where ename like '%S%')"
query = "select * from tab"
df=spark.read.format("jdbc").option("user","ousername").option("password","opassword").option("driver","oracle.jdbc.OracleDriver").option("query",query).option("url",host).load()
#df2=spark.read.format("jdbc").option("user","ousername").option("password","opassword")

df.show()
df.write.format("csv").option("header","true").save(op)
