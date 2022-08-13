#jdbc:mysql://mysql.conbyj3qndaj.ap-south-1.rds.amazonaws.com:3306/venutasks
from pyspark.sql import SparkSession

import sys
import pyspark.sql.functions as F
#spark = SparkSession.builder.appName("Write_To_Oracle").config("spark.driver.extraClassPath","C:\\work\\Drivers\\ojdbc7.jar").config("spark.executor.extraClassPath","C:\\work\\Drivers\\ojdbc7.jar").getOrCreate()
spark = SparkSession.builder.appName("Write_To_Oracle").config("spark.driver.extraClassPath","C:\\work\\Drivers\\ojdbc7.jar").getOrCreate()
host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
host2 = "jdbc:mysql://mysql.conbyj3qndaj.ap-south-1.rds.amazonaws.com:3306/venutasks"
#df=spark.read.format("jdbc").option("user","ousername").option("password","opassword").option("driver","oracle.jdbc.OracleDriver").option("dbtable","EMP").option("url",host).load()
op="C:\\work\\dataset\\test_drop_tab"
#query = "select * from EMP where sal>2500 and ename in (select ename from emp where ename like '%S%')"
query = "select * from emp"
df=spark.read.format("jdbc").option("user","ousername").option("password","opassword").option("driver","oracle.jdbc.OracleDriver").option("query",query).option("url",host).load()
df2=spark.read.format("jdbc").option("user","ousername").option("password","opassword").option("driver","com.mysql.jdbc.Driver").option("dbtable","dept").option("url",host2).load()
df2.createOrReplaceTempView("dept_temp")
df.createOrReplaceTempView("emp_temp")
join=spark.sql("select * from dept_temp join emp_temp on dept_temp.deptno = emp_temp.deptno")
#df.show()
#df.write.format("csv").option("header","true").save(op)
op=sys.argv[1]
join.write.format("csv").option("header","true").save(op)

spark.stop()
