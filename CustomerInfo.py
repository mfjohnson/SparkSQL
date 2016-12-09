from pyspark import HiveContext
from pyspark import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark import *
import os
import tempfile
os.chdir(tempfile.mkdtemp())
conf = SparkConf().setMaster("yarn-client").set("spark.executor.memory", "512m").setAppName("SparkSQL").set("spark.eventLog.enabled",False)
conf.set("spark.home","/usr/hdp/2.5.0.0-1245/spark")
sc = SparkContext(conf=conf)

sqlContext = HiveContext(sc)
