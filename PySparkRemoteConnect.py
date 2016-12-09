#!/public/spark-0.9.1/bin/pyspark

import os
import sys

# Set the path for spark installation
# this is the path where you have built spark using sbt/sbt assembly
os.environ['SPARK_HOME'] = "/usr/local/lib/spark-1.6.2-bin-hadoop2.6"
# os.environ['SPARK_HOME'] = "/home/jie/d2/spark-0.9.1"
# Append to PYTHONPATH so that pyspark could be found
sys.path.append("/usr/local/lib/spark-1.6.2-bin-hadoop2.6/python")
# sys.path.append("/home/jie/d2/spark-0.9.1/python")

# Now we are ready to import Spark Modules
try:
    from pyspark import SparkContext
    from pyspark import SparkConf

except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

import numpy as np



if __name__ =='__main__':
    conf=SparkConf()
#    conf.setMaster("spark://server2.hdp:7777")
    conf.setMaster("yarn-client")
    # conf.setMaster("local")
    conf.setAppName("spark_svm")
#    conf.set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)

