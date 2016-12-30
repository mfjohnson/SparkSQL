from pyspark import HiveContext

from pyspark import *
from pyspark import Row


from faker import Factory
from random import randrange,randint,choice
import uuid
import datetime
import os


print (os.getlogin())
print (os.getenv("SPARK_HOME","spark home"))

#----------------------------
sc = SparkContext(appName = "AtlasBuilder")
hc = HiveContext(sc)
tableList = hc.tableNames()

for tableName in tableList:
    print("*******   Processing Table {0}".format(tableName))
    df = hc.table(tableName)
    cols = df.schema.fields
    print(cols)

