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

tableName = "CUSTOMER_INFO3"

#----------------------------
sc = SparkContext(appName = "WriteRDD")
hc = HiveContext(sc)


#----------------------------
rowCountPerGroup = 10
rowDef = Row("transGroup","CustomerId","companyGroup","CompanyName","Name","EMAIL","Address","CustomerSince","AnnualSales","LastOrderDate")
fake = Factory.create()



#----------------------------
def random_date(start, end):
    secRange = randrange(0, (end - start).total_seconds(),1)
    d = start + datetime.timedelta(secRange)
    return(unicode(d))



def buildCustomerRDD(i) :
    d1 = datetime.date(randrange(2008,2016,1),randrange(1,12,1),1)
    d2 = datetime.date(2016,12,31)
    row = rowDef(i, str(uuid.uuid4()), choice(["GroupA","GroupB","GroupC"]),fake.company(), fake.name(), fake.email(), "ABC",randrange(1990,2016,1), randrange(1000,10000,1000),d1)
    return(row);

#---------------- Begin Main Processing
a = sc.parallelize(range(1,1000))
rddRows = a.map(lambda i: buildCustomerRDD(i) )
print("----------- Using the predefined schema")

print("----------- Using the inferred schema")
df = hc.inferSchema(rddRows)
df = hc.createDataFrame(rddRows)

df.printSchema()
print("----------- \n\n\n\n")


#hc.sql("CREATE TABLE CUSTOMER_INFO3 (transGroup int,customerId STRING, CompanyName String,contactName String, EMAIL string,Address string,CustomerSince INT, AnnualSales FLOAT,LastOrderDate DATE);")

df.show()
print("----------------------")

print (df.count())

df.write.format("orc").mode("overwrite").saveAsTable(tableName)
#def toCSVLine(data):
#  return ','.join(str(d) for d in data)
#
#lines = df.map(toCSVLine)
#lines.saveAsTextFile('CustomerInfo')
