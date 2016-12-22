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

tableName = "CUSTOMER_INFO4"

#----------------------------
sc = SparkContext(appName = "WriteRDD")
hc = HiveContext(sc)


#----------------------------
rowCountPerGroup = 10
rowDef = Row("transGroup","CustomerId","companyGroup","CompanyName","contactName","EMAIL","Address","CustomerSince","AnnualSales","LastOrderDate")
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

def defineFile(n):
    tableName = "CustomerInfo{0}".format(n)
    print("       ************   Source RDD:{0}".format(tableName))
    return(tableName)

def createAndPopulateTable(n) :
    tableName = defineFile(n)
    print("--------- started table #{0}".format(n))
    rddList = []
    for i in range(1,10):
         r = buildCustomerRDD(i)
         rddList.append(r);



    df = hc.createDataFrame(rddList)
    df.write.format("orc").mode("overwrite").saveAsTable(tableName)
    print("---------- ORC Table: {0}".format(tableName));
    return(rddList)

def testMapCall(s):
    print("********  {0}".format(s));

#---------------- Begin Main Processing
if __name__ =='__main__':
    tableCount = sc.parallelize(range(1,5))
    maxRowCount = sc.parallelize(range(1,20))

    for i in range(1,1000):
        createAndPopulateTable(i)

    print("*********               DONE")

