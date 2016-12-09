set set hive.exec.dynamic.partition.mode=nonstrict

USE default;
DROP TABLE IF EXISTS CUSTOMER_INFO1;
DROP TABLE IF EXISTS CUSTOMER_INFO1_ORC;

CREATE EXTERNAL TABLE CUSTOMER_INFO3 (
  transGroup int,
  customerId STRING,
  CompanyName String,
  contactName String,
  EMAIL string,
  Address string,
  CustomerSince INT,
  AnnualSales FLOAT,
  LastOrderDate DATE
) row format delimited fields terminated by ","
location "/user/mjohnson/CustomerInfo";




CREATE TABLE CUSTOMER_INFO1_ORC (
  customerId STRING,
  CompanyName String,
  contactName String,
  EMAIL string,
  Address string,
  CustomerSince INT,
  AnnualSales FLOAT,
  LastOrderDate DATE
) PARTITIONED BY (TRANSGROUP INT) STORED AS ORC;
INSERT OVERWRITE TABLE CUSTOMER_INFO_ORC PARTITION (TRANSGROUP)
  SELECT COMPANYNAME, CONTACTNAME, EMAIL, ADDRESS, CUSTOMERSINCE,ANNUALSALES, LASTORDERDATE FROM CUSTOMER_INFO1;

