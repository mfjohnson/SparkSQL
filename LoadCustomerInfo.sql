set set hive.exec.dynamic.partition.mode=nonstrict

USE default;
DROP TABLE IF EXISTS CUSTOMER_INFO1;
DROP TABLE IF EXISTS CUSTOMER_INFO1_ORC;

CREATE EXTERNAL TABLE CUSTOMER_INFO3 (
  transGroup int,
  customerId STRING,
  customerGroup STRING,
  CompanyName String,
  contactName String,
  EMAIL string,
  Address string,
  CustomerSince INT,
  AnnualSales FLOAT,
  LastOrderDate DATE
) row format delimited fields terminated by ","
location "/user/mjohnson/CustomerInfo";



