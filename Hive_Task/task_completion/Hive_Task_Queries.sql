-- Creating and loading tables
CREATE EXTERNAL TABLE classicmodels.customers (
    customer_id INT,
    business_name STRING,
    last_name STRING,
    first_name STRING,
    phone_number STRING,
    address_line1 STRING,
    address_line2 STRING,
    city STRING,
    state STRING,
    postal_code STRING,
    country STRING,
    country_code INT,
    credit_limit DECIMAL(15,2)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\u0001"
)
STORED AS TEXTFILE
LOCATION '/user/hue/classicmodels.db/customers';

CREATE EXTERNAL TABLE classicmodels.employees (
    employee_id INT,
    last_name STRING,
    first_name STRING,
    extension STRING,
    email STRING,
    office_code INT,
    reports_to INT,
    job_title STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\u0001"
)
STORED AS TEXTFILE
LOCATION '/user/hue/classicmodels.db/employees';

CREATE EXTERNAL TABLE classicmodels.offices (
    office_code INT,
    city STRING,
    phone STRING,
    address_line1 STRING,
    address_line2 STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    territory STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\u0001"
)
STORED AS TEXTFILE
LOCATION '/user/hue/classicmodels.db/offices';

CREATE EXTERNAL TABLE classicmodels.orderdetails (
    order_number INT,
    product_code STRING,
    quantity_ordered INT,
    price_each DECIMAL(10,2),
    order_line_number INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\u0001"
)
STORED AS TEXTFILE
LOCATION '/user/hue/classicmodels.db/orderdetails';

CREATE EXTERNAL TABLE classicmodels.payments (
    customer_number INT,
    check_number STRING,
    payment_date DATE,
    amount DECIMAL(10,2)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\u0001"
)
STORED AS TEXTFILE
LOCATION '/user/hue/classicmodels.db/payments';

CREATE EXTERNAL TABLE classicmodels.products (
    product_code STRING,
    product_name STRING,
    product_line STRING,
    product_scale STRING,
    product_vendor STRING,
    product_description STRING,
    quantity_in_stock INT,
    buy_price DECIMAL(10,2),
    msrp DECIMAL(10,2)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\u0001"
)
STORED AS TEXTFILE
LOCATION '/user/hue/classicmodels.db/products';

---------------------------------------------------------------------------------------------------

select * from classicmodels.employees
limit 10
;

---------------------------------------------------------------------------------------------------

select employee_id, first_name, last_name 
from classicmodels.employees
where employee_id between 1002 and 1100
order by last_name desc
limit 5
;

---------------------------------------------------------------------------------------------------

SELECT
  job_title,
  COUNT(*) AS employees_count
FROM classicmodels.employees
GROUP BY job_title
ORDER BY employees_count DESC
;

---------------------------------------------------------------------------------------------------

SHOW CREATE TABLE classicmodels.employees;

---------------------------------------------------------------------------------------------------

DESCRIBE EXTENDED classicmodels.employees;

---------------------------------------------------------------------------------------------------

select * from newdb.new_emp;

---------------------------------------------------------------------------------------------------

CREATE TABLE newdb.new_emp (
    employee_id INT,
    last_name STRING,
    first_name STRING,
    extension STRING,
    email STRING,
    office_code INT,
    reports_to INT,
    job_title STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\u0001"
)
STORED AS TEXTFILE
LOCATION '/user/hue/classicmodels.db/employees';

CREATE TABLE newdb.offices (
    office_code INT,
    city STRING,
    phone STRING,
    address_line1 STRING,
    address_line2 STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    territory STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\u0001"
)
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hue/classicmodels.db/offices'
INTO TABLE newdb.offices;

ALTER TABLE newdb.offices SET TBLPROPERTIES('EXTERNAL'='TRUE');


DROP TABLE newdb.offices;

DESCRIBE EXTENDED newdb.offices;

---------------------------------------------------------------------------------------------------

select country, count(*) as customers_n from classicmodels.customers
group by country
;

---------------------------------------------------------------------------------------------------

set hive.exec.dynamic.partition.mode=nonstrict;

show create table classicmodels.customers;

CREATE EXTERNAL TABLE `classicmodels.cust_country`(
  `customer_id` string COMMENT 'from deserializer', 
  `business_name` string COMMENT 'from deserializer', 
  `last_name` string COMMENT 'from deserializer', 
  `first_name` string COMMENT 'from deserializer', 
  `phone_number` string COMMENT 'from deserializer', 
  `address_line1` string COMMENT 'from deserializer', 
  `address_line2` string COMMENT 'from deserializer', 
  `city` string COMMENT 'from deserializer', 
  `state` string COMMENT 'from deserializer', 
  `postal_code` string COMMENT 'from deserializer', 
  `country_code` string COMMENT 'from deserializer', 
  `credit_limit` string COMMENT 'from deserializer'
)
PARTITIONED BY (country string)
STORED AS AVRO;

INSERT INTO TABLE classicmodels.cust_country
PARTITION (country)
SELECT
  customer_id,
  business_name,
  last_name,
  first_name,
  phone_number,
  address_line1,
  address_line2,
  city,
  state,
  postal_code,
  country_code,
  credit_limit,
  country
FROM classicmodels.customers
LIMIT 50
;

explain dependency select * from classicmodels.cust_country
where country = 'USA'
;

---------------------------------------------------------------------------------------------------

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE classicmodels.my_emp (
  id     INT,
  name   STRING,
  salary INT
)
CLUSTERED BY (id) INTO 5 BUCKETS
STORED AS ORC
TBLPROPERTIES (
  'transactional' = 'true'
);

DESCRIBE EXTENDED classicmodels.my_emp;

INSERT INTO my_emp VALUES
  (1, 'John', 10000),
  (2, 'Sara', 12000),
  (3, 'Adam', 8000)
;

UPDATE my_emp
SET salary = 9000
WHERE name = 'Adam';

INSERT INTO my_emp VALUES (4, 'Alex', 13000);

DELETE FROM my_emp
WHERE name = 'John';

select * from classicmodels.my_emp;

DESCRIBE EXTENDED classicmodels.offices;

---------------------------------------------------------------------------------------------------