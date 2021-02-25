create external schema ods 
from data catalog 
database 'spectrumdb' 
iam_role 'arn:aws:iam::121025365734:role/myRedshiftRole';

CREATE EXTERNAL TABLE ods.reviews(
reviewerID varchar(4000),
asin varchar(4000),
reviewerName varchar(4000),
helpful varchar(10) ,
reviewText varchar(4000),
overall double precision,
summary varchar(4000),
reviewTime varchar(4000)
)
STORED AS PARQUET
LOCATION 's3://gokhansbucket1/case_study/parquets/review/'; 

CREATE EXTERNAL TABLE ods.metadata(
asin varchar(4000),
title varchar(4000),
price double precision,
imUrl varchar(4000),
brand varchar(4000),
categories varchar(4000),
related  varchar(4000),
salesrank varchar(4000)  
)
STORED AS PARQUET
LOCATION 's3://gokhansbucket1/case_study/parquets/metadata/'; 


CREATE TABLE dwh.review (
reviewer_id varchar(4000),
asin varchar(4000),
helpful varchar(4000),
review_text varchar(4000),
overall double precision,
summary varchar(4000),
reviewTime date,
etl_date DATE
)

CREATE TABLE dwh.product(
id varchar(4000) ,
name varchar(4000),
category_id int,
price double precision,
imUrl varchar(4000),
related varchar(4000),
salesRank varchar(4000),
brand varchar(4000),
ETL_DATE DATETIME
);

CREATE TABLE dwh.category (
id int,
category varchar(4000),
creation_date date,
closure_date date,
is_active int
)

create table dwh.price_bucket(
title VARCHAR(4000),
brand VARCHAR(4000),
category_id int,
avg_price double precision,
acceptable_range double precision,
creation_date date,
closure_date date,
is_active int 
)

CREATE TABLE dwh.reviewer(
id varchar(4000),
name varchar(4000),
email_address varchar(4000),
ETL_DATE DATETIME  
)

Ref: R.reviewer_id - RU.id
Ref: P.category_id - PC.id
Ref: PB.category_id - PC.id
Ref: R.asin - P.id

CREATE TABLE dwh.dm_reviews (
review_id int,
asin varchar(4000),
reviever_id varchar(4000),
reviewer_name varchar(4000),
reviever_email varchar(4000),
helpful varchar(4000),
review_text varchar(4000),
overall int,
summary varchar(4000),
review_time date,
title varchar(4000),
price double precision,
img_url varchar(4000),
sales_rank int,
brand varchar(4000),
category_id int,
category varchar(4000),
price_bucket varchar(4000),
ETL_DATE DATETIME 
)

CREATE TABLE dwh.dm_product_price_bucket(
asin varchar(4000),
title varchar(4000),
brand varchar(4000),   
price_bucket varchar(4000),
category_id int,
category varchar(4000),
price double precision,
avg_price double precision,
etl_date datetime  
);
