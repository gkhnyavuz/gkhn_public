INSERT INTO DWH.REVIEW (
    reviewer_id,
    asin,
    helpful,
    review_text,
    overall,
    summary,
    reviewtime,
    ETL_DATE
)(
SELECT
    reviewerid,asin,helpful,reviewtext,overall,summary,TO_DATE(REPLACE(REPLACE(reviewtime,' ',''),',',''),'MMDDYYYY') as reviewtime,SYSDATE 
FROM
    ods.reviews  
group
    by reviewerid,asin,helpful,reviewtext,overall,summary,reviewtime 
);


------------

INSERT INTO DWH.product (
    id,
    name,
    category_id,
    price,
    imUrl,
    related,
    salesRank,
    brand,
    etl_date)(SELECT  asin,title,c.id as categoryid_id,price,imurl,related,salesRank,brand,SYSDATE
    FROM
        ods.metadata p left outer 
    join
        dwh.category c on p.categories=c.category
    group
        by asin,title,categoryid_id,price,imurl,related,salesRank,brand
);

-------------------------

INSERT INTO DWH.reviewer (
    id,
    name,
    ETL_DATE)(SELECT
           reviewerid,reviewername,SYSDATE 
    FROM
        ods.reviews 
    group
        by reviewerid,reviewername
);

--------------------

INSERT INTO dwh.dm_reviews 
(SELECT 
rw.asin as asin,
rw.reviewer_id as reviewer_id,
rwr.name as reviewer_name,
rwr.email_address as reviewer_email,
rw.helpful as helpful,
rw.review_text as review_text,
rw.overall as overall,
rw.summary as summary,
rw.reviewtime as review_time,
p.name as title,
p.price as price,
p.imurl as img_url,
p.salesrank as sales_rank,
p.brand as brand,
c.id as category_id,
c.category as category,
SYSDATE                                              
FROM dwh.review rw 
left outer join dwh.product p on rw.asin = p.id
left outer join dwh.reviewer rwr on rw.reviewer_id = rwr.id
left outer join dwh.category c on p.category_id=c.id
 );    

INSERT INTO dwh.dm_product_price_bucket
(
SELECT
p.id,
p.name,  
p.brand,
CASE WHEN p.price > (pb.avg_price + pb.avg_price*(acceptable_range/100)) THEN 'HIGH'
WHEN  p.price < (pb.avg_price - pb.avg_price*(acceptable_range/100)) THEN 'LOW' ELSE 'NORMAL' END as price_bucket,  
p.category_id,
c.category,
p.price,
pb.avg_price,
sysdate                                              
from dwh.product p 
inner join dwh.category c on p.category_id=c.id
inner join dwh.price_bucket pb on p.name=pb.title and p.brand=pb.brand
where 
CAST(SYSDATE AS DATE) between pb.creation_date and NVL(pb.closure_date,TO_DATE('31.12.2050','DD.MM.YYYY'))         
);    
               

-------------------------------

INSERT INTO DWH.PRICE_BUCKET
(SELECT 
name,
BRAND,
NULL,
10,
TO_DATE('01.01.1900','DD.MM.YYYY'),
NULL,
1 
FROM dwh.product p
where price IS NOT NULL
AND name IS NOT NULL
AND brand IS NOT NULL
group by  
name,
BRAND
)
----------------

update dwh.price_bucket
set avg_price = (SELECT AVG(p.price) FROM dwh.product p
where p.name=title and p.brand=brand
and price is not null 
and brand is not null
and name is not null                 
group by name,BRAND)