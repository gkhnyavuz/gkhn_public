#!/bin/bash

python /metadata_send_splitted.py
python /review_send_splitted.py

aws redshift-data execute-statement \
    --database <dev> \
    --cluster-identifier <examplecluster> \
    --secret-arn <arn:aws:iam::121025365734:role/myRedshiftRole>  \
    --sql "CALL DWH.REDSHIFT_ETL();" \