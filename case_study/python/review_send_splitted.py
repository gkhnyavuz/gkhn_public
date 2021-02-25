import pandas as pd
import gzip
from urllib.request import urlopen
import awswrangler as wr

url='https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/item_dedup.json.gz'


def parse(path):
  g =  gzip.open(urlopen(path), 'rb') 
  for l in g:
    yield eval(l)

def getDF(path):
  i = 0
  df = {}
  for d in parse(path):
    df[i] = d
    i += 1
    if (i%1000000)==0 :
       dfreview=pd.DataFrame.from_dict(df, orient='index')
       dfreview['helpful']=dfreview.helpful.astype(str)
       spath=("s3://gokhansbucket1/case_study/parquets/review/review_%d.parquet" %i)
       wr.s3.to_parquet(
          df=dfreview,
          path=spath
       )
       df.clear()
       
   
  dfreview=pd.DataFrame.from_dict(df, orient='index')
  dfreview['helpful']=dfreview.helpful.astype(str)
  spath="s3://gokhansbucket1/case_study/parquets/review/review_last.parquet"
  wr.s3.to_parquet(
     df=dfreview,
     path=spath
  )     

getDF(url)
