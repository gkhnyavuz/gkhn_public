import pandas as pd
import gzip
from urllib.request import urlopen
import awswrangler as wr

url='https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz'


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
       dfmetadata=pd.DataFrame.from_dict(df, orient='index')
       dfmetadata['categories'] = dfmetadata['categories'].str[0].str[0]
       dfmetadata['categories'] = dfmetadata.categories.astype(str)
       dfmetadata['salesRank']=dfmetadata.salesRank.astype(str)
       dfmetadata['related']=dfmetadata.related.astype(str)
       spath=("s3://gokhansbucket1/case_study/parquets/metadata/metadata_%d.parquet" %i)
       wr.s3.to_parquet(
          df=dfmetadata,
          path=spath
       )
       df.clear()
       
   
  dfmetadata=pd.DataFrame.from_dict(df, orient='index')
  dfmetadata['categories'] = dfmetadata['categories'].str[0].str[0]
  dfmetadata['categories'] = dfmetadata.categories.astype(str)
  dfmetadata['salesRank']=dfmetadata.salesRank.astype(str)
  dfmetadata['related']=dfmetadata.related.astype(str)
  spath="s3://gokhansbucket1/case_study/parquets/metadata/metadata.last"
  wr.s3.to_parquet(
     df=dfmetadata,
     path=spath
  )     

getDF(url)
