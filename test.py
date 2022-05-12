import csv
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import SparkContext
import pandas as pd

from pyproj import Transformer
import shapely
from shapely.geometry import Point


def readPatterns(partId, part):
  t = Transformer.from_crs(4326, 2263)
  if partId == 0: next(part)
  for x in csv.reader(part):
    if x[0] in outputSuermarket:
      if '2019-04-01' > x[12] >= '2019-03-01' or '2019-04-01' > x[13] >= '2019-03-01':
        temp = ast.literal_eval(x[19])
        id = int(x[18])
        for key in temp:
          k = int(key)
          if k in outputCBG:
            a = t.transform(outputCBG[k][0],outputCBG[k][1])
            b = t.transform(outputCBG[id][0],outputCBG[id][1])
            dis = Point(a).distance(Point((b)))/5280
            yield k, dis*temp[key], temp[key]

def main(sc):

    df = pd.read_csv('nyc_cbg_centroids.csv')
    outputCBG = df.set_index('cbg_fips').T.to_dict('list')
    
    final = pd.read_csv('nyc_cbg_centroids.csv')
    final = final[['cbg_fips']]
    final = final.rename({'cbg_fips':'cbg'}, axis=1)

    df_s = pd.read_csv('nyc_supermarkets.csv')
    outputSuermarket = df_s['safegraph_placekey'].to_numpy()

    output2020_03 = sc.textFile('/tmp/bdm/weekly-patterns-nyc-2019-2020') \
              .mapPartitionsWithIndex(readPatterns_2020_03)

    deptColumns = ["cbg","dis","count"]
    df_2019_03 = output2019_03.toDF(deptColumns)

    df_2019_03 = df_2019_03.groupBy('cbg').sum('dis', 'count')
    df_2019_03 = df_2019_03.withColumn('2019_03', (df_2019_03[1]/df_2019_03[2])).select('cbg', '2019_03')
    df_2019_03 = df_2019_03.toPandas()
    final = final.merge(df_2019_03, on = 'cbg', how = 'left')
    
    final.saveAsTextFile('test')

if __name__ == '__main__':
  sc = SparkContext()
  #spark = SparkSession(sc)
  main(sc)
