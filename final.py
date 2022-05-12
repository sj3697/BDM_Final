import csv
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import SparkContext

#!pip install pyproj
from pyproj import Transformer
import shapely
from shapely.geometry import Point
import pandas as pd


def readPlacekey(partId, part):
  if partId == 0: next(part)
  for x in csv.reader(part):
    yield x[9]


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



def readPatterns_2019_10(partId, part):
  t = Transformer.from_crs(4326, 2263)
  if partId == 0: next(part)
  for x in csv.reader(part):
    if x[0] in outputSuermarket:
      if '2019-11-01' > x[12] >= '2019-10-01' or '2019-11-01' > x[13] >= '2019-10-01':
        temp = ast.literal_eval(x[19])
        id = int(x[18])
        for key in temp:
          k = int(key)
          if k in outputCBG:
            a = t.transform(outputCBG[k][0],outputCBG[k][1])
            b = t.transform(outputCBG[id][0],outputCBG[id][1])
            dis = Point(a).distance(Point((b)))/5280
            yield k, dis*temp[key], temp[key]


def readPatterns_2020_03(partId, part):
  t = Transformer.from_crs(4326, 2263)
  if partId == 0: next(part)
  for x in csv.reader(part):
    if x[0] in outputSuermarket:
      if '2020-04-01' > x[12] >= '2020-03-01' or '2020-04-01' > x[13] >= '2020-03-01':
        temp = ast.literal_eval(x[19])
        id = int(x[18])
        for key in temp:
          k = int(key)
          if k in outputCBG:
            a = t.transform(outputCBG[k][0],outputCBG[k][1])
            b = t.transform(outputCBG[id][0],outputCBG[id][1])
            dis = Point(a).distance(Point((b)))/5280
            yield k, dis*temp[key], temp[key]



def readPatterns_2020_10(partId, part):
  t = Transformer.from_crs(4326, 2263)
  if partId == 0: next(part)
  for x in csv.reader(part):
    if x[0] in outputSuermarket:
      if '2020-11-01' > x[12] >= '2020-10-01' or '2020-11-01' > x[13] >= '2020-10-01':
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
    spark = SparkSession()
    df = pd.read_csv('nyc_cbg_centroids.csv')
    outputCBG = df.set_index('cbg_fips').T.to_dict('list')

    CBG = 'nyc_cbg_centroids.csv'
    final = spark.read.load(CBG, format='csv', header=True, inferSchema=True)
    final = final.select(final['cbg_fips'].alias('cbg'))
    
    outputSuermarket = sc.textFile('nyc_supermarkets.csv') \
                .mapPartitionsWithIndex(readPlacekey).collect()

    df = pd.read_csv('nyc_cbg_centroids.csv')
    outputCBG = df.set_index('cbg_fips').T.to_dict('list')

    CBG = 'nyc_cbg_centroids.csv'
    final = spark.read.load(CBG, format='csv', header=True, inferSchema=True)
    final = final.select(final['cbg_fips'].alias('cbg'))
    #final.show()
    
    # 2019_03
    output2019_03 = sc.textFile('/tmp/bdm/weekly-patterns-nyc-2019-2020') \
              .mapPartitionsWithIndex(readPatterns)
    #output2019_03.count()
    deptColumns = ["cbg","dis","count"]
    df_2019_03 = output2019_03.toDF(deptColumns)
    df_2019_03 = df_2019_03.groupBy('cbg').sum('dis', 'count')
    df_2019_03 = df_2019_03.withColumn('2019_03', (df_2019_03[1]/df_2019_03[2])).select('cbg', '2019_03')
    final = final.join(df_2019_03, on = 'cbg', how = 'left')
    #final.show()

    #2019_10
    output2019_10 = sc.textFile('/tmp/bdm/weekly-patterns-nyc-2019-2020') \
              .mapPartitionsWithIndex(readPatterns_2019_10)
    #output2019_10.count()

    deptColumns = ["cbg","dis","count"]
    df_2019_10 = output2019_10.toDF(deptColumns)
    df_2019_10 = df_2019_10.groupBy('cbg').sum('dis', 'count')
    df_2019_10 = df_2019_10.withColumn('2019_10', (df_2019_10[1]/df_2019_10[2])) \
        .select('cbg', '2019_10')
    final = final.join(df_2019_10, on = "cbg", how='left')
    #final.show()

    #2020_03
    output2020_03 = sc.textFile('/tmp/bdm/weekly-patterns-nyc-2019-2020') \
              .mapPartitionsWithIndex(readPatterns_2020_03)
    #output2020_03.count()

    deptColumns = ["cbg","dis","count"]
    df_2020_03 = output2020_03.toDF(deptColumns)
    df_2020_03 = df_2020_03.groupBy('cbg').sum('dis', 'count')
    df_2020_03 = df_2020_03.withColumn('2020_03', (df_2020_03[1]/df_2020_03[2])) \
        .select('cbg', '2020_03')
    final = final.join(df_2020_03, on = "cbg", how='left').cache()
    #final.show()

    #2020_10
    output2020_10 = sc.textFile('/tmp/bdm/weekly-patterns-nyc-2019-2020') \
              .mapPartitionsWithIndex(readPatterns_2020_10)
    #output2020_10.count()

    deptColumns = ["cbg","dis","count"]
    df_2020_10 = output2020_10.toDF(deptColumns)
    df_2020_10 = df_2020_10.groupBy('cbg').sum('dis', 'count')
    df_2020_10 = df_2020_10.withColumn('2020_10', (df_2020_10[1]/df_2020_10[2])) \
        .select('cbg', '2020_10')
    final = final.join(df_2020_10, on = "cbg", how='left').cache()
    #final.show()

    final = final.orderBy('cbg', ascending=True)
    final.saveAsTextFile('Final')
    #final.show()
if __name__ == '__main__':
  sc = SparkContext()
  main(sc)
