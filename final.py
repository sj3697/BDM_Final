import csv
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql import types as T
from pyspark import SparkContext
from pyspark.sql import Window
import pandas as pd
import ast

from pyproj import Transformer
import shapely
from shapely.geometry import Point



def main(sc,sqlcontext):
    df = pd.read_csv('nyc_cbg_centroids.csv')
    outputCBG = df.set_index('cbg_fips').T.to_dict('list')
    

    df_s = pd.read_csv('nyc_supermarkets.csv')
    outputSupermarket = df_s['safegraph_placekey'].to_numpy()

    def readPatterns_2(partId, part):
      t = Transformer.from_crs(4326, 2263)
      if partId == 0: next(part)
      for x in csv.reader(part):
        if x[0] in outputSupermarket:
          if '2019-04-01' > x[12] >= '2019-03-01' or '2019-04-01' > x[13] >= '2019-03-01':
            temp = ast.literal_eval(x[19])
            id = int(x[18])
            for key in temp:
              k = int(key)
              if k in outputCBG:
                a = t.transform(outputCBG[k][0],outputCBG[k][1])
                b = t.transform(outputCBG[id][0],outputCBG[id][1])
                dis = Point(a).distance(Point((b)))/5280
                for i in range(temp[key]):
                  yield k, dis*temp[key]
                  
      output2_2019_03 = sc.textFile('/tmp/bdm/weekly-patterns-nyc-2019-2020') \
              .mapPartitionsWithIndex(readPatterns_2)
      
##      deptColumns = ["cbg","dis"]
##      df2_2019_03 = output2_2019_03.toDF(deptColumns)
##      med = f.expr('percentile_approx(dis,0.5)')
##      df2_2019_03 = df2_2019_03.groupBy('cbg').agg(med.alias('2019_03'))

      
      #df2_2019_03.write.option("header", True).csv(sys.argv[1])
      output2_2019_03.saveAsTextFile('extra')
      
if __name__ == '__main__':
  sc = SparkContext()
  sqlcontext=SQLContext(sc)
  main(sc,sqlcontext)


