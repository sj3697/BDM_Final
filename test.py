import csv
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import SparkContext
import pandas as pd

def main(sc):
    spark = SparkSession(sc)
    final = pd.read_csv('nyc_cbg_centroids.csv')
    final = final[['cbg_fips']]
    final=spark.createDataFrame(final)
    
    final.saveAsTextFile('test')

if __name__ == '__main__':
  sc = SparkContext()
  #spark = SparkSession(sc)
  main(sc)
