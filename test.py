import csv
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import SparkContext
import pandas as pd

def main(sc):
    spark = SparkSession(sc)
    df = pd.read_csv('nyc_cbg_centroids.csv')
    outputCBG = df.set_index('cbg_fips').T.to_dict('list')

    #CBG = 'nyc_cbg_centroids.csv'
    final = spark.read.load('nyc_cbg_centroids.csv', format='csv', header=True, inferSchema=True)
    final = final.select(final['cbg_fips'].alias('cbg'))
    final.saveAsTextFile('test')

if __name__ == '__main__':
  sc = SparkContext()
  main(sc)
