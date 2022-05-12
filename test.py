import csv
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import SparkContext
import pandas as pd

def main(sc):
    def readPlacekey(partId, part):
    if partId == 0: next(part)
    for x in csv.reader(part):
        yield x[9]

    outputSuermarket = sc.textFile('nyc_supermarkets.csv') \
                .mapPartitionsWithIndex(readPlacekey).collect()
    
    outputSuermarket.saveAsTextFile('test')

if __name__ == '__main__':
  sc = SparkContext()
  #spark = SparkSession(sc)
  main(sc)
