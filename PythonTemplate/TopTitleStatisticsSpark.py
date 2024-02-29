#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext
import re 
conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)
numbers = lines.map(lambda line: int(line.split("\t")[1]))

total = numbers.sum()
count = numbers.count()
mean = total / count
min = numbers.min()
max = numbers.max()

# 计算方差: E[(X - μ)²] = E[X²] - E[X]²
mean_square = (numbers.map(lambda x: x ** 2).sum()) / count
variance = mean_square - mean ** 2
print(numbers)

#TODO

outputFile = open(sys.argv[2], "w")


outputFile.write('Mean\t%s\n' % int(mean))
outputFile.write('Sum\t%s\n' % total)
outputFile.write('Min\t%s\n' % min)
outputFile.write('Max\t%s\n' % max)
outputFile.write('Var\t%s\n' % int(variance))

sc.stop()