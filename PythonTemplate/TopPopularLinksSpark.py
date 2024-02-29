#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def split_line(line):
    right_pages_list = line.split(":")[1].strip().split(" ")
    left_page = line.split(":")[0]
    if left_page in right_pages_list:
        right_pages_list.remove(left_page)
    
    return right_pages_list

pages = lines.flatMap(lambda line: split_line(line))
page_count = pages.map(lambda page: (page,1)).reduceByKey(lambda count1, count2: count1 + count2)
top_10_page = page_count.takeOrdered(10, key = lambda x:-x[1])
top_10_page = sorted(top_10_page, key = lambda x:x[0])
outputFile = open(sys.argv[2], "w")
for page in top_10_page:
    outputFile.write(f'{page[0]}\t{page[1]}\n')
sc.stop()

