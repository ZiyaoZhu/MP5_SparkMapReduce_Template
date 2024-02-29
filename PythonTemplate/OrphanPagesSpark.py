#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

def split_line(line):
    right_pages_list = line.split(":")[1].strip().split(" ")
    left_page = line.split(":")[0].strip()
    # while left_page in right_pages_list:
    #     right_pages_list.remove(left_page)
    return right_pages_list

lines = sc.textFile(sys.argv[1], 1) 
left_pages = lines.map(lambda line: line.split(":")[0].strip())
right_pages = lines.flatMap(lambda line: split_line(line))

# left_page_set = set(left_pages.collect())
# right_page_set = set(right_pages.collect())

orphan_pages = left_pages.subtract(right_pages)
# int_orphan_pages = [int(i) for i in orphan_pages.collect()]

# orphan_pages = left_page_set.difference(right_page_set)
# int_orphan_pages = [int(i) for i in orphan_pages]
sorted_orphans = sorted(orphan_pages.collect())

output = open(sys.argv[2], "w")
for page in sorted_orphans:
    output.write('%s\n' % page)
    
sc.stop()

