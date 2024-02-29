#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)
def split_line(line):
    right_pages_list = line.split(":")[1].strip().split(" ")
    left_page = line.split(":")[0]
    if left_page in right_pages_list:
        right_pages_list.remove(left_page)
    
    return right_pages_list
leagueIds = sc.textFile(sys.argv[2], 1)
lines = sc.textFile(sys.argv[1], 1) 
pages = lines.flatMap(lambda line: split_line(line))
page_count = pages.map(lambda page: (page,1)).reduceByKey(lambda count1, count2: count1 + count2)

#TODO
leagueIds = sc.textFile(sys.argv[2], 1).map(lambda line: (line, None))
league_page_count = page_count.join(leagueIds)
league_page_count = league_page_count.map(lambda x: (x[0], x[1][0]))
sorted_league_page_count = league_page_count.sortBy(lambda x: x[0])
#TODO

output = open(sys.argv[3], "w")
for league in sorted_league_page_count.collect():
    count = 0
    for league2 in sorted_league_page_count.collect():
       if league2[1] < league[1]:
        count = count + 1
       
    output.write(f'{league[0]}\t{count}\n')
    
#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

