#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext
import re
stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
	stop_words_list = f.read().split('\n')

with open(delimitersPath) as f:
    delimiters = f.read().strip()
    delimiters = r"[{}]".format(re.escape(delimiters))
    print(delimiters)
    #TODO

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

words = lines.flatMap(lambda line: line.split(" ")).flatMap(lambda word: re.split(delimiters, word.lower()))
#TODO
filtered_words = words.filter(lambda word: word and word not in stop_words_list)
word_counts = filtered_words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)
top_10_words = word_counts.takeOrdered(10, key = lambda x:-x[1])

top_10_words = sorted(top_10_words, key = lambda x:(x[0],x[1]))
outputFile = open(sys.argv[4],"w")
for word in top_10_words:
    outputFile.write(f'{word[0]}\t{word[1]}\n')
outputFile.close()
#write results to output file. Foramt for each line: (line +"\n")

sc.stop()
