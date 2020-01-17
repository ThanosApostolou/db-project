"""
 Counts words in new text files created in the given directory
 Usage: hdfs_wordcount.py <directory>
   <directory> is the directory that Spark Streaming will use to find and read new text files.

 To run this on your local machine on directory `localdir`, run this example
    $ bin/spark-submit examples/src/main/python/streaming/hdfs_wordcount.py localdir

 Then create a text file in `localdir` and the words in the file will get counted.
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def flatmapper (line) :
    return line.split(" ")

def mapper (word) :
    return (word, 1)

def reducer (a, b) :
    return a+b

if __name__ == "__main__":
    sc = SparkContext(appName="MR")

    text_file = sc.textFile("hdfs://master:9000/test.txt")

    counts = text_file.flatMap(flatmapper).map(mapper).reduceByKey(reducer)
    print ("Printing counts\n")
    counts.print()
    print ("\n")
    print("Saving to file Q1-MR-out.txt\n")
    counts.saveAsTextFile("hdfs://master:9000/Q1-MR-out.txt")
