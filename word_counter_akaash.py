
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



# Updated original wordcount.py script for COP 6526 Homework 2 Assignment 

from __future__ import print_function

import sys
import re
from operator import add
from itertools import permutations

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.map(lambda x:x.lower()) \
                  .map(lambda x: x.encode('ascii')) \
                  .map(lambda x:re.sub(r"[,.;'@#?!&$]+\ *",'',x)) \
                  .map(lambda x:re.sub('[0-9]', '', x)) \
                  .map(lambda x: x.split(' ')) \
                  .map(lambda x: permutations(x,2)) \
                  .flatMap(lambda x: x) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)

    output = counts.collect()
    counts.saveAsTextFile("/output/HW2_FINAL.txt")
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()