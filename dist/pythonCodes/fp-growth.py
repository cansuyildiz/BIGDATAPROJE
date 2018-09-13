# -*- coding: utf-8 -*-
"""
Created on Fri May 18 10:44:39 2018

@author: cansu.yildiz
"""

from __future__ import print_function
import argparse
import h2o
import time
from math import sqrt

import findspark 
from pyspark import SparkConf, SparkContext 
from pysparkling import *
from pyspark.sql import SparkSession

from pyspark.sql.functions import split
from pyspark.ml.fpm import FPGrowth
from functools import reduce

def parseData(sc,data_filepath):
    data = (spark.read.text(data_filepath).select(split("value", "\s+").alias("items")))
    #data.show(truncate=False)
    
    return data

def evaluate(data, minsupport, minconfidence):
    fp = FPGrowth(minSupport=minsupport, minConfidence=minconfidence)
    fpm = fp.fit(data)
    
    return fpm

def show(fpGrowth):
    fpGrowth.freqItemsets.show(100,truncate=False)
    fpGrowth.associationRules.show(100)
    
if __name__ == "__main__": 

    parser = argparse.ArgumentParser(description='FP-Growth on Sparkling Water')
    parser.add_argument('--dataset', type=str, default="mush.txt")
    parser.add_argument('--minSupport', type=float, default=0.5)
    parser.add_argument('--minConfidence', type=float, default=0.5)

    args = parser.parse_args()    

    data_filepath= "hdfs://localhost:9000/user/cansu/" + args.dataset   
    minSupport = args.minSupport
    minConfidence = args.minConfidence
    
    #Initiate System----------------------------------------------
    findspark.init()
    #conf = SparkConf().setAppName("FP-Growth").setMaster("local[*]").set("spark.ui.port", "7077").set("spark.executor.memory", "10g").set("spark.driver.memory", "10g")
    #conf = SparkConf().setAppName("FP-Growth").setMaster("spark://192.168.34.225:7077").set("spark.ui.port", "9000").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    conf = SparkConf().setAppName("FP-Growth").setMaster("local[*]").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    #Initiate Spark Context
    sc = SparkContext(conf=conf)
    #Initiate Spark Session
    spark = SparkSession.builder.getOrCreate()
    #Initiate Sparkling Water Context
    h2oContext = H2OContext.getOrCreate(spark)
    h2oContext.show()
    #------------------------------------------------------------
    
    transactions = parseData(sc, data_filepath)
    
    duration=[]
    for i in range(2):
        t1=time.time()
        evaluate(transactions, minSupport, minConfidence)
        t2=time.time()
        duration.append(t2-t1)
    
    
    result = evaluate(transactions, minSupport, minConfidence)
    #show(result)
    
    sizeOfDuration=len(duration)
    #calculates avarage duration
    mean = reduce(lambda x, y: x + y, duration) / sizeOfDuration
    
    #calculates standart devaiation of durations
    differences= [x-mean for x in duration]
    sq_differences = [d**2 for d in differences]
    std= sqrt(sum(sq_differences)/(sizeOfDuration-1))
    
    print("mean= ",mean,"\nStandart Deviation= ",std)
    
    h2oContext.stop()
    sc.stop()

    file = open("fp-growth.txt","w") 
 
    file.write("mean = "+ str(mean)) 
    file.write("\nstandart deviation = " + str(std)) 
    #file.write("\nperformance = " + performance) 
 
    file.close() 


