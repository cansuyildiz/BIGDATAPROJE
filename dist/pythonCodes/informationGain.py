# -*- coding: utf-8 -*-
"""
Created on Tue May 08 10:14:49 2018

@author: cansu.yildiz
"""
from __future__ import print_function

import argparse
import h2o
import pandas as pd
import findspark
from pyspark import SparkConf, SparkContext
from pysparkling import *
from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import log2,log
from pyspark.sql import *
from pyspark.sql import functions as F

from math import sqrt
from functools import reduce

def parseData(data_filepath):    
    data=h2o.import_file(path=data_filepath)
    return data

def evaluate(h2oData, numOfResult, threshold):
    dfSpark = h2oContext.as_spark_frame(h2oData)
    numberOfColumn = len(dfSpark.columns)
    sumOfTable=dfSpark.count()
    
    crossTab = dfSpark.crosstab(dfSpark.columns[0], dfSpark.columns[numberOfColumn-1])
    crossTab = crossTab.drop(crossTab.columns[0])   
    crossTab_h2o = h2oContext.as_h2o_frame(crossTab)
    
    #calculates information gain of class-------------------------------------------
    df = pd.DataFrame({'col1': [1]})
    hf = h2o.H2OFrame(df)
    
    columnNum = len(crossTab_h2o.columns)
    for i in range(columnNum):
        total = crossTab_h2o[crossTab_h2o.columns[i]].sum()
        hf=hf.cbind(total)
    infogain=0
    for i in range(len(hf.columns)-1):
        infogain += (hf[hf.columns[i+1]] * (hf[hf.columns[i+1]]/sumOfTable).log2()).sum()/(-1 * sumOfTable) 
    #--------------------------------------------------------------------------------
        
    results = {}    
    #calculates information gain of every attribute ----------------------------------
    for j in range(numberOfColumn-1):
        
        length = len(crossTab.columns)
        newdf = crossTab.withColumn('summ', sum([crossTab[col] for col in range(length)]))
       
        newnewdf = newdf.select( sum([F.when( newdf[i]/newdf[length]==0 , 0 ).otherwise( (-1) * (newdf[i]/newdf[length])*log2(newdf[i]/newdf[length]) ) for i in range(length)]))

        newnewdf = newnewdf.withColumn("id", F.monotonically_increasing_id())
        newdf = newdf.withColumn("id", F.monotonically_increasing_id())
        newnewdf = newnewdf.join(newdf, "id")
        newnewdf = newnewdf.drop("id")

        attributeInfo = newnewdf.select( ([ newnewdf[0] * newnewdf["summ"] / float(sumOfTable) ]))

        total= attributeInfo.rdd.map(lambda tup: tup[0]).sum()
        result= infogain - total
        results[dfSpark.columns[j]] = result
        
        #filters values with threshold----------------------------------------------
        if(threshold!=-1 and result>=threshold):
            print("Information gain of " , dfSpark.columns[j] , " => ", result)
        elif(threshold==-1 and numOfResult==-1):
            print("Information gain of " , dfSpark.columns[j] , " => ", result)
        
        crossTab = dfSpark.crosstab(dfSpark.columns[j+1], dfSpark.columns[numberOfColumn-1])
        crossTab = crossTab.drop(crossTab.columns[0])
        
    #----------------------------------------------------------------------------------
    print("-----------------------------------")
    if(numOfResult!=-1):
            for i in range(numOfResult):
                maxx = max(results.keys(), key=(lambda k: results[k]))
                print("information gain of " , maxx , " = ", results[maxx])
                del results[maxx]
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Information Gain on Sparkling Water')
    parser.add_argument('--dataset', type=str, default="winequality-red2.csv")
    parser.add_argument('--numOfResult', type=int, default=-1)
    parser.add_argument('--threshold', type=float, default=-1)

    args = parser.parse_args()

    data_filepath = "hdfs://localhost:9000/user/cansu/" + args.dataset
    numOfResult = args.numOfResult
    threshold = args.threshold
    
    #Initiate System----------------------------------------------
    findspark.init()
    #conf = SparkConf().setAppName("Information Gain").setMaster("spark://192.168.34.225:7077").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    conf = SparkConf().setAppName("InformationGain").setMaster("local[*]").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    sc = SparkContext(conf=conf)   
    #Initiate Spark Session
    spark = SparkSession.builder.getOrCreate()
    #Initiate Spark Context
    h2oContext = H2OContext.getOrCreate(spark)
    h2oContext.show()
    #------------------------------------------------------------
    
    data=parseData(data_filepath)
    
    duration=[]
    for i in range(2):
        t1=time.time()
        evaluate(data, numOfResult, threshold)
        t2=time.time()
        duration.append(t2-t1)
    
    sizeOfDuration=len(duration)
    #calculates avarage duration
    mean = reduce(lambda x, y: x + y, duration) / sizeOfDuration
    
    #calculates standart devaiation of durations
    differences= [x-mean for x in duration]
    sq_differences = [d**2 for d in differences]
    std= sqrt(sum(sq_differences)/(sizeOfDuration-1))
    
    print("avarage time = ",mean,"\nStandart Deviation= ",std)
    
    h2oContext.stop()
    sc.stop()
    
    file = open("informationGain.txt","w") 
 
    file.write("mean = "+ str(mean)) 
    file.write("\nstandart deviation = " + str(std)) 
 
    file.close() 
