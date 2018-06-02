# -*- coding: utf-8 -*-
"""
Created on Tue May 08 13:51:56 2018

@author: cansu.yildiz
"""

from __future__ import print_function

import h2o
import findspark
import time
from h2o.estimators.kmeans import H2OKMeansEstimator
from pyspark import SparkConf, SparkContext 
from pysparkling import *
from pyspark.sql import SparkSession
from math import sqrt
from functools import reduce

def parseData(data_filepath):
    data=h2o.import_file(path=data_filepath)  
    data.describe()
    
    r=data.runif(1234)
    train=data[r<0.8]
    test=data[r>=0.8]
    
    predictors= data.columns
    print(predictors)
    
    return train,test,predictors


def evaluate(train,predictors):  
    kmeansEstimator = H2OKMeansEstimator(k=3, init="Random", seed=2, standardize=True)
    kmeansEstimator.train(x=predictors, training_frame = train)
    return kmeansEstimator

def getPrediction(estimator,test):
    prediction = estimator.predict(test)
    print(prediction)
    print("MODEL PERFORMANCE:\n")
    performance = estimator.model_performance(test)
    print(performance)

if __name__ == "__main__":
    
    data_filepath="hdfs://localhost:9000/user/cansu/winequality-red2.csv"   #reaction_network.txt"
    
    #Initiate System----------------------------------------------
    findspark.init()
    #conf = SparkConf().setAppName("K-Means").setMaster("spark://192.168.34.225:7077").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    conf = SparkConf().setAppName("Multiple Imputation").setMaster("local[*]").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g")
    sc = SparkContext(conf=conf)
    #Initiate Spark Session
    spark = SparkSession.builder.getOrCreate()
    #Initiate Spark Context
    h2oContext = H2OContext.getOrCreate(spark)
    h2oContext.show()
    #------------------------------------------------------------
    
    train,test,predictors = parseData(data_filepath)
    
    duration=[]
    for i in range(2):
        t1=time.time()
        estimator = evaluate(train,predictors)
        t2=time.time()
        duration.append(t2-t1)
    
    sizeOfDuration=len(duration)
    #calculates avarage duration
    mean = reduce(lambda x, y: x + y, duration) / sizeOfDuration
    
    #calculates standart devaiation of durations
    differences= [x-mean for x in duration]
    sq_differences = [d**2 for d in differences]
    std= sqrt(sum(sq_differences)/(sizeOfDuration-1))
    
    print("mean duration of K-Means algorithm= ",mean,"\nStandart Deviation= ",std)
    
    getPrediction(estimator,test)
    
    h2oContext.stop()
    sc.stop()

    file = open("kmeans.txt","w") 
 
    file.write("mean = "+ str(mean)) 
    file.write("\nstandart deviation = " + str(std)) 
 
    file.close() 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
