# -*- coding: utf-8 -*-
"""
Created on Tue May 08 11:49:34 2018

@author: cansu.yildiz
"""

from __future__ import print_function
import argparse
import h2o
from h2o.estimators.pca import H2OPrincipalComponentAnalysisEstimator
from pyspark import SparkConf, SparkContext 
from pysparkling import *
from pyspark.sql import SparkSession
import findspark
import time
from math import sqrt
from functools import reduce

def parseData(train_filepath,test_filepath):
    
    train = h2o.import_file(path=train_filepath)   
    test = h2o.import_file(path=test_filepath)
    
    train.describe()
         
    #train=train.drop(train.columns[0])
    #train=train.drop(train.columns[1])
    
    #test=test.drop(test.columns[0])
    #test=test.drop(test.columns[1])
       
    return train,test
    
def evaluate(train,k):
    #print(train.show())
    pcaEstimator = H2OPrincipalComponentAnalysisEstimator(k=k)
    pcaEstimator.train(x=train.columns, training_frame = train)
    #pcaModel = pcaEstimator.fit(train)
    return pcaEstimator

def getPrediction(estimator,train):
    prediction = estimator.predict(train)
    print(prediction)
    return prediction
    
    
    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='PCA on Sparkling Water')
    parser.add_argument('--dataset', type=str, default="winequality-red2.csv")
    parser.add_argument('--k', type=int, default=3)

    args = parser.parse_args()

    train_filepath="hdfs://localhost:9000/user/cansu/" + args.dataset   #"hdfs://192.168.34.252:9000/ozge/PCA_leaf_train.csv"  #"hdfs://192.168.34.252:9000/ozge/MLlib_logit.txt"  # reaction_network.txt
    test_filepath = "hdfs://localhost:9000/user/cansu/" + args.dataset
    k = args.k
    
    #Initiate System----------------------------------------------
    findspark.init()
    #conf = SparkConf().setAppName("PCA").setMaster("spark://192.168.34.225:7077").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    conf = SparkConf().setAppName("PCA").setMaster("local[*]").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g")     
    sc = SparkContext(conf=conf)
    #Initiate Spark Session
    spark = SparkSession.builder.getOrCreate()
    #Initiate Spark Context
    h2oContext = H2OContext.getOrCreate(spark)
    h2oContext.show()
    #------------------------------------------------------------
    
    train,test = parseData(train_filepath, test_filepath)

    duration=[]
    for i in range(2):
        t1=time.time()
        evaluate(train,k)
        t2=time.time()
        duration.append(t2-t1)
    
    
    sizeOfDuration=len(duration)
    #calculates avarage duration
    mean = reduce(lambda x, y: x + y, duration) / sizeOfDuration
    
    #calculates standart devaiation of durations
    differences= [x-mean for x in duration]
    sq_differences = [d**2 for d in differences]
    std= sqrt(sum(sq_differences)/(sizeOfDuration-1))
    
    print("mean of duration = ",mean,"\nStandart Deviation of duration= ",std)
    
    estimator = evaluate(train,k)
    prediction = getPrediction(estimator,train)
    
    h2oContext.stop()
    sc.stop()
    
    file = open("pca.txt","w") 
 
    file.write("mean = "+ str(mean)) 
    file.write("\nstandart deviation = " + str(std)) 
 
    file.close() 
    
    
    
    
    

    
