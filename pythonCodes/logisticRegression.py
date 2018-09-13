# -*- coding: utf-8 -*-
"""
Created on Tue May 08 09:54:00 2018

@author: cansu.yildiz
"""
from __future__ import print_function

import argparse
import h2o
import findspark
from h2o.estimators.glm import H2OGeneralizedLinearEstimator
from pyspark import SparkConf, SparkContext 
from pysparkling import *
from pyspark.sql import SparkSession
from math import sqrt
import time

from functools import reduce

def parseData(train_filepath,test_filepath,testLabels_filepath,response):
    
    data=h2o.import_file(path=train_filepath)   
    test=h2o.import_file(path=test_filepath)
    testLabels=h2o.import_file(path=testLabels_filepath)
    
    #data.drop("musteri id")
    
    r=data.runif(1234)
    train=data[r<0.8]
    validation=data[r>=0.8]
    
    predictors=data.columns
    print("predictors= ", predictors)
    
    #response='MEVDUAT'
    #response = "Survived"
  
    data[response]=data[response].asfactor()

    #test=test.drop(response)
    #print(test.columns)
    
    return train,validation,test,testLabels,predictors,response


def evaluate(train,validation,test,testLabels,predictors,response):

    logisticEstimator = H2OGeneralizedLinearEstimator(family="binomial",missing_values_handling="MeanImputation")
    logisticEstimator.train(x=predictors,y=response,training_frame=train,validation_frame=validation)

   # prediction=logisticEstimator.predict(test)
   # print("prediction for rv2 column= ", prediction)

    #print(logisticEstimator.confusion_matrix(train=True, valid=True))
    
    #print("accuracy= ", logisticEstimator.accuracy())
    

    

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Logistic Regression on Sparkling Water')
    parser.add_argument('--dataset', type=str, default="titanic_train.csv")
    parser.add_argument('--response', type=str)

    args = parser.parse_args()

    train_filepath="hdfs://localhost:9000/user/cansu/" + args.dataset     #titanic_train.csv 
    test_filepath="hdfs://localhost:9000/user/cansu/" + args.dataset 
    testLabels_filepath = "hdfs://localhost:9000/user/cansu/" + args.dataset 
    response = args.response
    
    #Initiate System----------------------------------------------
    findspark.init()
    #conf = SparkConf().setAppName("LogisticRegression").setMaster("spark://192.168.34.225:7077").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    conf = SparkConf().setAppName("LogisticRegression").setMaster("local[*]").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g")
    sc = SparkContext(conf=conf)
    #Initiate Spark Session
    spark = SparkSession.builder.getOrCreate()
    #Initiate Spark Context
    h2oContext = H2OContext.getOrCreate(spark)
    h2oContext.show()
    #------------------------------------------------------------
    
    train,validation,test,testLabels,predictors,response = parseData(train_filepath,test_filepath, testLabels_filepath,response)
    
    duration=[]
    for i in range(2):
        t1=time.time()
        evaluate(train,validation,test,testLabels,predictors,response)
        t2=time.time()
        duration.append(t2-t1)
    
    
    sizeOfDuration=len(duration)
    #calculates avarage duration
    mean = reduce(lambda x, y: x + y, duration) / sizeOfDuration
    
    #calculates standart devaiation of durations
    differences= [x-mean for x in duration]
    sq_differences = [d**2 for d in differences]
    std= sqrt(sum(sq_differences)/(sizeOfDuration-1))
    
    print("mean of logistic regression= ",mean,"\nStandart Deviation= ",std)
    
    h2oContext.stop()
    sc.stop()

    file = open("logisticRegression.txt","w") 
 
    file.write("mean = "+ str(mean)) 
    file.write("\nstandart deviation = " + str(std)) 
 
    file.close() 
    
