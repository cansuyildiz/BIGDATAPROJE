# -*- coding: utf-8 -*-
"""
Created on Tue May 08 10:52:24 2018

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
from functools import reduce

from math import sqrt
import time

def parseData(train_filepath,test_filepath,response):
    data=h2o.import_file(path=train_filepath)   
    test=h2o.import_file(path=test_filepath)
    
    #split data as validation and train
    r=data.runif(1234)
    train=data[r<0.8]
    validation=data[r>=0.8]
     
    #data.drop("BIREYSEL_KKB")
    data.drop("quality")
    predictors = data.columns
    #predictors = data.columns[1:-1]
    #response=data.columns[-1]
    #response = "BIREYSEL_KKB"
    #response = "quality"
    
    return train, validation, test, predictors
    
def evaluate(train,validation, predictors, response):
    
    linearEstimator1=H2OGeneralizedLinearEstimator(score_each_iteration=True, solver="l_bfgs", family="gaussian", early_stopping=False)
    linearEstimator1.train(x=predictors, y=response, training_frame=train, validation_frame=validation)
    
    linearEstimator2=H2OGeneralizedLinearEstimator(score_each_iteration=True, solver="l_bfgs",family="poisson", early_stopping=False)
    linearEstimator2.train(x=predictors, y=response, training_frame=train, validation_frame=validation)
    
    linearEstimator3=H2OGeneralizedLinearEstimator(score_each_iteration=True, solver="l_bfgs", family = "tweedie", early_stopping=False)
    linearEstimator3.train(x=predictors, y=response, training_frame=train, validation_frame=validation)

    prediction1 = linearEstimator1.predict(test)
    prediction2 = linearEstimator2.predict(test)
    prediction3 = linearEstimator3.predict(test)
    predictionResult = (prediction1+prediction2+prediction3)/3
       
    return linearEstimator1, linearEstimator2, linearEstimator3

def getPrediction(linearEstimator1, linearEstimator2, linearEstimator3, test):
    prediction1 = linearEstimator1.predict(test)
    prediction2 = linearEstimator2.predict(test)
    prediction3 = linearEstimator3.predict(test)
    
    print(prediction1.show())
    print(prediction2.show())
    print(prediction3.show())
    
    predictionResult = (prediction1+prediction2+prediction3)/3
    print(predictionResult)
    #print(test["BIREYSEL_KKB"])
    
    #MSE= ((test["rv2"] - predictionResult)*(test["rv2"] - predictionResult)).sum()/len(predictionResult)
    
    #return MSE

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Multiple Imputation on Sparkling Water')
    parser.add_argument('--dataset', type=str, default="winequality-red2.csv")
    parser.add_argument('--response', type=str)

    args = parser.parse_args() 

    train_filepath="hdfs://localhost:9000/user/cansu/" + args.dataset
    test_filepath="hdfs://localhost:9000/user/cansu/" + args.dataset
    response = args.response
    
    #Initiate System----------------------------------------------
    findspark.init()
    #conf = SparkConf().setAppName("MultipleImputation").setMaster("spark://192.168.34.225:7077").set("spark.ui.port", "7077").set("spark.executor.memory", "2g").set("spark.driver.memory", "2g") 
    conf = SparkConf().setAppName("MultipleImputation").setMaster("local[*]").set("spark.ui.port", "7077").set("spark.executor.memory", "1g").set("spark.driver.memory", "1g") 
    sc = SparkContext(conf=conf)
    #Initiate Spark Session
    spark = SparkSession.builder.getOrCreate()
    #Initiate Spark Context
    h2oContext = H2OContext.getOrCreate(spark)
    h2oContext.show()
    #------------------------------------------------------------
    
    train, validation, test, predictors = parseData(train_filepath,test_filepath,response)
    
    duration=[]
    for i in range(2):
        t1=time.time()
        evaluate(train, validation, predictors, response)
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
    
    #a,b,c = evaluate(train, validation, predictors, response)
    #getPrediction(a,b,c,test)
    
    h2oContext.stop()
    sc.stop()

    file = open("multipleImputation.txt","w") 
 
    file.write("mean = "+ str(mean)) 
    file.write("\nstandart deviation = " + str(std)) 
 
    file.close() 
