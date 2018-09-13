# -*- coding: utf-8 -*-
"""
Created on Fri May 04 10:40:37 2018

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


def parseData(train_filepath, response):

    data=h2o.import_file(path=train_filepath)   
    #test=h2o.import_file(path=test_filepath)
    
    data.describe()
    #data.drop("musteri id")
    
    r=data.runif(1234)
    train=data[r<0.8]
    test=data[r>=0.8]
    
    #predictors= data.columns
    #predictors=['Appliances', 'lights', 'T1', 'RH_1', 'T2', 'RH_2', 'T3', 'RH_3', 'T4', 'RH_4', 'T5', 'RH_5', 'T6', 'RH_6', 'T7', 'RH_7', 'T8', 'RH_8', 'T9', 'RH_9', 'T_out', 'Press_mm_hg', 'RH_out', 'Windspeed', 'Visibility', 'Tdewpoint', 'rv1']
    predictors=data.columns
    print("predictors= ", predictors)
    
    
    #response='rv2'
    #response = "quality"
    #response = 'ADRES1_SEHIR_KODU'
    #test=test.drop("date")
    test.drop(response)
    #print(test.columns)
    
    return train,test,predictors

def evaluate(train,test,predictors,response):

    linearEstimator=H2OGeneralizedLinearEstimator(score_each_iteration=True, solver="l_bfgs", early_stopping=False)
    linearEstimator.train(x=predictors, y=response, training_frame=train)#, validation_frame=validation)

    return linearEstimator
    
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Linear Regression on Sparkling Water')
    parser.add_argument('--dataset', type=str, default="winequality-red2.csv")
    parser.add_argument('--response', type=str)

    args = parser.parse_args()    

    train_filepath="hdfs://localhost:9000/user/cansu/" + args.dataset     #energydata_complete.csv"
    response = args.response
    #test_filepath="hdfs://192.168.34.252:9000/ozge/energy_test.csv"
    
    #Initiate System----------------------------------------------
    findspark.init()
    #conf = SparkConf().setAppName("LinearRegression").setMaster("spark://192.168.34.225:7077").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    conf = SparkConf().setAppName("LinearRegression").setMaster("local[*]").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g")
    sc = SparkContext(conf=conf)
    #Initiate Spark Session
    spark = SparkSession.builder.getOrCreate()
    #Initiate Spark Context
    h2oContext = H2OContext.getOrCreate(spark)
    h2oContext.show()
    #------------------------------------------------------------
    
    train,test,predictors = parseData(train_filepath,response)
    
    duration=[]
    for i in range(2):
        t1=time.time()
        evaluate(train,test,predictors,response)
        t2=time.time()
        duration.append(t2-t1)
    
    
    sizeOfDuration=len(duration)
    #calculates avarage duration
    mean = reduce(lambda x, y: x + y, duration) / sizeOfDuration
    
    #calculates standart devaiation of durations
    differences= [x-mean for x in duration]
    sq_differences = [d**2 for d in differences]
    std= sqrt(sum(sq_differences)/(sizeOfDuration-1))
    
    linearEstimator = evaluate(train,test,predictors,response)
    prediction=linearEstimator.predict(test)
    print("prediction for response column= ", prediction)
    print(linearEstimator.auc)
    
    
    print("mean duration of linear regression= ",mean,"\nStandart Deviation= ",std)
    
    
    h2oContext.stop()
    sc.stop()

    file = open("linearRegression.txt","w") 
 
    file.write("mean = "+ str(mean)) 
    file.write("\nstandart deviation = " + str(std)) 
 
    file.close() 

	
