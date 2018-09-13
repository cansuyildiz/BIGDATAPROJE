# -*- coding: utf-8 -*-
"""
Created on Wed May 09 15:36:16 2018

@author: cansu.yildiz
"""

from __future__ import print_function

import argparse
import h2o
import findspark
from pyspark import SparkConf, SparkContext 
from pysparkling import *
from pyspark.sql import SparkSession
from math import sqrt
import time

from h2o.estimators.random_forest import H2ORandomForestEstimator
from h2o.estimators.gbm import H2OGradientBoostingEstimator
from h2o.estimators.stackedensemble import H2OStackedEnsembleEstimator

from functools import reduce

def parseData(data_filepath,response):
    
    print("import and parse data: ")
    data=h2o.import_file(path=data_filepath)
    
    predictors=data.columns
    #response = "KRED? KARTI"
    
    data[response]=data[response].asfactor()   #generating label as an enum(categorical column)
    
    r = data.runif(1234)
    train = data[r < 0.8]
    test = data[r >= 0.8]
    
    return train, test, predictors

def evaluate(train,predictors,response,ntrees,max_depth,min_rows,learn_rate,nfolds,seed):
    nfolds=5
    #generating a model
    # Train and cross-validate a GBM
    gradientBoostingEstimator = H2OGradientBoostingEstimator(distribution="auto",
                                              ntrees=ntrees,
                                              max_depth=max_depth,
                                              min_rows=min_rows,
                                              learn_rate=learn_rate,
                                              nfolds=nfolds,
                                              fold_assignment="Modulo",
                                              keep_cross_validation_predictions=True,
                                              seed=seed) 
    gradientBoostingEstimator.train(x=predictors, y=response, training_frame=train)
    
    nfolds=5
    randomForestEstimator = H2ORandomForestEstimator(ntrees=ntrees,  max_depth=max_depth, 
                                         nfolds=nfolds,
                                         fold_assignment="Modulo",
                                         keep_cross_validation_predictions=True,
                                         seed=seed)
    randomForestEstimator.train(x=predictors, y=response, training_frame=train)

    stackingEstimator = H2OStackedEnsembleEstimator(metalearner_nfolds=5, metalearner_algorithm="AUTO", base_models=[gradientBoostingEstimator, randomForestEstimator])
    stackingEstimator.train(x=predictors, y=response, training_frame=train)
    
    return gradientBoostingEstimator,randomForestEstimator,stackingEstimator

    
def prediction(estimator,test):
    perf=estimator.model_performance(test)
    MSE = perf.mse()
    confusionMatrix = perf.confusion_matrix()
    return MSE,confusionMatrix

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Stacking on Sparkling Water')
    parser.add_argument('--dataset', type=str, default="winequality-red2.csv")
    parser.add_argument('--ntrees', type=int, default=50)
    parser.add_argument('--maxDepth', type=int, default=3)
    parser.add_argument('--minRows', type=int, default=2)
    parser.add_argument('--learnRate', type=float, default=0.2)
    parser.add_argument('--nFolds', type=int, default=5)
    parser.add_argument('--seed', type=int, default=1)
    parser.add_argument('--response', type=str)

    args = parser.parse_args()    

    data_filepath = "hdfs://localhost:9000/user/cansu/" + args.dataset
    ntrees = args.ntrees
    max_depth = args.maxDepth
    min_rows = args.minRows
    learn_rate = args.learnRate
    nfolds = args.nFolds
    seed = args.seed
    response = args.response
    
    #Initiate System----------------------------------------------
    findspark.init()
    #conf = SparkConf().setAppName("Stacking").setMaster("spark://192.168.34.225:7077").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    conf = SparkConf().setAppName("Stacking").setMaster("local[*]").set("spark.ui.port", "7077").set("spark.executor.memory", "10g").set("spark.driver.memory", "10g")
    sc = SparkContext(conf=conf)
    #Initiate Spark Session
    spark = SparkSession.builder.getOrCreate()
    #Initiate Spark Context
    h2oContext = H2OContext.getOrCreate(spark)
    h2oContext.show()
    #------------------------------------------------------------
    
    train, test, predictors = parseData(data_filepath,response)
    
    duration=[]
    for i in range(2):
        t1=time.time()
        gradientBoostingEstimator,randomForestEstimator,stackingEstimator = evaluate(train,predictors,response,ntrees,max_depth,min_rows,learn_rate,nfolds,seed)
        t2=time.time()
        duration.append(t2-t1)
    
    
    sizeOfDuration=len(duration)
    #calculates avarage duration
    mean = reduce(lambda x, y: x + y, duration) / sizeOfDuration
    
    #calculates standart devaiation of durations
    differences= [x-mean for x in duration]
    sq_differences = [d**2 for d in differences]
    std= sqrt(sum(sq_differences)/(sizeOfDuration-1))
    
    print("mean= ",mean,"\nStandart Deviation= ",std)
    
    MSE, confusionMatrix = prediction(gradientBoostingEstimator,test)
    print("gradientBoostingEstimator Mse= ",MSE, " confusion Matrix= ",confusionMatrix)
    MSE, confusionMatrix = prediction(randomForestEstimator,test)
    print("randomForestEstimator Mse= ",MSE, " confusion Matrix= ",confusionMatrix)
    MSE, confusionMatrix = prediction(stackingEstimator,test)
    print("stackingEstimator Mse= ",MSE, " confusion Matrix= ",confusionMatrix)
    
    h2oContext.stop()
    sc.stop()
    
    file = open("stacking.txt","w") 
 
    file.write("mean = "+ str(mean)) 
    file.write("\nstandart deviation = " + str(std))
    file.write("\nMSE = " + str(MSE) + "\nconfusion Matrix= \n" + str(confusionMatrix))
 
    file.close() 
    
    
    
    
    
    
    
    
    
    
    
    
    
