# -*- coding: utf-8 -*-
"""
Created on Thu May 10 16:44:41 2018

@author: cansu.yildiz
"""

from __future__ import print_function

import argparse
import h2o
import numpy as np
import findspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame, DataFrameStatFunctions
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pysparkling import *
import pyspark.sql.functions as sqlfunc
from functools import reduce

from math import sqrt
import time

def parse_data(spark,filePath,columnIndex1,columnIndex2):
    
    dataframe = spark.read.csv(filePath,header=False)
    columnName1=dataframe.columns[columnIndex1]
    columnName2=dataframe.columns[columnIndex2] 
    
    dataframe_with_id = dataframe.withColumn(columnName1, dataframe[columnName1].cast("double"))\
                 .withColumn(columnName2, dataframe[columnName2].cast("double"))\
                 .withColumn("id", sqlfunc.monotonically_increasing_id())
    columns_with_id = dataframe_with_id.select(columnName1, columnName2, "id")
    columns_with_id = columns_with_id.withColumnRenamed(columnName1, "column1").withColumnRenamed(columnName2, "column2")
    
    return dataframe_with_id, columns_with_id

def mahalanobis_distance(columns):
    cov_11 = columns.cov('column1', 'column1')
    cov_12 = columns.cov('column1', 'column2')
    cov_22 = columns.cov('column2', 'column2')
    covariance = np.array([[cov_11, cov_12],
                        [cov_12, cov_22]])
    
    inv = np.linalg.inv(covariance)
    a = inv[0][0]
    b = inv[0][1]
    c = inv[1][1]
    
    xy_mean = float(columns.select([sqlfunc.mean('column1')]).collect()[0][0]), \
              float(columns.select([sqlfunc.mean('column2')]).collect()[0][0])
    diff_xy = columns.select((columns['column1']-xy_mean[0]).alias('x-x_mean'),
                       (columns['column2']-xy_mean[1]).alias('y-y_mean'), columns['id'])
    distance = diff_xy.select(((sqlfunc.pow(diff_xy['x-x_mean'], 2)*a)
                                + (2*diff_xy['x-x_mean']*diff_xy['y-y_mean']*b)
                                + (sqlfunc.pow(diff_xy['y-y_mean'], 2)*c)).alias("dist"), diff_xy['id'])
    return distance

def remove_outliers(dataframe_with_id, distance, threshold_coeff):

    threshold = float(distance.select([sqlfunc.mean('dist')]).collect()[0][0])
    threshold *= threshold_coeff
    outliers_dist = distance.filter(distance.dist > threshold)
    cleaned_dataframe = dataframe_with_id.select('id').subtract(outliers_dist.select('id')).toDF('new').join(dataframe_with_id, sqlfunc.col('new') == dataframe_with_id.id).drop('new')
    outliers = dataframe_with_id.select('id').subtract(cleaned_dataframe.select('id')).toDF('new').join(dataframe_with_id, sqlfunc.col('new') == dataframe_with_id.id).drop('new')
    cleaned_dataframe = cleaned_dataframe.drop('id')
    outliers = outliers.drop('id')
    return cleaned_dataframe, outliers

def duration_of_calculating_distance(columns_with_id):
    duration=[]
    for i in range(100):
        t1=time.time()
        mahalanobis_distance(columns_with_id)
        t2=time.time()
        duration.append(t2-t1)

    sizeOfDuration=len(duration)
    #calculates avarage duration
    mean = reduce(lambda x, y: x + y, duration) / sizeOfDuration
    
    #calculates standart devaiation of durations
    differences= [x-mean for x in duration]
    sq_differences = [d**2 for d in differences]
    std= sqrt(sum(sq_differences)/(sizeOfDuration-1))
    return mean,std

def duration_of_removing_outliers(dataframe_with_id, distance, threshold):
    duration=[]
    for i in range(2):
        t1=time.time()
        remove_outliers(dataframe_with_id, distance, threshold) 
        t2=time.time()
        duration.append(t2-t1)

    sizeOfDuration=len(duration)
    #calculates avarage duration
    mean = reduce(lambda x, y: x + y, duration) / sizeOfDuration
    
    #calculates standart devaiation of durations
    differences= [x-mean for x in duration]
    sq_differences = [d**2 for d in differences]
    std= sqrt(sum(sq_differences)/(sizeOfDuration-1))
    return mean,std

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Mahalanobis on Sparkling Water')
    parser.add_argument('--dataset', type=str, default="winequality-red2.csv")
    parser.add_argument('--columnIndex1', type=int, default=0)
    parser.add_argument('--columnIndex2', type=int, default=3)
    parser.add_argument('--threshold', type=float, default=1.5)

    args = parser.parse_args()

    filePath="hdfs://localhost:9000/user/cansu/" + args.dataset   #airfoilSelfNoiseshort.csv"
    columnIndex1 = args.columnIndex1
    columnIndex2 = args.columnIndex2
    threshold = args.threshold
    
    #Initiate System----------------------------------------------
    findspark.init()
    #conf = SparkConf().setAppName("MahalanobisDistance").setMaster("spark://192.168.34.225:7077").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    conf = SparkConf().setAppName("MahalanobisDistance").setMaster("local[*]").set("spark.ui.port", "7077").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g") 
    sc = SparkContext(conf=conf)
    #Initiate Spark Session
    spark = SparkSession.builder.getOrCreate()
    #Initiate Spark Context
    h2oContext = H2OContext.getOrCreate(spark)
    h2oContext.show()
    #------------------------------------------------------------

    dataframe_with_id, columns_with_id = parse_data(spark,filePath,columnIndex1,columnIndex2)
    
    distance = mahalanobis_distance(columns_with_id)
    
    cleaned_dataframe, outliers = remove_outliers(dataframe_with_id, distance, threshold) 
    
    mean1, std1 = duration_of_calculating_distance(columns_with_id)
    mean2, std2 = duration_of_removing_outliers(dataframe_with_id, distance, threshold)
    
    print("Distance= ", distance)
    print("Cleaned Dataframe = ", cleaned_dataframe)
    print("Outliers = ", outliers)
    print("Mean duration of calculating distance = ", mean1, " with std= ", std1)
    print("Mean duration of removing outliers = ", mean2, " with std= ", std2)
    
    h2oContext.stop()
    sc.stop()

    file = open("mahalanobis.txt","w") 
 
    file.write("Mean duration of calculating distance = "+ str(mean1)) 
    file.write("\nMean duration of removing outliers = "+ str(mean2)) 
    file.write("\nstandart deviation of calculating distance =" + str(std1)) 
    file.write("\nstandart deviation of removing outliers =" + str(std2)) 
 
    file.close() 


    
    
    
    
    
    
