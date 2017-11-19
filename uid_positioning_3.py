'''
Created on 2017/05/26

@author: ellie
'''

import numpy as np
import pandas as pd
import math
import operator
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark import SparkContext


warehouse_location = 'spark-warehouse'
spark = SparkSession.builder.appName("Python Spark SQL Hive integration example").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
# sqlDF = spark.sql("show tables")
sqlDF = spark.sql("select * from yx_location")
eps = 0.0056
min_points = 10


def _dist(p, q):
    return math.sqrt(np.power(p-q, 2).sum())


def _eps_neighborhood(p, q, eps):
    return _dist(p, q) < eps


def _region_query(m, point_id, eps):
    n_points = m.shape[1]
    seeds = []
    for i in range(0, n_points):
        if _eps_neighborhood(m[:, point_id], m[:, i], eps):
            seeds.append(i)
    return seeds


def _get_density(m, point_id, eps, min_points):
    seeds = _region_query(m, point_id, eps)
    if len(seeds) < min_points:
        return 0
    else:
        return len(seeds)


def uid_positioning(m, eps, min_points):
    m = sqlContext.createDataFrame(m, ['latitude', 'longitude'])
    m = m.toPandas().values
    m = np.transpose(m)
    m = m.astype(np.float)
    n_points = m.shape[1]
    density = []
    for point_id in range(0, n_points):
        temp_density = _get_density(m, point_id, eps, min_points)
        density.append(temp_density)
    density = dict(enumerate(density, 0))
    density = sorted(density.items(), key=operator.itemgetter(1), reverse=True)
    first = m[:, density[0][0]]
    first = first.tolist()
    return first


location = sqlDF.rdd.map(lambda x: (x[0], list(x[1:]))).groupByKey().mapValues(uid_positioning)
sqlContext = SQLContext(sc)
location = SparkSession.createDataFrame(location, ['uid', 'latitude', 'longitude'])
hc = HiveContext(SparkContext)
location.write.format("orc").saveAsTable("yx_location_process")
