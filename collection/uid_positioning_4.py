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


eps = 0.0056
min_points = 8


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


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("get gps").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.kryoserializer.buffer.max", "2048m")
    sc = spark.sparkContext

    sqlDF = spark.sql("select * from yx_location")

    locationrdd = sqlDF.rdd.map(lambda x: (x[0], list(x[1:]))).groupByKey().mapValues(list)
    locationmap = locationrdd.collectAsMap()
    for key in locationmap:
        m = np.transpose(np.array(locationmap[key])).astype(np.float)
        locationmap[key] = uid_positioning(m, eps, min_points)

    location = sc.parallelize(locationmap.items()).toDF(['key', 'val_1'])
    location.show()
    temp = location.toPandas()
    temp.to_csv('/home/ubuntu/location.csv', encoding='utf8')

