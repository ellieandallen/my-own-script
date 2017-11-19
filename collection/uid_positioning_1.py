'''
Created on 2017/05/22

@author: ellie
'''


from os.path import expanduser, join
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import math

# warehouse_location = 'spark-warehouse'
# spark = SparkSession.builder.appName("Python Spark SQL Hive integration example").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
# locationDF = spark.sql("select uid, latitude, longitude from userlocation where uid in (select uid from uid_yx)")
#
# def f(x):
#     x = sorted(x, key=lambda x: x[2])
#
# location = locationDF.rdd.map(lambda x: (x[0], list(x[1:]))).groupByKey().mapValues(f)
#
UNCLASSIFIED = False
NOISE = None


def _dist(p, q):
    return math.sqrt(np.power(p-q, 2).sum())


def _eps_neighborhood(p, q, eps):
    return _dist(p, q) < eps


def _region_query(m, point_id, eps):
    n_points = m.shape[1]
    seeds = []
    for i in range(0, n_points):
        if _eps_neighborhood(m[:, point_id], m[:, 1], eps):
            seeds.append(i)
    return seeds


def _expand_cluster(m, classifications, point_id, cluster_id, eps, min_points):
    seeds = _region_query(m, point_id, eps)
    if len(seeds) < min_points:
        classifications[point_id] = NOISE
        return False
    else:
        classifications[point_id] = cluster_id
        for seed_id in seeds:
            classifications[seed_id] = cluster_id

        while len(seeds) > 0:
            current_point = seeds[0]
            results = _region_query(m, current_point, eps)
            if len(results) >= min_points:
                for i in range(0, len(results)):
                    result_point = results[i]
                    if classifications[result_point] == UNCLASSIFIED or classifications[result_point] == NOISE:
                        if classifications[result_point] == UNCLASSIFIED:
                            seeds.append(result_point)
                        classifications[result_point] = cluster_id
            seeds = seeds[1:]
        return True


def dbscan(m, eps, min_points):
    cluster_id = 1
    n_points = m.shape[1]
    classificaions = [UNCLASSIFIED] * n_points
    for point_id in range(0, n_points):
        point = m[:, point_id]
        if classificaions[point_id] == UNCLASSIFIED:
            if _expand_cluster(m, classificaions, point_id, cluster_id, eps, min_points):
                cluster_id = cluster_id + 1

        return classificaions







df = pd.read_csv("/home/ellie/test.csv")
X = df.values

