'''
Created on 2017/05/22

@author: ellie
'''


from os.path import expanduser, join
# from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import math


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
        if _eps_neighborhood(m[:, point_id], m[:, i], eps):
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
    print classificaions
    cluster_index = 0
    clusters = []
    cluster = []
    m.tolist()
    for point_id in range(0, n_points):
        point = m[:, point_id].tolist()
        print point
        if classificaions[point_id] == cluster_index:
            cluster.append(point)
        else:
            if len(cluster) != 0:
                clusters.append(cluster)
                cluster = []
            if classificaions[point_id] != NOISE:
                cluster_index = classificaions[point_id]
                cluster.append(point)
    return clusters



m = np.matrix('1 1.2 0.8 100 3.7 3.9 3.6 10; 1.1 0.8 1 100 4 3.9 4.1 10')
eps = 0.5
min_points = 2
print dbscan(m, eps, min_points)

