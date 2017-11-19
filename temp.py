'''
Created on 2017/05/16

@author: ellie
'''

import pandas as pd
import numpy as np

# x = [[u'-6.2240235', u'106.5398028', u'1493083249663'], [u'-6.2240235', u'106.5398028', u'1493083262527']]
# x = sorted(x, key=lambda x : x[2])
# a = 0
# b = 0
# for j in range(1, len(x)):
#     time_interval = int(x[j][2]) - int(x[j - 1][2])
#     distance = ((float(x[j][1]) - float(x[j - 1][1])) ** 2 + (float(x[j][0]) - float(x[j - 1][0])) ** 2) ** 0.5
#     if time_interval < 5000 and distance > 0.0001:
#         a = a + 1
#         b = b + 1
#         break
#     elif time_interval < 5000:
#         b = b + 1
#         break
#     else:
#         pass

# print 'allen love ellie'
# a = ['5','7','1','2']
# b = dict(enumerate(a, 0))
# print b
#
# import operator
# sorted_b = sorted(b.items(), key=operator.itemgetter(1), reverse=True)
# print sorted_b
# print type(sorted_b)
# print sorted_b[0]
# print sorted_b[0][0]
#
# p = np.array([-6.301713, 106.841899])
# q = np.array([-6.297127, 106.838803])
# print _dist(p, q)

# df = pd.read_csv('/home/ellie/unsolved.csv', encoding='utf8')
# df['province'] = ''
# df['city'] = ''
# df['district'] = ''
# df['street'] = ''
#
# print type(df['address'][0])
# df['address'] = df['address'].astype(str).str.replace('[', '').str.replace(']', '').str.split(',')
# print df[:5]
#
# print len(df['address'][0])
# print df['address'][0]
#
# for i in range(0, len(df['address'])):
#     if len(df['address'][i]) == 7:
#         df['province'][i] = df['address'][i][5]
#         df['city'][i] = df['address'][i][4]
#         df['district'][i] = df['address'][i][3]
#         df['street'][i] = df['address'][i][2] + df['address'][i][1] + df['address'][i][0]
#     else:
#         pass
#
# df.to_csv('/home/ellie/unsolved_a.csv', encoding='utf8')





# import math
# import operator
#
#
# EPS = 0.0056
# MIN_POINTS = 2
#
#
# def _dist(p, q):
#     return math.sqrt(np.power(p-q, 2).sum())
#
#
# def _eps_neighborhood(p, q, eps):
#     return _dist(p, q) < eps
#
#
# def _region_query(m, point_id, eps):
#     n_points = m.shape[1]
#     seeds = []
#     for i in range(0, n_points):
#         if _eps_neighborhood(m[:, point_id], m[:, i], eps):
#             seeds.append(i)
#     return seeds
#
#
# def _get_density(m, point_id, eps, min_points):
#     seeds = _region_query(m, point_id, eps)
#     if len(seeds) < min_points:
#         return 0
#     else:
#         return len(seeds)
#
#
# def uid_positioning(m, eps=EPS, min_points=MIN_POINTS):
#     m = np.transpose(np.array(m)).astype(np.float)
#     n_points = m.shape[1]
#     density = []
#     for point_id in range(0, n_points):
#         temp_density = _get_density(m, point_id, eps, min_points)
#         density.append(temp_density)
#     density = dict(enumerate(density, 0))
#     density = sorted(density.items(), key=operator.itemgetter(1), reverse=True)
#     first = m[:, density[0][0]]
#     first = first.tolist()
#     return first
#
# if __name__ == '__main__':
#     M = [[1, 2], [5, 4], [7, 2], [1, 2]]
#     print type(M)
#     result = uid_positioning(m=M)
#     print result





#
# class Fib:
#     def __init__(self, n):
#         self.prev = 0
#         self.cur = 1
#         self.n = n
#
#     def __iter__(self):
#         return self
#
#     def __next__(self):
#         if self.n > 0:
#             value = self.cur
#             self.cur = self.cur + self.prev
#             self.prev = value
#             self.n -= 1
#             return value
#         else:
#             raise StopIteration()
#
#     def next(self):
#         return self.__next__()
#
# f = Fib(20)
# print hasattr(f, '__iter__')
# print hasattr(f, '__getitem__')
#
# print([i for i in f])


# df = pd.read_csv('/home/ellie/company_mapping_new', encoding='utf8')
# df = df.values
# for i in range(1, len(df)):
#     if df[i][0]>df[i-1][0] and df[i][1]>=df[i-1][1]:
#         pass
#     else:
#         print i
#         break










