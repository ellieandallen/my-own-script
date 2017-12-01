# @author: ellie

# Created on 2017-11-30

import os
import sys
import logging
import pickle
from itertools import groupby


def _merge_vec(vector):
    # merge vectors of the same uid
    result = []
    for key, group in groupby(vector, lambda x: x[0]):
        temp = []
        for vec in group:
            temp.extend(vec[1])
        result.append([key, temp])

    # add count of words
    for i in range(0, len(result)):
        temp = []
        for a, b in groupby(sorted(result[i][1]), key=lambda item: item[0]):
            temp.append((a, sum([item[1] for item in list(b)])))
        result[i][1] = temp

    return result

# def _uid_update(j, i):
#     # sort a tuple list like [(212, 1), (1675, 1), (1696, 1), (497, 2)] to [(212, 1), (497, 2), (1675, 1), (1696, 1)]
#     # use b = sorted(a, key=lambda tup: tup[0])
#     length = len(j[1])
#     if j[0] == i[0]:
#         for k in range(0, length):
#             if k[]
#
#
# def _uid_exist(v, i):
#     for j in v:
#         if j[0] == i[0]:
#             return 1
#         else:
#             pass
#     return 0


def merge_vec(merged_vector, fullname):
    # read vector
    with open(fullname, 'rb') as data_file:
        sms_vector = pickle.load(data_file)

    merged_vector.extend(sms_vector)
    merged_vector = _merge_vec(merged_vector)

    return merged_vector

if __name__ == '__main__':
    program = os.path.basename(sys.argv[0])  # get file name
    logger = logging.getLogger(program)
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)

    # input dir
    inp = '/data'
    foldername = 'sms_process2'

    logger.info("running " + foldername + " files.")
    rootdir = inp + '/' + foldername

    merged = []  # init the merged vector

    # 3 args:1.parent dir 2.all dir names(without path) 3.all file names
    for parent, dirnames, filenames in os.walk(rootdir):
        for filename in filenames:
            sms_file = rootdir + '/' + filename
            logger.info("Dealing with file: " + sms_file)

            merged = merge_vec(merged, sms_file)

    with open('/data/sms_commonfiles/uid_vector.vec', 'wb') as vector_file:
        pickle.dump(merged, vector_file, pickle.HIGHEST_PROTOCOL)

    print 'game over'
