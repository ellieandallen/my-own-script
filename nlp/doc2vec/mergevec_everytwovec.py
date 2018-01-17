# @author: lionheart

# Created on 2017-12-22

import os
import sys
import logging
import pickle
from itertools import groupby
from itertools import izip
from itertools import izip_longest
import multiprocessing as mp
from operator import itemgetter


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks"""
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def _merge_vec(vector):
    vector = sorted(vector, key=itemgetter(0))
    # vector = sorted(vector, key=lambda x: x[0])
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


def _read_vec(file):
    with open(file, 'rb') as data_file:
        sms_vector = pickle.load(data_file)
    return sms_vector


def merge_2vec(file_list, output_path):
    sms_vec = []
    for file_path in file_list:
        vector = _read_vec(file_path)
        sms_vec.extend(vector)

    sms_vec = _merge_vec(sms_vec)

    with open(output_path, 'wb') as data_file:
        pickle.dump(sms_vec, data_file, pickle.HIGHEST_PROTOCOL)

if __name__ == '__main__':
    program = os.path.basename(sys.argv[0])  # get file name
    logger = logging.getLogger(program)
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)

    # input dir
    inp = '/data'
    foldername = 'sms_process2'
    outp = '/data/sms_process3'

    logger.info("running " + foldername + " files.")
    rootdir = inp + '/' + foldername

    # init multipool
    pool = mp.Pool(processes=50)

    # 3 args:1.parent dir 2.all dir names(without path) 3.all file names
    for parent, dirnames, filenames in os.walk(rootdir):
        if len(filenames) % 2 == 0:
            # b = dict(zip(filenames[0::2], filenames[1::2]))
            i_filenames = iter(filenames)
            dict_filenames = dict(izip(i_filenames, i_filenames))
            path = []
            for k, v in dict_filenames.items():
                path.append([rootdir + '/' + k, rootdir + '/' + v])

            for files in path:
                outfile_name = str(files[0][-51:-25]) + '-process3' + '.csv'  # output file
                output_path = outp + '/' + outfile_name
                pool.apply_async(merge_2vec, args=(files, output_path))

        else:
            cp_file = _read_vec(rootdir + '/' + str(filenames[-1]))
            with open(outp + '/' + str(filenames[-1][-51:-25]) + '-process3' + '.csv', 'wb') as vector_file:
                pickle.dump(cp_file, vector_file)
            del filenames[-1]

            i_filenames = iter(filenames)
            dict_filenames = dict(izip(i_filenames, i_filenames))
            path = []
            for k, v in dict_filenames.items():
                path.append([rootdir + '/' + k, rootdir + '/' + v])

            for files in path:
                outfile_name = str(files[0][-51:-25]) + '-process3' + '.csv'  # output file
                output_path = outp + '/' + outfile_name
                pool.apply_async(merge_2vec, args=(files, output_path))

    pool.close()
    pool.join()

    print 'game over'

for parent, dirnames, filenames in os.walk(rootdir):
    file_list = []
    for group in grouper(filenames, 5, fillvalue=None):
        tup = filter(None, group)
        path = []
        for filename in tup:
            path.append(rootdir + '/' + filename)
        file_list.append(path)

    for files in file_list:
        outfile_name = str(files[0][-51:-25]) + '-process3' + '.csv'  # output file
        output_path = outp + '/' + outfile_name
        pool.apply_async(merge_2vec, args=(files, output_path))


pool.close()
pool.join()

print 'game over'

# import os
# import sys
# import logging
# import pickle
# import multiprocessing as mp
#
#
# def get_uid(in_path, out_path, _dict):
#     with open(in_path, 'rb') as f:
#         _vec = pickle.load(f)
#
#     temp = []
#     for i in _vec:
#         if _dict.get(int(i[0])):
#             temp.append(i)
#         else:
#             pass
#
#     with open(out_path, 'wb') as f:
#         pickle.dump(temp, f, pickle.HIGHEST_PROTOCOL)
#
#
# if __name__ == '__main__':
#     program = os.path.basename(sys.argv[0])  # get file name
#     logger = logging.getLogger(program)
#     logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
#     logging.root.setLevel(level=logging.INFO)
#
#     # input dir
#     inp = '/data/sms_process2'
#     outp = '/data/sms_process3'
#
#     logger.info("running " + inp + " files.")
#
#     # get uid dict
#     with open('/data/sms_commonfiles/uid_label.dict', 'rb') as f:
#         uid_dict = pickle.load(f)
#
#     # init multipool
#     pool = mp.Pool(processes=4)
#
#     # 3 args:1.parent dir 2.all dir names(without path) 3.all file names
#     for parent, dirnames, filenames in os.walk(inp):
#         for filename in filenames:
#             input_path = inp + '/' + filename
#             outfile_name = str(filename[:-12]) + '-process3' + '.csv'  # output file
#             output_path = outp + '/' + outfile_name
#             pool.apply_async(get_uid, args=(input_path, output_path, uid_dict))
#
#     pool.close()
#     pool.join()
#
#     print 'game over'

# uidset = set([x for x in result if result.count(x) > 1])
