# @author: ellie

# Created on 2017-11-20

import logging
import os
import sys
import pickle
import multiprocessing as mp
import time
from itertools import groupby
import collections


def _word_vector_update(word_vector, word, vocab):
    a = 0
    for j in range(0, len(word_vector)):
        if vocab[word] == word_vector[j][0]:
            a += 1
            word_vector[j] = (word_vector[j][0], word_vector[j][1] + 1)
        else:
            pass
    return a, word_vector


def _count_word(list_of_tuple):
    dct = collections.defaultdict(int)
    for cust_id, contrib in list_of_tuple:
        dct[cust_id] += contrib

    return dct.items()


def _merge_vec(vector):
    result = []
    for key, group in groupby(vector, lambda x: x[0]):
        temp = []
        # listOfThings = ",".join([thing[1] for thi  ng in group])
        # listOfThings.extend([thing[1] for thing in group])
        for vec in group:
            temp.extend(vec[1])
        result.append([key, temp])

    # add count of words
    for i in range(0, len(result)):
        result[i][1] = _count_word(result[i][1])

    # or i can use this code
    # for i in range(0, len(result)):
    #     temp = []
    #     for a, b in groupby(sorted(result[i][1]), key=lambda item: item[0]):
    #         temp.append((a, sum([item[1] for item in list(b)])))
    #     result[i][1] = temp

    return result


def sms2vec(fullname, output_path):
    # read the word dictionary, len(dict) == 10000
    with open('/data/sms_commonfiles/vocab.dict', 'rb') as dict_file:
        vocab = pickle.load(dict_file)

    # read sms
    with open(fullname, 'rb') as data_file:
        sms = pickle.load(data_file)

    # transform every sms to a vector like[(6308, 1), (1675, 1), (1696, 1), (1488, 1), (269, 1), (1534, 2), (1709, 1)]
    for k in range(0, len(sms)):
        temp = []
        for i in sms[k][1]:
            if vocab.get(i):
                _exist, _update = _word_vector_update(temp, i, vocab)
                if _exist:
                    temp = _update
                else:
                    temp.append((vocab[i], 1))
            else:
                pass
        sms[k][1] = temp

    # merge uid's sms into one vector
    sms = _merge_vec(sms)

    # dump data
    with open(output_path, 'w') as data_file:
        pickle.dump(sms, data_file)

    print "Finish file: " + rootdir + '/' + filename + ", " + time.time()

if __name__ == '__main__':
    program = os.path.basename(sys.argv[0])  # get file name
    logger = logging.getLogger(program)
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)

    # input dir
    inp = '/data'
    folders = ['sms_process1']
    outp = '/data/sms_process2'

    # # get the word dictionary, len(dict) == 10000
    # with open('/data/sms_commonfiles/vocabulary_1.list', 'r') as v:
    #     vocabulary = pickle.load(v)
    # for i in range(0, len(vocabulary)):
    #     vocabulary[i] = vocabulary[i].replace('\n', '')
    #
    # vocab = {k: v for v, k in enumerate(vocabulary)}
    # with open('/data/sms_commonfiles/vocab.dict', 'wb') as f:
    #     pickle.dump(vocab, f, pickle.HIGHEST_PROTOCOL)

    # init multipool
    pool = mp.Pool(processes=50)

    for foldername in folders:
        logger.info("running " + foldername + " files.")

        rootdir = inp + '/' + foldername

        # 3 args:1.parent dir 2.all dir names(without path) 3.all file names
        for parent, dirnames, filenames in os.walk(rootdir):
            for filename in filenames:
                outfile_name = filename + '-process2' + '.csv'  # output file
                output_path = outp + '/' + outfile_name

                logger.info("Dealing with file: " + rootdir + '/' + filename)

                pool.apply_async(sms2vec, args=(rootdir + '/' + filename, output_path))
                # print rootdir + '/' + filename

        pool.close()
        pool.join()

    print '''game over'''
