# @author: ellie

# Created on 2017-11-06

# -*- coding: utf-8 -*-

import logging
import os.path
import sys
import re
import pickle
import multiprocessing as mp
import time


# transfrom a message text to a word list
def strip_stopwords(fullname, output_path):
    with open('/data/sms_commonfiles/id.stopwords.02.01.2016.txt', 'rb') as data_file:
        stoplist = data_file.read()
    stoplist = stoplist.split('\n')  # str2list
    #     print type(stoplist)  # list
    stoplist.pop()

    documents = []
    with open(fullname, 'rb') as doc_file:
        for line in doc_file.readlines():
            if re.match('^\d{13}\|', line):
                documents.append(line)
            else:
                l = documents[-1].replace('\n', ' ') + line
                documents[-1] = l

    texts = []
    for i in range(0, len(documents)):
        temp = documents[i].split('|')
        temp[12] = re.sub("[\s\.\!\/{}\?\-\=<>\;\\\\《》\|\[\]_,$%^*:()\"\'+——！，。？、~@#【】；￥%……&*（）]+$", "",
                          temp[12].lower())
        temp[12] = re.sub("^[\s\.\!\/{}\?\-\=<>\;\\\\《》\|\[\]_,$%^*:()\"\'+——！，。？、~@#【】；￥%……&*（）]+", "",
                          temp[12].lower())
        temp[12] = re.split("[\s\.\!\/{}\?\-\=<>\;\\\\《》\|\[\]_,$%^*:()\"\'+——！，。？、~@#【】；￥%……&*（）]+", temp[12])
        result = [temp[4], [word for word in temp[12] if word not in stoplist]]
        texts.append(result)

    # dump data
    with open(output_path, 'w') as data_file:
        pickle.dump(texts, data_file)

    print "Finish file: " + rootdir + '/' + filename + ", " + time.time()


if __name__ == '__main__':
    program = os.path.basename(sys.argv[0])  # 得到文件名
    logger = logging.getLogger(program)
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)

    # 输入文件目录
    inp = '/data'
    folders = ['userSmsMessage']
    outp = '/data/sms_process1'

    # 多进程初始化
    pool = mp.Pool(processes=50)

    for foldername in folders:
        i = 0
        logger.info("running " + foldername + " files.")

        rootdir = inp + '/' + foldername

        # 三个参数：分别返回1.父目录 2.所有文件夹名字（不含路径） 3.所有文件名字
        for parent, dirnames, filenames in os.walk(rootdir):
            for filename in filenames:
                outfile_name = filename + '-process1' + '.csv'  # 输出文件
                output_path = outp + '/' + outfile_name

                logger.info("Dealing with file: " + rootdir + '/' + filename)

                pool.apply_async(strip_stopwords, args=(rootdir + '/' + filename, output_path))
                #                 print rootdir + '/' + filename

                i += 1
                #                 logger.info("Saved " + str(i) + " files.")
        pool.close()
        pool.join()

    print '''game over'''
