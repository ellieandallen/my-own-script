# @author: ellie

# Created on 2017-11-06

import gensim
import os
import logging
import sys
import pickle
import time


def deal_blank(l):
    corpus = []
    for j in range(0, len(l)):
        corpus.append(gensim.models.doc2vec.TaggedDocument([x for x in l[j][1] if x is not ''], [j]))
    return corpus


if __name__ == '__main__':
    program = os.path.basename(sys.argv[0])  # get file name
    logger = logging.getLogger(program)
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)

    # input directory
    inp = '/data'
    folders = ['sms_process1']
    # outp = '/data/sms_process2'

    # init doc2vec model
    model = gensim.models.doc2vec.Doc2Vec(size=64, min_count=20, iter=100)

    for foldername in folders:
        i = 0
        logger.info("running " + foldername + " files.")

        rootdir = inp + '/' + foldername

        # three parameters:1.parent directory 2.folder name 3.file name
        for parent, dirnames, filenames in os.walk(rootdir):
            for filename in filenames:
                logger.info("Dealing with file: " + rootdir + '/' + filename)
                logger.info(time.time())
                # read file to a list and process it
                with open(rootdir + '/' + filename, 'rb') as data_file:
                    train_file = pickle.load(data_file)

                train_corpus = deal_blank(train_file)
                try:
                    model = gensim.models.doc2vec.Doc2Vec.load('/data/model/doc2vec.model1')
                except:
                    pass
                try:
                    model.build_vocab(train_corpus)
                except:
                    pass
                model.train(train_corpus, total_examples=model.corpus_count, epochs=model.iter)
                # print rootdir + '/' + filename
                model.save('/data/model/doc2vec.model1')

                # i += 1
                # logger.info("Saved " + str(i) + " files.")

    print '''game over'''
