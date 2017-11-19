'''
Created on 2017/06/27

@author: ellie
'''

import logging
from collections import defaultdict
from pprint import pprint
import os
from gensim import corpora, models, similarities
import numpy as np


logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

# list1 = ['a', 'a', 'a']
# str1 = ''.join(list1)
# print str1
# print type(str1)


def get_corpus():
    documents = []
    with open('/home/ubuntu/data/ellie/sms', 'rb') as doc_file:
        for line in doc_file.readlines():
            documents.append(line)
    print type(documents)  # list
    print type(documents[0])  # str

    with open('./id.stopwords.02.01.2016.txt', 'rb') as data_file:
        stoplist = data_file.read()
    stoplist = stoplist.split('\n')  # str2list
    print type(stoplist)  # list
    stoplist.pop()

    texts = [[word for word in document.lower().replace('/n', ' ').replace('"', ' ').split() if word not in stoplist]
            for document in documents]
    print type(texts)  # list

    frequency = defaultdict(int)
    for text in texts:
        for token in text:
            frequency[token] += 1

    texts = [[token for token in text if frequency[token] > 1]
             for text in texts]

    pprint(texts)

    dictionary = corpora.Dictionary(texts)  # get the dict
    dictionary.save('/home/ubuntu/data/ellie/sms.dict')

    corpus = [dictionary.doc2bow(text) for text in texts]  # get the corpus
    corpora.MmCorpus.serialize('/home/ubuntu/data/ellie/sms.mm', corpus)


def get_model():
    if os.path.exists('/home/ubuntu/data/ellie/sms.dict'):
        dictionary = corpora.Dictionary.load('/home/ubuntu/data/ellie/sms.dict')
        corpus = corpora.MmCorpus('/home/ubuntu/data/ellie/sms.mm')
        print 'it is ok'

        tfidf = models.TfidfModel(corpus)  # initialize a model
        corpus_tfidf = tfidf[corpus]
        # for doc in corpus_tfidf:
        #     print doc

        lsi = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=50)
        corpus_lsi = lsi[corpus_tfidf]
        lsi.save('/home/ubuntu/data/ellie/sms.lsi')  # use lsi
        # lsi = models.LsiModel.load('/home/ubuntu/data/ellie/sms.lsi')
        # lsi.print_topics(50)
        # index = similarities.MatrixSimilarity(corpus_lsi)
        # a = index[corpus_lsi]
        # b = list(enumerate(a[0]))
        # c = []
        # for i in range(0, len(b)):
        #     if b[i][1] > 0.8:
        #         c.append(b[i])
        # print c

        rp = models.RpModel(corpus_tfidf, num_topics=50)
        corpus_rp = rp[corpus_tfidf]
        rp.save('/home/ubuntu/data/ellie/sms.rp')  # use rp

        lda = models.LdaModel(corpus_tfidf, id2word=dictionary, num_topics=50)
        corpus_lda = lda[corpus_tfidf]
        lda.save('/home/ubuntu/data/ellie/sms.lda')  # use lda
        index = similarities.MatrixSimilarity(corpus_lda)
        a = index[corpus_lda]


        hdp = models.HdpModel(corpus, id2word=dictionary)
        corpus_hdp = hdp[corpus]
        hdp.save('/home/ubuntu/data/ellie/sms.hdp')  # use hdp

    else:
        print 'no dict'