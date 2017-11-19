# @author: ellie

# Created on 2017-11-16

# -*- coding: utf-8 -*-
import collections
import tensorflow as tf
import numpy as np
from batched import batched
from EmbeddingModel import EmbeddingModel
from skipgrams import skipgrams
from readsms import ReadSms
from utils import AttrDict

WIKI_DOWNLOAD_DIR = './wikipedia/meta/'

params = AttrDict(
    vocabulary_size=10000,
    max_context=10,
    embedding_size=200,
    contrastive_examples=100,
    learning_rate=0.5,
    momentum=0.5,
    batch_size=1000,)

data = tf.placeholder(tf.int32, [None])
target = tf.placeholder(tf.int32, [None])
model = EmbeddingModel(data, target, params)

corpus = Wikipedia(
    'https://dumps.wikimedia.org/idwiki/20170901/'
    'idwiki-20170901-pages-meta-current.xml.bz2',
    WIKI_DOWNLOAD_DIR,
    params.vocabulary_size)
examples = skipgrams(corpus, params.max_context)
batches = batched(examples, params.batch_size)

sess = tf.Session()
sess.run(tf.global_variables_initializer())
average = collections.deque(maxlen=100)
for index, batch in enumerate(batches):
    feed_dict = {data: batch[0], target: batch[1]}
    cost, _ = sess.run([model.cost, model.optimize], feed_dict)
    average.append(cost)
    print('{}: {:5.1f}'.format(index + 1, sum(average) / len(average)))
    if index > 100000:
        break

embeddings = sess.run(model.embeddings)
np.save('./wikipedia/output/idembeddings.npy', embeddings)
sess.close()
