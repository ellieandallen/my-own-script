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

INPUT_SMS_DIR = '/data/sms_process1/'
OUTPUT_SMS_DIR = '/data/sms_process2/'

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

corpus = ReadSms(INPUT_SMS_DIR, OUTPUT_SMS_DIR, params.vocabulary_size)
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
np.save('/data/sms_process2/idembeddings.npy', embeddings)
sess.close()

# config = tf.ConfigProto()
# config.gpu_options.allow_growth = True
# config.log_device_placement = True
# sess = tf.Session(config=config)
# sess.run(tf.global_variables_initializer())
# average = collections.deque(maxlen=100)

# for index, batch in enumerate(batches):
#     num_gpus = 4
#     four_dict = list(next(enumerate(batches)) for i in range(num_gpus))
#     # feed_dict = {data: batch[0], target: batch[1]}
#     # cost, _ = sess.run([model.cost, model.optimize], feed_dict)
#     for i in range(num_gpus):
#         with tf.device(tf.DeviceSpec(device_type="GPU", device_index=i)):
#             with tf.variable_scope(tf.get_variable_scope(), reuse=i > 0):
#                 feed_dict = {data: four_dict[i][1][0], target: four_dict[i][1][1]}
#                 cost, _ = sess.run([model.cost, model.optimize], feed_dict)
#                 average.append(cost)
#     print('{}: {:5.1f}'.format(index + 1, sum(average) / len(average)))
#     if index > 100000:
#         break
        
# embeddings = sess.run(model.embeddings)
# np.save('/data/sms_process2/idembeddings.npy', embeddings)
# sess.close()
