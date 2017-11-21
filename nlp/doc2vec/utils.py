# -*- coding: utf-8 -*-

import errno
import os
import shutil
import functools
import pickle
import tensorflow as tf

try:
    from urllib.request import urlopen
except:
    from urllib2 import urlopen


def ensure_directory(directory):
    """创建指定路径上不存在的目录"""
    directory = os.path.expanduser(directory)
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e


def download(url, directory, filename = None):
    """下载一个文件， 返回文件名，已存在则不下载，未指定文件名则从url解析并返回path"""
    if not filename:
        _, filename = os.path.split(url)
    directory = os.path.expanduser(directory)
    ensure_directory(directory)
    filepath = os.path.join(directory, filename)
    if os.path.isfile(filepath):
        return filepath
    print("Download", filepath)
    with urlopen(url) as response, open(filepath, 'wb') as file_:
        shutil.copyfileobj(response, file_)
    return filepath


def disk_cache(basename, directory, method = False):
    """用于将可腌制的（pickleable）的返回值缓存到磁盘的函数修饰器
    对于无效的情况，利用一个从函数参数计算得到散列值
    如果是‘method’，则跳过第一个参数（通常是self或cls）
    缓存的filepath为'directory/basename-hash.pickle'"""
    directory = os.path.expanduser(directory)
    ensure_directory(directory)

    def wrapper(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            key = (tuple(args), tuple(kwargs.items()))
            # 无效散列值不可用self或cls
            if method and key:
                key = key[1:]
            filename = '{}-{}.pickle'.format(basename, hash(key))
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                with open(filepath, 'rb') as handle:
                    return pickle.load(handle)
            result = func(*args, **kwargs)
            with open(filepath, 'wb') as handle:
                pickle.dump(result, handle)
            return result
        return wrapped
    return wrapper

# example:
# @disk_cache('dataset', '/home/ubuntu/dataset')
# def get_dataset(one_hot = True):
#     dataset = Dataset('http://example.com/dataset.bz2')
#     dataset = Tokenize(dataset)
#     if one_hot:
#         dataset = OneHotEncoding(dataset)
#     return dataset


class AttrDict(dict):
    """属性字典"""
    def __getattr__(self, key):
        if key not in self:
            raise AttributeError
        return self[key]

    def __setattr__(self, key, value):
        if key not in self:
            raise AttributeError
        self[key] = value

# example
# parmas = AttrDict({'key': value, })
# params = AttrDict(key = value, )


# def get_params():
#     key = value
#     return AttrDict(**locals())


def get_params():
    learning_rate = 0.003
    optimizer = tf.train.AdamOptimizer(learning_rate)
    return AttrDict(**locals())

# 惰性属性修饰器
# 保证后续对属性名的调用将返回数据流图中已有节点


def lazy_property(function):
    attribute = '_lazy_' + function.__name__

    @property
    @functools.wraps(function)
    def wrapper(self):
        if not hasattr(self, attribute):
            setattr(self, attribute, function(self))
        return getattr(self, attribute)
    return wrapper

# example


class Model:
    def __init__(self, data, target):
        self.data = data
        self.target = target
        self.prediction  # 增加
        self.optimize  # 增加
        self.error  # 增加

    # @property改为
    @lazy_property
    def prediction(self):
        data_size = int(self.data.get_shape()[1])
        target_size = int(self.target.get_shape()[1])
        weight = tf.Variable(tf.truncated_normal([data_size, target_size]))
        bias = tf.Variable(tf.constant(0.1, shape = [target_size]))
        incoming = tf.matmul(self.data, weight) + bias
        return tf.nn.softmax(incoming)

    # @property改为
    @lazy_property
    def optimize(self):
        cross_entropy = -tf.reduce_sum(self.target, tf.log(self.prediction))
        optimizer = tf.train.RMSPropOptimizer(0.03)
        return optimizer.minimize(cross_entropy)

    # @property改为
    @lazy_property
    def error(self):
        mistakes = tf.not_equal(tf.argmax(self.target, 1), tf.argmax(self.prediction, 1))
        return tf.reduce_mean(tf.cast(mistakes, tf.float32))

# 默认数据流图
# def main():
#     data = tf.placeholder()
#     target = tf.placeholder()
#     model = Model()
#
# with tf.Graph().as_default():
#     main()


def overwrite_graph(function):
    """覆盖数据流图修饰器"""
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        with tf.Graph().as_default():
            return function(*args, **kwargs)
        return wrapper

# example
# @overwrite_graph
# def main():

#     data = tf.placeholder()
#     target = tf.placeholder()
#     model = Model()
# main()
