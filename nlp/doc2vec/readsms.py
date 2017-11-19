# @author: ellie

# Created on 2017-11-16

# -*- coding: utf-8 -*-
import bz2
import pickle
import collections
import os
from lxml import etree
from utils import download


class ReadSms:
    def __init__(self, inputfile_dir, outputfile_dir, vocabulary_size=10000):
        self._sms_path = inputfile_dir
        self._vocabulary_path = os.path.join(outputfile_dir, 'vocabulary.bz2')
        if not os.path.isfile(self._vocabulary_path):
            print 'build vocabulary'
            self._build_vocabulary(vocabulary_size)
        with bz2.BZ2File(self._vocabulary_path, 'r') as vocabulary:
            print("read vocabulary")
            self._vocabulary = [x.strip() for x in vocabulary]
        self._indices = {x: i for i, x in enumerate(self._vocabulary)}

    def __iter__(self):
        for sms_file in os.listdir(self._sms_path):
            if not os.path.isdir(sms_file):
                with open(self._sms_path + '/' + sms_file, 'rb') as data_file:
                    l = pickle.load(data_file)
                    for j in range(0, len(l)):
                        words = [x for x in l[j][1] if x is not '']
                        words = [self.encode(x) for x in words]
                        yield words

    @property
    def vocabulary_size(self):
        return len(self._vocabulary)

    def encode(self, word):
        return self._indices.get(word, 0)

    def decode(self, index):
        return self._vocabulary[index]

    def _build_vocabulary(self, vocabulary_size):
        counter = collections.Counter()
        for sms_file in os.listdir(self._sms_path):
            if not os.path.isdir(sms_file):
                with open(self._sms_path + '/' + sms_file, 'rb') as data_file:
                    l = pickle.load(data_file)
                    for j in range(0, len(l)):
                        words = [x for x in l[j][1] if x is not '']
                        counter.update(words)
        common = ['<unk>'] + counter.most_common(vocabulary_size - 1)
        common = [x[0] for x in common]
        with bz2.BZ2File(self._vocabulary_path, 'w') as vocabulary:
            for word in common:
                vocabulary.write(word + '\n')
