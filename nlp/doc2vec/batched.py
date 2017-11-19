# -*- coding: utf-8 -*-
import numpy as np


def batched(iterator, batch_size):
    """将数字流分批分组并将其作为Numpy数组生成."""
    while True:
        data = np.zeros(batch_size)
        target = np.zeros(batch_size)
        for index in range(batch_size):
            data[index], target[index] = next(iterator)
        yield data, target
