# @author: lionheart

# Created on 2017-09-26

from perceptron import Perceptron

f = lambda x: x


class LinearUnit(Perceptron):
    def __init__(self, input_num):
        Perceptron.__init__(self, input_num, f)


def get_training_dataset():
    input_vecs = [[5], [7], [4.5], [9], [1.6]]
    labels = [5500, 7400, 4100, 10000, 5000]
    return input_vecs, labels


def train_linear_unit():
    lu = LinearUnit(1)
    input_vecs, labels = get_training_dataset()
    lu.train(input_vecs, labels, 20000, 0.01)
    return lu


if __name__ == '__main__':
    linear_unit = train_linear_unit()
    print linear_unit
    print linear_unit.predict([1.5])
    print linear_unit.predict([10])
