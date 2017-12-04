# @author: ellie

# Created on 2017-12-01

import numpy as np
from scipy import sparse
import pickle


def read_data(file_name):
    with open('/data/sms_commonfiles/' + file_name, 'rb') as data_file:
        # uid_label = {k: v for k, v in uid_label}
        data = pickle.load(data_file)
    return data


def write_data(file_name, data):
    with open('/data/sms_commonfiles/' + file_name, 'wb') as data_file:
        pickle.dump(data, data_file, pickle.HIGHEST_PROTOCOL)


def _vec_split(all_label, all_vector):
    _label = []
    _uid = []
    _train_set = []

    for i in all_vector:
        if all_label.get(int(i[0]), -1) != -1:
            _label.append(all_label.get(int(i[0])))
            _uid.append(int(i[0]))
            _train_set.append(i[1])
        else:
            pass

    _label = np.asarray(_label, dtype=np.float32)
    _uid = np.asarray(_uid, dtype=np.float32)
    return _label, _uid, _train_set


def _vec2matrix(vec):
    data = []
    row = []
    col = []
    for i in range(0, len(vec)):
        for j in vec[i]:
            data.append(j[1])
            col.append(j[0])
            row.append(i)

    _matrix = sparse.csr_matrix((data, (row, col)), shape=(len(vec), 10000))
    return _matrix


if __name__ == '__main__':
    uid_label = read_data('uid_label.dict')
    uid_vector = read_data('uid_vector.vec')
    # uid_vector = sorted(uid_vector, key=lambda tup: int(tup[0]))
    label, uid, train_set = _vec_split(uid_label, uid_vector)

    # sparse_A = sparse.csr_matrix(np.matrix(list_A))
    matrix = _vec2matrix(train_set)
    # matrix.toarray()
    write_data('uid.matrix', matrix)
    write_data('uid.label', label)
    write_data('uid', uid)
    print 'game over'
