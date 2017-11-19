# @author: ellie

# Created on 2017-09-29

f1 = lambda x: (x * (x + 1))
l = [1, 4, 3, 7, 6, 8]
l = map(f1, l)
print l

f2 = lambda x, y: x * y
l = reduce(f2, l)
print l
print 'this is map and reduce'

l = [i for i in range(1, 10)]
print l
print l[-2::-1]
print l[-1]

