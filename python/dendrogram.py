#!/usr/bin/python3

import numpy as np
import sys
from seaborn import clustermap

argv=sys.argv
a_len=len(argv)
expected="Usage:\ndendrogram <correlation_matrix_file> <label_file> <output_stub> optional: <scaling>"
if a_len < 4:
    print("To few arguments")
    print(expected)
    exit(-1)
elif a_len > 5:
    print("Too many arguments")
    print(expected)
    exit(-1)

filename = argv[1]
label_name = argv[2]
output_stub=argv[3]

scale = float(argv[4]) if len(argv) == 5 else 1.0

data = np.loadtxt(filename, dtype=float)
data = np.nan_to_num(data)
print(data)

labels = np.loadtxt(label_name, dtype=str, delimiter='@')
print(labels)

print(len(data))
print(len(data[0]))
dissimilarity = 1 - abs(data)

fig_size=(30 * scale, 30.0 * scale)
res=clustermap(dissimilarity, yticklabels=labels, xticklabels=labels, figsize=fig_size)
dis_name = "%s_dissimilarity.pdf" % output_stub
res.savefig(dis_name)
res=clustermap(data, yticklabels=labels, xticklabels=labels, figsize=fig_size)
cor_name = "%s_correlation.pdf" % output_stub
res.savefig(cor_name)