#!/usr/bin/python3

import numpy as np
import sys
from seaborn import clustermap

filename = sys.argv[1]
label_name = sys.argv[2]

data = np.loadtxt(filename,dtype=float)
data = np.nan_to_num(data)
print(data)

labels = np.loadtxt(label_name, dtype=str, delimiter='@')
print(labels)

print(len(data))
print(len(data[0]))
dissimilarity = 1 - abs(data)

res=clustermap(dissimilarity, yticklabels=labels, xticklabels=labels, figsize=(30,30))
res.savefig("dissimilarity.pdf")
res=clustermap(data, yticklabels=labels, xticklabels=labels, figsize=(30,30))
res.savefig("correlation.pdf")