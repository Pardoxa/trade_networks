#!/usr/bin/python3

import numpy as np
import sys
from scipy.cluster.hierarchy import linkage, dendrogram, fcluster
from scipy.spatial.distance import squareform
import matplotlib.pyplot as plt

filename = sys.argv[1]
label_name = sys.argv[2]

data = np.loadtxt(filename,dtype=float)
data = np.nan_to_num(data)
print(data)

labels = np.loadtxt(label_name, dtype=str, delimiter='@')
print(labels)

print(len(data))
print(len(data[0]))
plt.figure(figsize=(5,12))
dissimilarity = 1 - abs(data)
print(dissimilarity)
Z = linkage(dissimilarity, 'complete')
dendrogram(Z, orientation='left', leaf_rotation=0, labels=labels)
plt.savefig("den.pdf", format="pdf", bbox_inches="tight")