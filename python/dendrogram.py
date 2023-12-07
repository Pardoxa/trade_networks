#!/usr/bin/python3

import numpy as np
import sys
from seaborn import clustermap
from matplotlib import pyplot as plt
from scipy.cluster import hierarchy

def plot_all(data, name: str, labels, fig_size, method):
    res=clustermap(data, yticklabels=labels, xticklabels=labels, figsize=fig_size, method=method)
    dis_name = "%s_all.pdf" % name
    res.savefig(dis_name)
    plt.clf()
    hierarchy.dendrogram(res.dendrogram_row.linkage, orientation="left", labels = labels,distance_sort=False) 
    dis_den_name = "%s_dendro.pdf" % name
    plt.savefig(dis_den_name)

argv=sys.argv
a_len=len(argv)
usage="Usage:\n%s <correlation_matrix_file> <label_file> <output_stub> <method> optional: <scaling>" % argv[0]
possible_methods="Possible methods: average single weighted centroid median ward"
if a_len < 5:
    print("Too few arguments")
    print(usage)
    print(possible_methods)
    exit(-1)
elif a_len > 6:
    print("Too many arguments")
    print(usage)
    print(possible_methods)
    exit(-1)

filename = argv[1]
label_name = argv[2]
output_stub=argv[3]
method=str(argv[4])

scale = float(argv[5]) if a_len == 6 else 1.0

data = np.loadtxt(filename, dtype=float)
data = np.nan_to_num(data)
print(data)

labels = np.loadtxt(label_name, dtype=str, delimiter='@')
print(labels)

print(len(data))
print(len(data[0]))
dissimilarity = 1 - abs(data)
dis_name = "%s_dissimilarity" % output_stub
fig_size=(30 * scale, 30.0 * scale)
plot_all(dissimilarity, dis_name, labels, fig_size, method)
other= 1 - data
other_name = "%s_other" % output_stub
plot_all(other, other_name, labels, fig_size, method)
cor_name = "%s_correlation" % output_stub
plot_all(data, cor_name, labels, fig_size, method)


