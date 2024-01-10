#!/usr/bin/python3

import numpy as np
import sys
import argparse
from seaborn import clustermap
from matplotlib import pyplot as plt
from scipy.cluster import hierarchy

def plot_all(data, name: str, labels, fig_size, method, threshold):
    res=clustermap(data, yticklabels=labels, xticklabels=labels, figsize=fig_size, method=method)
    dis_name = "%s_all.pdf" % name
    res.savefig(dis_name)
    plt.clf()
    r=hierarchy.dendrogram(res.dendrogram_row.linkage, orientation="left", labels = labels,distance_sort=False, color_threshold=threshold) 
    dis_den_name = "%s_dendro.pdf" % name
    plt.savefig(dis_den_name)
    c_set=set()
    dat_name="%s_dendro.dat" % name
    c_list=r["leaves_color_list"]
    country_list=r["ivl"]
    for col in c_list:
        c_set.add(col)
    c_set=[c for c in c_set]
    c_set.sort()
    
    file=open(dat_name, "w")
    for color in c_set:
        file.write("#")
        file.write(color)
        file.write("\n")
        length=len(c_list)
        for i in range(0,length):
            c=c_list[i]
            if c==color:
                data=country_list[i]
                file.write(data)
                file.write("\n")
    file.close()

argv=sys.argv
parser = argparse.ArgumentParser(
    prog="dendrogram.py", 
    description="Calculates the dendrogram of a correlation matrix",
    epilog="Possible methods: average single weighted centroid median ward complete"
)
parser.add_argument("correlation_matrix_file", type=str)
parser.add_argument("label_file", type=str)
parser.add_argument("output_stub", type=str)
parser.add_argument("method", type=str)
parser.add_argument('-s', '--scaling', type=float, default=1.0)
parser.add_argument('-t', '--threshold', type=float, help="threshold for groups", default=6)

args = parser.parse_args()
a_len=len(argv)

filename = args.correlation_matrix_file
label_name = args.label_file
output_stub=args.output_stub
method=args.method
threshold=args.threshold
scale = args.scaling

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
plot_all(dissimilarity, dis_name, labels, fig_size, method, threshold)
other= 1 - data
other_name = "%s_other" % output_stub
plot_all(other, other_name, labels, fig_size, method, threshold)
cor_name = "%s_correlation" % output_stub
plot_all(data, cor_name, labels, fig_size, method, threshold)


