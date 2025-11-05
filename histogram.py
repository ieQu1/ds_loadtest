#!/usr/bin/env python
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import math

def measurement_groups(df, sweep_by):
    return (df
            .drop(columns=['Val', 'Metric', sweep_by])
            .drop_duplicates()
            )

def histogram(ax, bins, df, label, j):
    print('j =', j)
    subset = df.merge(pd.DataFrame([j]), how='inner')
    counts, bin_edges = np.histogram(subset['Val'], bins=50)
    ax.hist(
        bin_edges[:-1],
        bins=bins,
        weights=counts,
        alpha=1,
        label=label,
        linewidth=1,
        histtype='step',
        log=False
    )

def boxplot(ax, df, i):
    subset = df.merge(pd.DataFrame([i]), how='inner')
    subset.boxplot(column='Val', ax=ax, showfliers=False)

def sel_metric(df):
    return df[(df['Metric'] == 't')]

# Parse CLI args and load data
if len(sys.argv) < 3:
    print(
        """
Usage:

  histogram.py PlotTitle SweepBy ExperimentCSVFile+
        """,
        file=sys.stderr
    )
    exit(1)

title = sys.argv[1]
sweep_by = sys.argv[2]

def read_csv(i):
    filename = sys.argv[i]
    df = pd.read_csv(filename, sep=';')
    df['File'] = i - 2
    return df[(df['Metric'] == 't')]

df = pd.concat([read_csv(i) for i in range(3, len(sys.argv))])

partitions = sorted(df[sweep_by].unique())
groups = measurement_groups(df, sweep_by)

plt.title(title)
fig, ax = plt.subplots(nrows=len(partitions), ncols=2, sharey=False)
fig.set_size_inches(8, len(partitions) * 4)
fig.set_dpi(500)

pltn = 0
for i in partitions:
    part = df[(df[sweep_by] == i)]

    # Draw boxplots:
    bplt = ax[pltn, 0]
    part.boxplot(
        column='Val',
        ax=bplt,
        showfliers=False,
        showmeans=True,
        by=['File', 'DB']
    )
    bplt.set_title(sweep_by + '=' + str(i))
    bplt.grid(False)

    # Draw histograms:
    bins = np.linspace(part['Val'].min(), part['Val'].quantile(q=0.99), 30)
    hist = ax[pltn, 1]
    hist.grid(True)
    for j in groups.itertuples():
        histogram(hist, bins, part, str(j), j)

    pltn = pltn+1

plt.tight_layout()
plt.legend()
fn = title + ".png"
plt.savefig(fn)
