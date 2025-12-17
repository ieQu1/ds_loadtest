#!/usr/bin/env python
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def sel_metric(df):
    return df[(df['Metric'] == 'tps')]

# Load data
title = sys.argv[1]
output = sys.argv[2]
pivot = sys.argv[3]
df_e = sel_metric(pd.read_csv(sys.argv[4], sep=';'))
df_c = sel_metric(pd.read_csv(sys.argv[5], sep=';'))
df_e['type']='Experiment'
df_c['type']='Control'
df = pd.concat([df_e, df_c])

print(df)

# Plot data
df.pivot(index=pivot, columns='type', values='Val').plot()

plt.title('Throughput of '+ title +' operations')
plt.xlabel(pivot)
plt.ylabel('Throughput, MB/s')
plt.tight_layout()
plt.legend()
plt.savefig(output)
