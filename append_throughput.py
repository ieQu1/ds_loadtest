#!/usr/bin/env python
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def sel_metric(df):
    return df[(df['Metric'] == 'throughput')]

# Load data
output = sys.argv[1]
df_e = sel_metric(pd.read_csv(sys.argv[2], sep=';'))
df_c = sel_metric(pd.read_csv(sys.argv[3], sep=';'))
df_e['type']='Experiment'
df_c['type']='Control'
df = pd.concat([df_e, df_c])

print(df)

# Plot data
df.pivot(index='PayloadSize', columns='type', values='Val').plot()

plt.title('Throughput of append operations')
plt.xlabel('Payload size, B')
plt.ylabel('Throughput, MB/s')
plt.tight_layout()
plt.legend()
plt.savefig(output)
