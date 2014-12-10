import pandas as pd
from matplotlib import pyplot as plt
import json
import sys
import seaborn as sns

data = json.load(open('benchmarkresult.json'))
if not data['ok']:
    print 'Benchmark had error:', data['fatalmsg']
    sys.exit(1)

d = pd.DataFrame.from_records(data['metrics'])

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

#TODO: we are going to want to compare plots across providers, so we might need another key
ids = d['id'].unique()
for id_name in d['id'].unique():
    tmp = d[d['id'] == id_name]
    plt.clf()
    ax = sns.violinplot(tmp['value'], tmp['provider'], figsize=(24,16))
    ax.set_title(id_name)
    fig = ax.get_figure()
    fig.savefig('violin{0}.pdf'.format(id_name))


for id_name, groupdata in d.groupby('id'):
    print id_name
    groupdata['value'] /= groupdata['iteration'].max()
    print groupdata['value'].describe()
    plt.clf()
    ax = groupdata.plot(kind='line',x='iteration', y='value', figsize=(24,16), legend=False, table=groupdata['value'].describe())
    ax.set_ylabel('Latency (us)')
    ax.set_xlabel('')
    ax.set_title(id_name)
    fig = ax.get_figure()
    fig.savefig('{0}.pdf'.format(id_name))
