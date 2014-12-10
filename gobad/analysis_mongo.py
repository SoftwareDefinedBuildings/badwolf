import pandas as pd
from matplotlib import pyplot as plt
import json
import sys

data = json.load(open('benchmarkresult.json'))
if not data['ok']:
    print 'Benchmark had error:', data['fatalmsg']
    sys.exit(1)

d = pd.DataFrame.from_records(data['metrics'])

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
