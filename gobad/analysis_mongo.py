import pandas as pd
import json
import sys

data = json.load(open('benchmarkresult.json'))
if not data['ok']:
    print 'Benchmark had error:', data['fatalmsg']
    sys.exit(1)

d = pd.DataFrame.from_records(data['metrics'])

for id_name, groupdata in d.groupby('id'):
    print id_name, groupdata['value'].describe()
