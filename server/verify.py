"""HTTP checks and independent oracle verification for the smallest clean fixture."""
import concurrent.futures, gzip, hashlib, json, os, subprocess, time, urllib.request, urllib.error
from pathlib import Path
import duckdb

ROOT=Path(__file__).resolve().parent; BASE=os.environ.get('SYNTHETIC_TEST_BASE','http://127.0.0.1:18600')
def request(path,encoding='identity'):
    with urllib.request.urlopen(urllib.request.Request(BASE+path,headers={'Accept-Encoding':encoding}),timeout=60) as r:
        b=r.read();h=r.headers  # HTTP field names are case-insensitive, including Hyper's lowercase names.
        if h.get('Content-Encoding')=='gzip':b=gzip.decompress(b)
        return r.status,h,b

for dataset in ('taxi','lineitem'):
 for variant in ('clean','messy'):
    prefix=f'/{dataset}/100mb/{variant}'
    m=json.loads(request(prefix+'/manifest.json')[2])
    for limit in (500,5000):
        body=request(prefix+f'/offset?offset=0&limit={limit}')[2]
        assert len(json.loads(body)['results'])==limit
        assert request(prefix+f'/offset?offset=0&limit={limit}','gzip')[2]==body
        assert json.loads(request(prefix+f'/offset?offset={m["row_count"]}&limit={limit}')[2])=={'results':[]}
        c=json.loads(request(prefix+f'/cursor?limit={limit}')[2]);assert c['next']==str(limit) and len(c['items'])==limit
        last=(m['row_count']-1)//limit*limit
        assert json.loads(request(prefix+f'/cursor?cursor={last}&limit={limit}')[2])['next']==''
        assert 'rel="next"' in request(prefix+f'/link?limit={limit}')[1]['Link']
        assert 'Link' not in request(prefix+f'/link?offset={last}&limit={limit}')[1]
    raw=request(prefix+'/export.ndjson')[2]
    assert hashlib.sha256(raw).hexdigest()==m['ndjson_sha256']
    assert raw.count(b'\n')==m['row_count']
    # Independent recomputation of one numeric sum and null count from canonical Parquet.
    con=duckdb.connect();file=ROOT/'data'/dataset/'100mb'/variant/'rendered.parquet'
    k='fare_amount' if dataset=='taxi' else 'l_extendedprice'
    val=f"json_extract_string(payload,'$.{k}')"
    sums=con.execute(f"SELECT sum(try_cast({val} AS DECIMAL(38,10))), count(*) FILTER(WHERE {val} IS NULL) FROM read_parquet(?)",[str(file)]).fetchone()
    assert str(sums[0])==m['numeric'][k]['sum'];assert sums[1]==m['null_count_including_missing'][k]
    print('PASS',prefix,m['row_count'])

def profile(*args):subprocess.run([str(ROOT/'.venv/bin/python'),str(ROOT/'control.py'),'reset'],check=True,stdout=subprocess.DEVNULL);subprocess.run([str(ROOT/'.venv/bin/python'),str(ROOT/'control.py'),'profile',*args],check=True,stdout=subprocess.DEVNULL)
try:
 for delay in (0,50,150,300):
    profile('--latency',str(delay));t=time.monotonic();request('/taxi/offset?limit=500');elapsed=time.monotonic()-t;assert elapsed>=delay/1000;print('LATENCY',delay,round(elapsed,3))
 for form in ('seconds','date'):
    profile('--rps','1','--retry-format',form)
    statuses=[]
    for _ in range(12):
      try:request('/taxi/offset?limit=500')
      except urllib.error.HTTPError as e:
        if e.code==429:statuses.append(e.headers['Retry-After'])
    assert statuses;assert statuses[0].isdigit()==(form=='seconds');print('429',form,statuses[0])
 profile('--errors','1')
 try:request('/taxi/offset?limit=500');raise AssertionError('No 500')
 except urllib.error.HTTPError as e:assert e.code==500
 profile('--resets','1')
 try:request('/taxi/offset?limit=500');raise AssertionError('No reset')
 except (urllib.error.URLError,ConnectionError, __import__('http.client',fromlist=['RemoteDisconnected']).RemoteDisconnected):pass
 print('FAULTS PASS')
finally:subprocess.run([str(ROOT/'.venv/bin/python'),str(ROOT/'control.py'),'reset'],check=True)
