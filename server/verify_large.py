"""Stream every export over HTTP and verify all fixture boundaries without Spark."""
import concurrent.futures,gzip,hashlib,json,os,time,urllib.request,urllib.error
from pathlib import Path
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parent; BASE=os.environ.get('SYNTHETIC_TEST_BASE','http://127.0.0.1:18600')
def get(path):
    with urllib.request.urlopen(BASE+path,timeout=120) as r:return r.read()
def verify(prefix):
    deadline=time.monotonic()+1200
    while True:
        try:m=json.loads(get(prefix+'/manifest.json'));break
        except urllib.error.HTTPError as e:
            if e.code!=503 or time.monotonic()>deadline:raise
            time.sleep(10)
    rows=pq.ParquetFile(ROOT/'data'/prefix.strip('/')/'rendered.parquet').metadata.num_rows
    assert rows==m['row_count']
    for limit in (500,5000):
        last=(rows-1)//limit*limit
        c=json.loads(get(prefix+f'/cursor?cursor={last}&limit={limit}'))
        assert c['next']=='' and len(c['items'])==rows-last
        assert json.loads(get(prefix+f'/offset?offset={rows}&limit={limit}'))=={'results':[]}
    digest=hashlib.sha256();lines=0;size=0
    with urllib.request.urlopen(BASE+prefix+'/export.ndjson',timeout=120) as response:
        assert response.headers.get_content_type()=='application/x-ndjson'
        while True:
            block=response.read(4*1024*1024)
            if not block:break
            digest.update(block);lines+=block.count(b'\n');size+=len(block)
    assert digest.hexdigest()==m['ndjson_sha256'];assert lines==rows;assert size==m['json_bytes_ndjson']
    result={'fixture':prefix,'rows':rows,'json_bytes':size,'http_export_sha256':digest.hexdigest(),'verified':True}
    print(json.dumps(result),flush=True);return result
prefixes=[f'/{d}/{s}/{v}' for d in ('taxi','lineitem') for s in ('100mb','2gb','10gb') for v in ('clean','messy')]
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
    result=list(pool.map(verify,prefixes))
(ROOT/'verification-all.json').write_text(json.dumps(result,indent=2))
