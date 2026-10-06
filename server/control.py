import argparse,json,subprocess,time,urllib.request,urllib.error
from pathlib import Path
from settings import DEFAULT
p=argparse.ArgumentParser();p.add_argument('command',choices=['start','stop','status','profile','reset','sync']);p.add_argument('--latency',type=int,choices=[0,50,150,300]);p.add_argument('--rps',type=int);p.add_argument('--retry-after',type=int);p.add_argument('--retry-format',choices=['seconds','date']);p.add_argument('--errors',type=float);p.add_argument('--resets',type=float);p.add_argument('--seed',type=int);a=p.parse_args()
if a.command in ('start','stop','status'):
    raise SystemExit(subprocess.call(['sudo','-n','systemctl',a.command,'synthetic-rest-api.service','synthetic-rest-toxiproxy.service']))
path=Path(__file__).resolve().parent/'profile.json'
v=DEFAULT.copy() if a.command=='reset' or not path.exists() else json.loads(path.read_text())
for arg,key in [('latency','latency_ms'),('rps','rate_per_second'),('retry_after','retry_after'),('retry_format','retry_format'),('errors','errors'),('resets','resets'),('seed','seed')]:
    if getattr(a,arg) is not None:v[key]=getattr(a,arg)
if not 0<=v['errors']<=1 or not 0<=v['resets']<=1 or v['errors']+v['resets']>1 or v['rate_per_second']<0 or v['retry_after']<1:raise ValueError('invalid profile')
def api(method,path,data=None):
    r=urllib.request.Request('http://127.0.0.1:18604'+path,data=json.dumps(data).encode() if data is not None else None,method=method,headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(r,timeout=5) as response:return response.read()
# Toxiproxy runs only under install.sh. sync is its start hook, so wait for it then; otherwise a
# server run without it can still use every profile but --resets.
proxy=False;tries=30 if a.command=='sync' else 1
for attempt in range(tries):
    try:api('GET','/proxies');proxy=True;break
    except urllib.error.URLError:
        if attempt<tries-1:time.sleep(.2)
if proxy:
    try:api('DELETE','/proxies/synthetic/toxics/reset_peer')
    except urllib.error.HTTPError as e:
        if e.code!=404:raise
    if v['resets']:
        api('POST','/proxies/synthetic/toxics',{'name':'reset_peer','type':'reset_peer','stream':'downstream','toxicity':v['resets'],'attributes':{'timeout':0}})
elif v['resets']:raise SystemExit('--resets needs Toxiproxy, which deploy/install.sh runs on 127.0.0.1:18604')
tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(v));tmp.replace(path);print(json.dumps(v,indent=2))
