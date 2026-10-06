"""Boundary, method and gzip checks for all twelve Axum fixture routes."""
import gzip,json,os,urllib.request,urllib.error
BASE=os.environ.get('SYNTHETIC_TEST_BASE','http://127.0.0.1:18600')
# The server's SYNTHETIC_PUBLIC_BASE, which its Link headers use.
PUBLIC=os.environ.get('SYNTHETIC_PUBLIC_BASE',BASE).rstrip('/')
def request(path,encoding='identity',method='GET'):
    req=urllib.request.Request(BASE+path,headers={'Accept-Encoding':encoding},method=method)
    try:
        with urllib.request.urlopen(req,timeout=60) as r:return r.status,r.headers,r.read()
    except urllib.error.HTTPError as e:return e.code,e.headers,e.read()
assert json.loads(request('/health')[2])=={'status':'ok','ready':12}
for dataset in ('taxi','lineitem'):
 for size in ('100mb','2gb','10gb'):
  for variant in ('clean','messy'):
   prefix=f'/{dataset}/{size}/{variant}'
   manifest=json.loads(request(prefix+'/manifest.json')[2]);rows=manifest['row_count']
   assert request(prefix+'/openapi.yaml')[0]==200
   for limit in (500,5000):
    page=request(prefix+f'/offset?limit={limit}')
    assert page[0]==200 and len(json.loads(page[2])['results'])==limit
    compressed=request(prefix+f'/offset?limit={limit}','gzip')
    assert compressed[1].get('Content-Encoding')=='gzip'
    assert gzip.decompress(compressed[2])==page[2]
    identity=request(prefix+f'/offset?limit={limit}','gzip;q=0')
    assert identity[1].get('Content-Encoding') is None and identity[2]==page[2]
    assert request(prefix+f'/offset?limit={limit}',method='HEAD')[2]==b''
    last=(rows-1)//limit*limit
    for style,field in [('offset','offset'),('cursor','cursor'),('link','offset')]:
     final=request(prefix+f'/{style}?{field}={last}&limit={limit}')
     body=json.loads(final[2]);assert len(body['items' if style=='cursor' else 'results'])==rows-last
     if style=='cursor':assert body['next']==''
     if style=='link':assert final[1].get('Link') is None
     empty=json.loads(request(prefix+f'/{style}?{field}={rows+1}&limit={limit}')[2])
     assert empty==({'items':[],'next':''} if style=='cursor' else {'results':[]})
    link=request(prefix+f'/link?limit={limit}')
    assert link[1]['Link']==f'<{PUBLIC}{prefix}/link?offset={limit}&limit={limit}>; rel="next"'
   for suffix in ('/offset?offset=-1','/offset?offset=1','/offset?offset=nope','/offset?limit=42','/cursor?cursor=-1'):
    assert request(prefix+suffix)[0]==400
   assert request(prefix+'/offset',method='POST')[0]==405
   assert request(prefix+'/bogus')[0]==404
   print('BOUNDARIES PASS',prefix,flush=True)
assert request('/taxi/../clean/offset')[0]==404
print('ALL AXUM EDGE CHECKS PASS',flush=True)
