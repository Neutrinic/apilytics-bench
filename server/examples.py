"""Write apilytics configs for every fixture, page size and pagination style into configs/."""
import os
from pathlib import Path
root=Path(__file__).resolve().parent/'configs';root.mkdir(exist_ok=True)
public=os.environ.get('SYNTHETIC_PUBLIC_BASE','http://127.0.0.1:18600').rstrip('/')
for dataset in ('taxi','lineitem'):
 for size in ('100mb','2gb','10gb'):
  for variant in ('clean','messy'):
   base=f'{public}/{dataset}/{size}/{variant}'
   for limit in (500,5000):
    for style in ('offset','cursor','link'):
     pagination=('style = offset\n offset-param = offset' if style=='offset' else 'style = cursor\n cursor-param = cursor\n cursor-path = "/next"' if style=='cursor' else 'style = link_header')
     text=f'''openapi = "{base}/openapi.yaml"
auth {{ type = none }}
pagination {{
 {pagination}
 page-size-param = limit
 max-page-size = {limit}
 max-pages = 100000
}}
schema {{ flatten-depth = 2, array-handling = keep_array }}
http {{ max-retries = 5, max-backoff = 30s, timeout = 60s }}
tables {{ records {{ endpoint = "/{style}", data-path = "/{'items' if style=='cursor' else 'results'}" }} }}
'''
     (root/f'{dataset}-{size}-{variant}-{style}-{limit}.conf').write_text(text)
   # The spec describes the export as a string, with no record schema, so it's read in variant mode.
   (root/f'{dataset}-{size}-{variant}-ndjson.conf').write_text(f'''openapi = "{base}/openapi.yaml"
auth {{ type = none }}
pagination {{ style = none }}
schema {{ mode = variant }}
http {{ response-format = ndjson, timeout = 300s }}
tables {{ records {{ endpoint = "/export.ndjson" }} }}
''')
