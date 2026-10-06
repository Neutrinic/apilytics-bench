"""Recover an oracle failure from closed canonical Parquet, without changing the seed."""
import argparse,gzip,hashlib,json,subprocess
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq, yaml, orjson
import generate as g

class Closed:
    def close(self):pass

def recover(dataset,variant):
    root=g.ROOT/'data'/dataset/'10gb'/variant
    if (root/'manifest.json').exists():return
    # A failed oracle has already closed Parquet/export and written OpenAPI.
    # The other variant may have had buffered rows at shutdown; recover only
    # persisted rows, then continue from the same original source position.
    spec=yaml.safe_load((root/'openapi.yaml').read_text()) if (root/'openapi.yaml').exists() else None
    previous=root/'rendered.previous.parquet'
    current=root/'rendered.parquet'
    pf=pq.ParquetFile(previous if previous.exists() else current)
    if spec:
        props=spec['paths']['/offset']['get']['responses']['200']['content']['application/json']['schema']['properties']['results']['items']['properties']
    else:
        first=next(g.sources(dataset)); props=g.schema_properties(pq.ParquetFile(first).schema_arrow)
    rates={} if variant=='clean' else {k:.02 for k in g.RATES}
    if not previous.exists():current.rename(previous)
    output=g.Output(dataset,'10gb',variant,props,20261004,rates)
    for batch in pf.iter_batches(batch_size=10000):
        payloads=[x.encode() for x in batch.column('payload').to_pylist()]
        chunk=b'\n'.join(payloads)+b'\n'
        output.export.write(chunk);output.hash.update(chunk);output.bytes+=len(chunk);output.rows+=len(payloads)
        output.pw.write_table(pa.Table.from_batches([batch]));output.page_start=output.rows
    print('RECOVERED PREFIX',dataset,variant,output.rows,output.bytes,flush=True)
    if output.bytes<g.TARGETS['10gb']:
        if variant!='clean':raise RuntimeError('Messy prefix incomplete: regeneration required')
        skip=output.rows;index=output.rows
        for source in g.sources(dataset):
            source_pf=pq.ParquetFile(source)
            if skip>=source_pf.metadata.num_rows:skip-=source_pf.metadata.num_rows;continue
            for batch in source_pf.iter_batches(batch_size=10000):
                if skip>=len(batch):skip-=len(batch);continue
                batch=batch.slice(skip);skip=0
                for row in batch.to_pylist():
                    row={k:g.normalize(v) for k,v in row.items()};row['benchmark_id']=index;index+=1
                    output.add(row,orjson.dumps(row))
                    if output.bytes>=g.TARGETS['10gb']:break
                if output.bytes>=g.TARGETS['10gb']:break
            if output.bytes>=g.TARGETS['10gb']:break
    if output.bytes<g.TARGETS['10gb']:raise RuntimeError('Source exhausted during recovery')
    output.finish()
    # Keep the previous Parquet until complete verification, rather than deleting
    # the only recovery input prematurely.

p=argparse.ArgumentParser();p.add_argument('dataset',choices=['taxi','lineitem']);args=p.parse_args()
if subprocess.run(['systemctl','is-active','--quiet',f'synthetic-rest-generate-{args.dataset}']).returncode==0:
    raise RuntimeError('Stop/wait for the original generator before recovering')
for variant in ('messy','clean'):recover(args.dataset,variant)
