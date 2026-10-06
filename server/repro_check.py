"""Regenerate the 100MB fixtures independently and verify seeded payload hashes."""
import argparse,json,shutil,tempfile
from pathlib import Path
import generate as g

original=g.ROOT
for dataset,source in [('taxi','yellow_tripdata_2024-01.parquet'),('lineitem','lineitem-sf5.parquet')]:
    tmp=Path(tempfile.mkdtemp(prefix='regencheck-',dir=original))
    try:
        g.ROOT=tmp
        g.sources=lambda _,file=original/'sources'/source:iter([file])
        args=argparse.Namespace(seed=20261004,rate=.02,rates='{}',sizes=['100mb'])
        g.generate(dataset,args)
        for variant in ('clean','messy'):
            before=json.loads((original/'data'/dataset/'100mb'/variant/'manifest.json').read_text())
            after=json.loads((tmp/'data'/dataset/'100mb'/variant/'manifest.json').read_text())
            assert before==after,(dataset,variant)
            print('REPRODUCIBLE',dataset,variant,after['ndjson_sha256'],flush=True)
    finally:
        assert tmp.parent==original and tmp.name.startswith('regencheck-')
        shutil.rmtree(tmp)
g.ROOT=original
