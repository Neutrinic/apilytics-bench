import argparse, datetime as dt, decimal, gzip, hashlib, json, math, os, random, shutil, urllib.request
from pathlib import Path
import duckdb, orjson, pyarrow as pa, pyarrow.parquet as pq, yaml

ROOT=Path(__file__).resolve().parent
# The URL clients will use, written into each fixture's OpenAPI servers entry.
PUBLIC_BASE=os.environ.get('SYNTHETIC_PUBLIC_BASE','http://127.0.0.1:18600').rstrip('/')
TARGETS={'100mb':100_000_000,'2gb':2_000_000_000,'10gb':10_000_000_000}
RATES=('nested','arrays','null','missing','extra','numeric_string','fraction','large_id','timestamp')

def normalize(v):
    if isinstance(v,decimal.Decimal): return float(v)
    if isinstance(v,(dt.datetime,dt.date)): return v.isoformat()+('Z' if isinstance(v,dt.datetime) else 'T00:00:00Z')
    if isinstance(v,float) and not math.isfinite(v): return None
    return v

def inject(row,rng,rate):
    d=dict(row); nums=[k for k,v in d.items() if isinstance(v,(float,int))]; ints=[k for k,v in d.items() if isinstance(v,int)]
    dates=[k for k,v in d.items() if isinstance(v,str) and ('time' in k.lower() or 'date' in k.lower())]
    k=rng.choice(nums) if nums else 'benchmark_id'
    if rng.random()<rate['nested']: d['pickup']={'lat':40.7,'lon':-73.9,'zone':{'borough':'synthetic','name':'fixture'}}
    if rng.random()<rate['arrays']: d['fees']=[{'type':'synthetic','amount':1.25},{'type':'synthetic','amount':0.5}]
    if rng.random()<rate['null']: d[rng.choice(list(row))]=None
    if rng.random()<rate['missing']: d.pop(rng.choice(list(row)),None)
    if rng.random()<rate['extra']: d['unknown_extra']={'seeded':True,'value':rng.randrange(100)}
    if rng.random()<rate['numeric_string']: d[k]=str(row[k])
    if ints and rng.random()<rate['fraction']: d[rng.choice(ints)]=4.2
    if rng.random()<rate['large_id']: d['benchmark_id']=2**40+row['benchmark_id']
    if dates and rng.random()<rate['timestamp']:
        key=rng.choice(dates); original=row[key]
        d[key]=rng.choice(['','2024-13-01',original.replace('T',' '),original.rstrip('Z')])
    return d

def write_gz(path,data):
    path.parent.mkdir(parents=True,exist_ok=True)
    with gzip.GzipFile(filename=str(path),mode='wb',compresslevel=1,mtime=0) as f:f.write(data)

def schema_properties(schema):
    props={'benchmark_id':{'type':'integer'}}
    for field in schema:
        t=field.type
        props[field.name]=({'type':'string','format':'date-time'} if pa.types.is_timestamp(t) or pa.types.is_date(t)
           else {'type':'integer'} if pa.types.is_integer(t) else {'type':'number'} if pa.types.is_floating(t) or pa.types.is_decimal(t) else {'type':'string'})
        props[field.name]['nullable']=True
    props['pickup']={'type':'object','properties':{'lat':{'type':'number'},'lon':{'type':'number'},'zone':{'type':'object','properties':{'borough':{'type':'string'},'name':{'type':'string'}}}}}
    props['fees']={'type':'array','items':{'type':'object','properties':{'type':{'type':'string'},'amount':{'type':'number'}}}}
    return props

class Output:
    def __init__(self,dataset,size,variant,props,seed,rates):
        self.root=ROOT/'data'/dataset/size/variant
        if (self.root/'manifest.json').exists(): raise RuntimeError(f'Already generated: {self.root}')
        self.root.mkdir(parents=True,exist_ok=True)
        self.rows=0; self.bytes=0; self.buffer=[]; self.props=props; self.seed=seed; self.rates=rates
        self.hash=hashlib.sha256(); self.export=gzip.GzipFile(filename=str(self.root/'export.ndjson.gz'),mode='wb',compresslevel=1,mtime=0)
        self.pw=pq.ParquetWriter(self.root/'rendered.parquet',pa.schema([('benchmark_id',pa.int64()),('payload',pa.string())]),compression='zstd')
        self.pending=[]; self.block=[]; self.page_start=0
    def add(self,r,payload):
        self.rows+=1; self.bytes+=len(payload)+1; self.hash.update(payload+b'\n')
        self.block.append(payload); self.pending.append({'benchmark_id':r['benchmark_id'],'payload':payload.decode()})
        if len(self.block)==5000:self.flush()
    def flush(self):
        if not self.block:return
        self.export.write(b'\n'.join(self.block)+b'\n')
        self.pw.write_table(pa.Table.from_pylist(self.pending,schema=self.pw.schema))
        for limit in (500,5000):
            for j in range(0,len(self.block),limit):
                start=self.page_start+j; data=b','.join(self.block[j:j+limit])
                write_gz(self.root/f'offset/{limit}/{start}.json.gz',b'{"results":['+data+b']}')
                # next is appended as a static JSON string; final pages are corrected in finish().
                next_value=str(start+min(limit,len(self.block)-j))
                write_gz(self.root/f'cursor/{limit}/{start}.json.gz',b'{"items":['+data+b'],"next":"'+next_value.encode()+b'"}')
        self.page_start+=len(self.block); self.block=[]; self.pending=[]
    def finish(self):
        self.flush(); self.export.close(); self.pw.close()
        for limit in (500,5000):
            last=((self.rows-1)//limit)*limit
            path=self.root/f'cursor/{limit}/{last}.json.gz'
            payload=orjson.loads(gzip.decompress(path.read_bytes())); payload['next']=''
            write_gz(path,orjson.dumps(payload))
        spec={'openapi':'3.0.3','info':{'title':'Synthetic '+self.root.parts[-3], 'version':'1'},
          'servers':[{'url':PUBLIC_BASE+'/'+'/'.join(self.root.parts[-3:])}], 'paths':{}}
        item={'type':'object','properties':self.props,'additionalProperties':True}
        for style in ('offset','cursor','link'):
            response={'type':'object','properties':{('items' if style=='cursor' else 'results'):{'type':'array','items':item}}}
            if style=='cursor':response['properties']['next']={'type':'string'}
            params=[{'name':k,'in':'query','schema':{'type':t}} for k,t in [('limit','integer'),('cursor' if style=='cursor' else 'offset','string' if style=='cursor' else 'integer')]]
            spec['paths']['/'+style]={'get':{'operationId':style,'parameters':params,'responses':{'200':{'description':'Page','content':{'application/json':{'schema':response}}}}}}
        spec['paths']['/export.ndjson']={'get':{'operationId':'export_ndjson','responses':{'200':{'description':'NDJSON stream; one JSON object per line','content':{'application/x-ndjson':{'schema':{'type':'string'}}}}}}}
        (self.root/'openapi.yaml').write_text(yaml.safe_dump(spec,sort_keys=False))
        con=duckdb.connect(); con.execute('SET threads=2'); con.execute("SET memory_limit='6GB'");con.execute('SET preserve_insertion_order=false')
        path=str(self.root/'rendered.parquet').replace("'","''")
        # Extract the complete property list once per JSON record, rather than
        # reparsing a large payload for every aggregate column.
        paths=','.join("'$.\""+k+"\"'" for k in self.props)
        con.execute(f"CREATE VIEW records AS SELECT payload, json_extract(payload,[{paths}]) AS fields FROM read_parquet('{path}')")
        expressions=[]; descriptors=[]
        def agg(group,key,labels,exprs):
            expressions.extend(exprs);descriptors.append((group,key,labels))
        stats={}; missing={}; kinds={}; sums={}; stamps={}; distinct={}
        for index,(k,p) in enumerate(self.props.items(),1):
            q=f'fields[{index}]'; text=f"json_extract_string({q}, '$')"
            agg('null',k,['count'],[f"count(*) FILTER(WHERE {q} IS NULL OR {q}='null')"])
            agg('missing',k,['count'],[f'count(*) FILTER(WHERE {q} IS NULL)'])
            if p['type'] in ('integer','number'):
                val=f'try_cast({text} AS DECIMAL(38,10))'
                agg('numeric',k,['sum','min','max'],[f'sum({val})',f'min({val})',f'max({val})'])
            if p.get('format')=='date-time':
                val=f'try_cast({text} AS TIMESTAMP)'
                agg('timestamps',k,['min','max','invalid_count'],[f'min({val})',f'max({val})',f"count(*) FILTER(WHERE {q} IS NOT NULL AND {q}!='null' AND {val} IS NULL)"])
            if k=='benchmark_id' or 'key' in k.lower() or k.lower().endswith('id'):
                agg('distinct',k,['count'],[f'count(DISTINCT {text})'])
        print('ORACLE',self.root,flush=True)
        values=iter(con.execute('SELECT '+','.join(expressions)+' FROM records').fetchone())
        for group,key,labels in descriptors:
            row=[next(values) for _ in labels]
            if group=='null':stats[key]=row[0]
            elif group=='missing':missing[key]=row[0]
            elif group=='distinct':distinct[key]=row[0]
            elif group=='numeric':sums[key]={k:str(v) if v is not None else None for k,v in zip(labels,row)}
            else:stamps[key]={'min':str(row[0]),'max':str(row[1]),'invalid_count':row[2]}
        manifest={'dataset':self.root.parts[-3],'size':self.root.parts[-2],'variant':self.root.parts[-1],'row_count':self.rows,
          'json_bytes_ndjson':self.bytes,'ndjson_sha256':self.hash.hexdigest(),'seed':self.seed,'rates':self.rates,
          'null_count_including_missing':stats,'missing_count':missing,'json_type_counts':kinds,'numeric':sums,'timestamps':stamps,
          'distinct_ids':distinct,'page_sizes':[500,5000],'oracle':'rendered.parquet payload column; numeric strings parsed as DECIMAL(38,10), null/invalid excluded; timestamps naive UTC; IDs distinct by textual value',
          'generator_versions':{'duckdb':duckdb.__version__,'pyarrow':pa.__version__}}
        tmp=self.root/'manifest.json.tmp';tmp.write_bytes(orjson.dumps(manifest,option=orjson.OPT_INDENT_2));tmp.rename(self.root/'manifest.json')
        print('READY',self.root,self.rows,self.bytes,flush=True)

def sources(dataset):
    source=ROOT/'sources';source.mkdir(exist_ok=True)
    if dataset=='taxi':
        for month in range(1,13):
            file=source/f'yellow_tripdata_2024-{month:02}.parquet'
            url=f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file.name}'
            if not file.exists():
                print('DOWNLOAD',url,flush=True); tmp=file.with_suffix('.download');urllib.request.urlretrieve(url,tmp);tmp.rename(file)
            yield file
    else:
        file=source/'lineitem-sf5.parquet'
        if not file.exists():
            print('DBGEN sf=5',flush=True); con=duckdb.connect(str(source/'dbgen.duckdb'));con.execute("SET memory_limit='4GB'");con.execute('SET threads=4')
            con.execute('INSTALL tpch');con.execute('LOAD tpch');con.execute('CALL dbgen(sf=5)')
            con.execute(f"COPY lineitem TO '{file}' (FORMAT PARQUET, COMPRESSION ZSTD)");con.close()
        yield file

def generate(dataset,args):
    rates={k:args.rate for k in RATES};rates.update(json.loads(args.rates))
    if set(rates)!=set(RATES) or any(not 0<=v<=1 for v in rates.values()):raise ValueError('Unknown injection category or rate outside [0,1]')
    rng=random.Random(args.seed);outputs=None; rowid=0
    for source in sources(dataset):
        pf=pq.ParquetFile(source)
        if outputs is None:
            props=schema_properties(pf.schema_arrow)
            outputs=[Output(dataset,size,var,props,args.seed,{} if var=='clean' else rates) for size in args.sizes for var in ('clean','messy') if not (ROOT/'data'/dataset/size/var/'manifest.json').exists()]
            if not outputs:return
        for batch in pf.iter_batches(batch_size=10000):
            for row in batch.to_pylist():
                row={k:normalize(v) for k,v in row.items()};row['benchmark_id']=rowid; rowid+=1
                clean=orjson.dumps(row); messy=orjson.dumps(inject(row,rng,rates))
                completed=[]
                for output in outputs:
                    payload=clean if output.root.name=='clean' else messy;output.add(row,payload)
                    if output.bytes>=TARGETS[output.root.parent.name]:completed.append(output)
                for output in completed:output.finish();outputs.remove(output)
                if not outputs:return
    if outputs:raise RuntimeError('Source exhausted before target; increase sf or source months')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('dataset',choices=['taxi','lineitem']);p.add_argument('--sizes',nargs='+',choices=list(TARGETS),default=list(TARGETS));p.add_argument('--seed',type=int,default=20261004);p.add_argument('--rate',type=float,default=.02);p.add_argument('--rates',default='{}');args=p.parse_args()
    if not 0<=args.rate<=1:raise ValueError('rate must be between zero and one')
    generate(args.dataset,args)
