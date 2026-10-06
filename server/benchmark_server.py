"""Server throughput check: concurrent clients fetching the same page; does not submit Spark jobs.
Run it from another machine to measure the network, or on the server to measure its own ceiling."""
import argparse,asyncio,time,json
from pathlib import Path
import aiohttp

async def run(encoding,base,seconds=8,concurrency=16):
    start=time.monotonic();end=start+seconds;counts=[0,0]
    async with aiohttp.ClientSession(auto_decompress=False,headers={'Accept-Encoding':encoding},connector=aiohttp.TCPConnector(limit=concurrency)) as session:
        async def worker():
            while time.monotonic()<end:
                async with session.get(base+'/taxi/offset?limit=5000') as r:
                    assert r.status==200
                    body=await r.read();counts[0]+=1;counts[1]+=len(body)
        await asyncio.gather(*(worker() for _ in range(concurrency)))
    elapsed=time.monotonic()-start
    return {'encoding':encoding,'concurrency':concurrency,'seconds':round(elapsed,2),'requests':counts[0],'response_body_bytes':counts[1],'wire_MB_per_second':round(counts[1]/elapsed/1e6,2),'body_Gbps':round(counts[1]*8/elapsed/1e9,3)}
async def main():
    p=argparse.ArgumentParser();p.add_argument('--base',default='http://127.0.0.1:18600');p.add_argument('--seconds',type=float,default=8);p.add_argument('--concurrency',type=int,default=16);p.add_argument('--repeats',type=int,default=1);p.add_argument('--output',default='server-throughput.json');a=p.parse_args()
    result=[]
    for repeat in range(a.repeats):
        for encoding in ('identity','gzip'):
            r=await run(encoding,a.base,a.seconds,a.concurrency);r.update(base=a.base,repeat=repeat+1);result.append(r);print(json.dumps(r),flush=True)
    Path(a.output).write_text(json.dumps(result,indent=2))
asyncio.run(main())
