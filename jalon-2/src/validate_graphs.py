import json
from collections import Counter,defaultdict
from common import ROOT,session,save
from pyspark.sql import functions as F


def run():
    spark=session('Jalon2-Validation-Graphes')
    try:
        base='s3a://lake/gold/graph_analysis'
        gf=spark.read.parquet(base+'/graphframes_degrees')
        gx=spark.read.parquet(base+'/graphx')
        assert gf.count()==gx.count()
        columns=['tenant_id','id','vertex_id','degree']
        assert gf.select(columns).exceptAll(gx.select(columns)).count()==0
        assert gx.select(columns).exceptAll(gf.select(columns)).count()==0
        vs=[json.loads(x) for x in (ROOT/'data/graph_vertices.jsonl').read_text().splitlines()]
        es=[json.loads(x) for x in (ROOT/'data/graph_edges.jsonl').read_text().splitlines()]
        ids={v['id']:v['vertex_id'] for v in vs}; adj=defaultdict(set)
        for e in es: adj[ids[e['src']]].add(ids[e['dst']]);adj[ids[e['dst']]].add(ids[e['src']])
        components={}
        for v in vs:
            vid=v['vertex_id']
            if vid in components: continue
            pending=[vid];visited=set()
            while pending:
                x=pending.pop()
                if x in visited: continue
                visited.add(x);pending.extend(adj[x]-visited)
            for x in visited: components[x]=min(visited)
        rows=gx.collect()
        rankings = {}
        for row in sorted(rows, key=lambda row: (-row.pagerank, row.id)):
            leaders = rankings.setdefault(row.tenant_id, [])
            if len(leaders) < 5:
                leaders.append({'id': row.id, 'degree': row.degree, 'pagerank': row.pagerank})
        save('graphx-dashboard.json', rankings)
        assert all(r.component==components[r.vertex_id] for r in rows)
        assert all(r.pagerank>0 for r in rows)
        result={'status':'PASS','matched_vertices':len(rows),'degree_mismatches':0,
            'components_match_independent_DFS':True,'components':len(set(components.values())),
            'tenants':gx.select('tenant_id').distinct().count(),'pagerank_positive':True,
            'note':'Degrees compared on identical directed multigraphs; connected components independently checked as weak connectivity.'}
        save('graph_validation.json',result);print(json.dumps(result,indent=2))
    finally: spark.stop()


if __name__=='__main__': run()
