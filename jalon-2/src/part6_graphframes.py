"""Tenant-isolated RBAC demonstration over real restaurant identifiers.
Access policies/users are explicitly simulated: the source has no access logs.
"""
import json
from collections import defaultdict
from pathlib import Path
from time import perf_counter
from graphframes import GraphFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,StringType,LongType
from common import session,save,ROOT

BASE='s3a://lake/gold/graph_analysis'


def validate(vertices,edges):
    if len({v['id'] for v in vertices})!=len(vertices): raise ValueError('duplicate vertex identifier')
    lookup={v['id']:v['tenant_id'] for v in vertices}
    for e in edges:
        if e['src'] not in lookup or e['dst'] not in lookup: raise ValueError('orphan edge')
        if lookup[e['src']]!=lookup[e['dst']] or e['tenant_id']!=lookup[e['src']]: raise ValueError('cross-tenant edge')


def run():
    restaurants=[json.loads(x) for x in (ROOT/'data/restaurants.json').read_text().splitlines()]
    grouped=defaultdict(list)
    for r in restaurants: grouped['tenant-'+r['city'].split('-')[-1]].append(r)
    vertices=[];edges=[]
    def vertex(id,tenant,type,label): vertices.append(dict(id=id,tenant_id=tenant,type=type,label=label,provenance='dataset_restaurant' if type=='resource' else 'simulated_RBAC'))
    def edge(src,dst,tenant,rel): edges.append(dict(src=src,dst=dst,tenant_id=tenant,relationship=rel,provenance='simulated_RBAC'))
    for tenant,refs in sorted(grouped.items()):
        roles=['analyst','dispatcher','manager']
        for role in roles: vertex(tenant+':role:'+role,tenant,'role',role)
        for i in range(12):
            uid=tenant+':user:'+str(i);vertex(uid,tenant,'user','SIM-operator-'+str(i))
            assigned=roles if i==0 else [roles[i%3]]
            for role in assigned: edge(uid,tenant+':role:'+role,tenant,'HAS_ROLE')
        for j,ref in enumerate(sorted(refs,key=lambda r:r['restaurant_id'])):
            rid=tenant+':resource:'+ref['restaurant_id'];vertex(rid,tenant,'resource',ref['restaurant_id'])
            allowed = roles if j == 0 else sorted({'manager', roles[j % len(roles)]})
            for role in allowed:
                edge(tenant+':role:'+role,rid,tenant,'CAN_ACCESS')
            edge(tenant+':user:'+str(j%12),rid,tenant,'ACCESSED')
    vertices.sort(key=lambda x:x['id'])
    for i,v in enumerate(vertices,start=1): v['vertex_id']=i
    validate(vertices,edges)
    # Adversarial tests are excluded from the actual analysis/export.
    rejected=[]
    for name,vs,es in [('cross_tenant',vertices,edges+[dict(edges[0],dst=vertices[-1]['id'])]),
                       ('duplicate_id',vertices+[vertices[0]],edges),
                       ('orphan_edge',vertices,edges+[dict(edges[0],dst='missing')])]:
        try: validate(vs,es)
        except ValueError: rejected.append(name)
    assert len(rejected)==3
    spark=session('Jalon2-Partie6-GraphFrames')
    try:
        v=spark.createDataFrame(vertices).cache();e=spark.createDataFrame(edges).cache()
        assert v.count()==len(vertices) and e.count()==len(edges)
        graph=GraphFrame(v,e);start=perf_counter()
        degrees=v.select('id','tenant_id','vertex_id').join(graph.degrees,'id','left')\
            .join(graph.inDegrees,'id','left').join(graph.outDegrees,'id','left').fillna(0)
        degree_rows=degrees.collect();degree_seconds=perf_counter()-start
        expected=defaultdict(int)
        for r in edges: expected[r['src']]+=1;expected[r['dst']]+=1
        assert all(r.degree==expected[r.id] for r in degree_rows)
        start=perf_counter()
        motif=graph.find('(u)-[ur]->(r); (r)-[rr]->(s)').filter(
            (F.col('u.type')=='user') & (F.col('r.type')=='role') & (F.col('s.type')=='resource')
            & (F.col('ur.relationship')=='HAS_ROLE') & (F.col('rr.relationship')=='CAN_ACCESS')
            & (F.col('u.tenant_id')==F.col('r.tenant_id')) & (F.col('r.tenant_id')==F.col('s.tenant_id')))
        flat=motif.select(F.col('u.tenant_id').alias('tenant_id'),F.col('u.id').alias('user_id'),F.col('r.id').alias('role_id'),F.col('s.id').alias('resource_id'))
        motifs=flat.count();motif_seconds=perf_counter()-start
        flat.show(12,truncate=False);degrees.orderBy(F.desc('degree')).show(12,truncate=False)
        anomalies=[]
        for r in degree_rows:
            vertex_type=next(x['type'] for x in vertices if x['id']==r.id)
            roles=sum(x['relationship']=='HAS_ROLE' and x['src']==r.id for x in edges)
            access=sum(x['relationship']=='CAN_ACCESS' and x['dst']==r.id for x in edges)
            if vertex_type=='user' and roles>=3: anomalies.append({'id':r.id,'tenant_id':r.tenant_id,'rule':'roles >= 3','value':roles,'reason':'Simulated broad-role operator; review required, not evidence of fraud.'})
            if vertex_type=='resource' and access>=3: anomalies.append({'id':r.id,'tenant_id':r.tenant_id,'rule':'incoming CAN_ACCESS >= 3','value':access,'reason':'All three roles can access this resource; ordinary resources allow one or two roles.'})
        assert sum(a['rule']=='incoming CAN_ACCESS >= 3' for a in anomalies) == len(grouped)
        for label,df in [('vertices',v),('edges',e),('graphframes_degrees',degrees),('motifs',flat),('anomalies',spark.createDataFrame(anomalies))]:
            df.write.mode('overwrite').partitionBy('tenant_id').parquet(BASE+'/'+label)
            if label in ('vertices','edges'): df.write.mode('overwrite').parquet('/workspace/artifacts/graph/'+label)
        tenant_rows=[]
        for tenant in sorted(grouped):
            tenant_rows.append({'tenant_id':tenant,'vertices':sum(v['tenant_id']==tenant for v in vertices),
                'edges':sum(e['tenant_id']==tenant for e in edges),'resources':len(grouped[tenant])})
        result={'application_id':spark.sparkContext.applicationId,'vertices':len(vertices),'edges':len(edges),
            'tenants':tenant_rows,'motifs':motifs,'inter_tenant_edges':0,'negative_tests_rejected':rejected,
            'degree_seconds':degree_seconds,'motif_seconds':motif_seconds,'anomalies':anomalies,
            'source':'Kaggle restaurant identifiers; users, roles and access relations are an explicit simulation as required by part 6.',
            'thresholds':{'user_roles':3,'resource_role_access':3},'version':'0.8.4-spark3.5-s_2.12'}
        save('graphframes.json',result)
        (ROOT/'data/graph_vertices.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in vertices))
        (ROOT/'data/graph_edges.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in edges))
        print(json.dumps({k:v for k,v in result.items() if k!='anomalies'},indent=2))
    finally: spark.stop()


if __name__=='__main__': run()
