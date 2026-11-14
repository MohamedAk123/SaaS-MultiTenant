"""Build the report from saved batch, streaming, graph and SaaS evidence."""
import json,math
from collections import Counter
from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet,ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,PageBreak,Preformatted
from reportlab.graphics.shapes import Drawing,Rect,String,Line,Polygon
from reportlab.graphics import renderSVG

ROOT=Path(__file__).resolve().parents[1]
BATCH=ROOT.parent/'jalon-1'
OUT=ROOT.parent/'report';OUT.mkdir(exist_ok=True)
E=ROOT/'evidence';E.mkdir(exist_ok=True)
def load(p): return json.loads((ROOT/p).read_text(encoding='utf-8'))
def load_batch(p): return json.loads((BATCH/p).read_text(encoding='utf-8'))
m=load('data/generation_manifest.json');p1=load_batch('results/part1_cluster.json');p2=load_batch('results/part2_rdd.json');p3=load_batch('results/part3_sql.json')
sv=load('results/jalon2/latest.json');metrics=load('results/jalon2/streaming-metrics.json');gf=load('results/graphframes.json');gv=load('results/graph_validation.json')
stages=load_batch('results/execution_metrics.json');history=load('results/jalon2/delta-history.json');gold=load('results/jalon2/gold-sample.json')
gx=[];times=[]
for file in (ROOT/'results/graphx_results').glob('part-*.json'): gx.extend(json.loads(x) for x in file.read_text().splitlines())
for file in (ROOT/'results/graphx_timings').glob('part-*.json'): times.extend(json.loads(x) for x in file.read_text().splitlines())
S=getSampleStyleSheet();navy=colors.HexColor('#17324d');teal=colors.HexColor('#00838f')
S.add(ParagraphStyle(name='TitleX',fontName='Helvetica-Bold',fontSize=21,leading=25,textColor=navy,spaceAfter=13))
S.add(ParagraphStyle(name='HeadX',fontName='Helvetica-Bold',fontSize=12,leading=16,textColor=teal,spaceBefore=12,spaceAfter=7))
S.add(ParagraphStyle(name='BodyX',fontName='Helvetica',fontSize=9,leading=13,spaceAfter=8))
S.add(ParagraphStyle(name='SmallX',fontName='Helvetica',fontSize=7.5,leading=10,spaceAfter=6,textColor=navy))
S.add(ParagraphStyle(name='CodeX',fontName='Courier',fontSize=7,leading=9,spaceAfter=9))
story=[];md=[]
def title(s): story.append(Paragraph(s,S['TitleX']));md.append('# '+s+'\n')
def h(s): story.append(Paragraph(s,S['HeadX']));md.append('## '+s+'\n')
def p(s,small=False): story.append(Paragraph(s,S['SmallX' if small else 'BodyX']));md.append(s+'\n')
def code(s): story.append(Preformatted(s,S['CodeX']));md.append('```text\n'+s+'\n```\n')
def table(headers,rows,widths=None):
    data=[[Paragraph(escape(str(x)),S['SmallX']) for x in row] for row in [headers]+rows]
    t=Table(data,colWidths=widths or [499/len(headers)]*len(headers),repeatRows=1,hAlign='LEFT')
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#dceef0')),('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),6),('RIGHTPADDING',(0,0),(-1,-1),6),('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),3),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f3f6fa')]),('LINEBELOW',(0,0),(-1,0),0.6,teal)]))
    story.extend([t,Spacer(1,8)])
    md.append('| '+' | '.join(map(str,headers))+' |\n| '+' | '.join(['---']*len(headers))+' |\n'+'\n'.join('| '+' | '.join(map(str,row))+' |' for row in rows)+'\n')
def page(): story.append(PageBreak())
def diagram():
    d=Drawing(499,220)
    d.add(Rect(0,0,499,220,fillColor=colors.HexColor('#f4f7fb'),strokeColor=None))
    d.add(String(12,201,'Sous-graphe tenant-1 : politique simulee, ressources Kaggle',fontName='Helvetica-Bold',fontSize=10,fillColor=navy))
    vertices=[json.loads(x) for x in (ROOT/'data/graph_vertices.jsonl').read_text().splitlines()]
    refs=[x for x in vertices if x['tenant_id']=='tenant-1' and x['type']=='resource'][:3]
    nodes={'tenant-1:user:0':(12,90,'SIM-operator-0')}
    for i,role in enumerate(['analyst','dispatcher','manager']):nodes['tenant-1:role:'+role]=(180,150-i*60,role)
    for i,r in enumerate(refs): nodes[r['id']]=(360,150-i*60,r['label'])
    edges=[json.loads(x) for x in (ROOT/'data/graph_edges.jsonl').read_text().splitlines()]
    for edge in edges:
        if edge['src'] in nodes and edge['dst'] in nodes and edge['relationship']!='ACCESSED':
            x,y,_=nodes[edge['src']];xx,yy,_=nodes[edge['dst']]
            d.add(Line(x+115,y+13,xx-4,yy+13,strokeColor=colors.HexColor('#7795a6'),strokeWidth=0.8))
            d.add(Polygon([xx-3,yy+13,xx-9,yy+16,xx-9,yy+10],fillColor=teal,strokeColor=None))
    for key,(x,y,label) in nodes.items():
        d.add(Rect(x,y,115,27,rx=4,ry=4,fillColor=colors.HexColor('#d6eaf8' if ':user:' in key else '#d5efeb' if ':role:' in key else '#fce7c2'),strokeColor=None))
        d.add(String(x+7,y+10,label,fontName='Helvetica',fontSize=8,fillColor=navy))
    d.add(String(130,16,'HAS_ROLE',fontSize=8,fillColor=teal));d.add(String(285,16,'CAN_ACCESS',fontSize=8,fillColor=teal))
    renderSVG.drawToFile(d,str(E/'graph-subgraph.svg'))
    return d

title('SaaS multi-tenant | Jalons 1 et 2')
p('Écosystème Big Data 2 - EMSI - 2026/2027<br/><b>Binôme : noms à renseigner.</b> Exécutions du 14 novembre 2026 (UTC).')
h('Source externe et périmètre')
p('Dataset <b>DoorDash ETA Prediction, Kaggle, version 1</b> : 197 428 livraisons de restaurants de 2015. La source indique avoir ajouté du bruit pour masquer des détails commerciaux. Référence et licence déclarée MIT dans DATASET.md. Aucun jeu de commandes entièrement inventé ne sert désormais de base aux résultats.')
p('Sélection : 200 restaurants, 24 500 commandes et deux états observés par commande. Les 49 000 événements reçoivent 1 000 doublons et 5 % de retards pédagogiques. <b>USD conservés</b> ; city désigne un marché anonymisé. Distances et coordonnées inconnues restent nulles. Livreurs, clients et droits SaaS sont simulés et signalés.')
h('Partie 1 - Déploiement et rôles')
table(['Composant','Rôle dans le cluster'],[['Driver / Jupyter','Construit les plans et pilote les tâches.'],['SparkContext','Point d’entrée pour RDD, planification et connexion au cluster.'],['Master standalone','Alloue les ressources ; deux Workers lancent les Executors.'],['Executors','Exécutent les partitions, les échanges et les états des requêtes.'],['Kafka / MinIO','Trois partitions orders ; stockage Bronze, Silver, Gold et checkpoints.']],[125,374])
p('Le cluster annonce <b>deux Workers et quatre cœurs</b>. defaultParallelism=4 est un réglage de parallélisme, vérifié séparément des ressources matérielles. Chaque application utilise au plus quatre cœurs ; les interfaces sont locales.')
p('<b>MapReduce :</b> les chaînes de jobs matérialisent souvent leurs intermédiaires sur disque, alors que Spark peut réutiliser des données en mémoire. Le batch et les coûts de lancement conviennent moins aux indicateurs fréquents ; les micro-batches Spark permettent ici une actualisation progressive. Spark conserve néanmoins des E/S et des shuffles.')
p('Preuves : results/part1_cluster.json ; Spark Master http://localhost:8080 ; Kafka orders à trois partitions ; bucket lake.',True)

page();title('Partie 2 | Batch RDD')
h('Chiffre d’affaires événementiel et panier moyen')
p('Filtre DELIVERED, montants additionnés en centimes entiers. Les parties 2 et 3 conservent les doublons pour comparer les API avant le dédoublonnage de la partie 5. Les valeurs ne représentent donc pas encore le CA des commandes uniques.')
table(['Marché','CA (USD)','Événements livrés','Panier (USD)'],[[r['city'],f"{r['revenue_usd']:,.2f}",r['delivered_events'],f"{r['average_usd']:.2f}"] for r in p2['cities']],[115,130,125,129])
p(f"<b>Total : {p2['delivered_events']:,} événements livrés ; {sum(r['revenue_usd'] for r in p2['cities']):,.2f} USD.</b>")
table(['Top restaurant','Événements DELIVERED'],[[r['restaurant_id'],r['delivered_events']] for r in p2['top5_restaurants']],[250,249])
h('Transformations, actions et shuffle')
code('textFile -> map(json.loads) -> filter(DELIVERED)\n         -> map(city, (centimes, 1))\n         -> reduceByKey(somme des couples) -> collect')
p('textFile, map, filter et reduceByKey construisent un calcul différé ; collect, count et takeOrdered lancent les jobs. La combinaison locale réduit les échanges avant le regroupement par ville. Le shuffle sépare les stages. Le premier job comporte deux stages ; les dépendances du lineage permettent de recalculer une partition perdue depuis une source encore disponible.')
p(f"Exécution complète : <b>{stages['part2']['number_of_jobs']} jobs et {stages['part2']['number_of_completed_stage_attempts']} stages terminés</b>. Application {p2['application_id']}. Le lineage est conservé dans results/part2_lineage.txt ; détails et DAG dans le History Server.",True)

page();title('Partie 3 | DataFrames et SQL')
h('Schéma explicite, jointure et qualité')
p('StructType impose les colonnes et TimestampType, DecimalType(12,2), IntegerType, DoubleType. Lecture FAILFAST sans inférence. La jointure stream-static ultérieure réutilise le même référentiel. Les 200 identifiants restaurant sont uniques ; aucune commande orpheline ni multiplication de lignes n’est acceptée.')
p(f"<b>{p3['input_events']:,} lignes en entrée, {p3['joined_rows']:,} après jointure et {p3['silver_rows']:,} relues en Parquet.</b> Partitionnement city, compression Snappy. Les distances nulles sont conservées comme inconnues ; les bornes 2 et 5 km sont testées séparément.")
table(['API / calcul','Trois mesures (s)','Médiane (s)'],[[k,' / '.join(f'{x:.3f}' for x in v['seconds']),f"{v['median_seconds']:.3f}"] for k,v in p3['benchmarks'].items()],[125,265,109])
p('Même entrée JSON, quatre cœurs, une chauffe et trois répétitions, ordre alterné et aucun cache() explicite. Des caches système/JVM/S3 peuvent persister. Les temps ne constituent pas un classement universel. L’action consomme la bande de distance pour éviter que Catalyst ne supprime l’UDF inutilisée.')
table(['API','Typage / optimisation'],[['RDD','Objets Python, transformations explicites ; pas de Catalyst relationnel.'],['DataFrame / SQL','Schéma, optimisation Catalyst, exécution JVM et génération de code.'],['Dataset[T]','API typée Scala/Java avec encodeurs ; pas d’équivalent typé en PySpark.']],[130,369])
h('Lecture de explain(True)')
code('Parsed / Analyzed Logical Plan : jointure et resolution des types\nOptimized Logical Plan : projections et filtres isnotnull\nPhysical Plan : BroadcastHashJoin [...], Inner, BuildRight\n               +- BroadcastExchange\n                  +- FileScan json [restaurants...]')
p('La diffusion du référentiel est explicitement demandée et justifiée par ses 200 lignes. Parsed/Analyzed résolvent les expressions ; Optimized montre les réécritures de Catalyst ; Physical décrit la stratégie exécutée. La version native when/otherwise reste visible par l’optimiseur et évite les échanges Python/JVM.')
p('RDD = DataFrame = SQL = oracle Python : montants, paniers, top 5 et catégories vérifiés. Plan complet : results/part3_explain_join.txt.',True)

page();title('Partie 4 | Micro-batches Kafka')
h('Source -> transformations -> sinks')
code('Kaggle -> producer (20 evenements/s) -> Kafka orders [3 partitions]\n  readStream(kafka) -> from_json(schema)\n      |-> console append : nouvelles lignes\n      |-> groupBy(city,status) -> console complete\n      |-> JSON brut + topic/partition/offset -> Bronze\n      |-> nettoyage + watermark + dedup + reference -> Silver')
p('Le flux est une <b>table infinie</b> : les nouvelles lignes enrichissent le résultat au fil du temps. Chaque requête possède son propre checkpoint. Bronze conserve le JSON original sous raw_json avec les métadonnées Kafka ; Silver enrichit et écrit du Parquet partitionné city/date.')
table(['Mode','Usage et condition'],[['append','Nouvelles lignes ; agrégats seulement lorsque leurs fenêtres sont finalisées.'],['update','Lignes modifiées à chaque trigger ; nécessite un sink compatible.'],['complete','Table de résultat entière à chaque trigger ; comptage ville/statut en console.']],[100,399])
p('<b>Micro-batch :</b> trigger(processingTime="5 seconds") lance des lots, avec garanties possibles exactly-once si source et sink conviennent. <b>Continu :</b> trigger(continuous=...) vise une faible latence avec garanties at-least-once et un sous-ensemble d’opérations ; les agrégations avec état de ce pipeline utilisent les micro-batches.')
table(['Requête','Lots non vides','Trigger maximal (ms)'],[[k,v['nonempty_batches'],v['max_trigger_ms']] for k,v in metrics.items() if k in ['bronze','silver','raw_console','counts_console']],[190,125,184])
h('Extrait de console et preuve des lots')
progress=load('results/jalon2/progress-proof.json')
table(['Console brute : batch','Lignes entrantes','Horodatage UTC'],[[x['batchId'],x['numInputRows'],x['timestamp']] for x in progress['raw_console'] if x['numInputRows']][:5],[140,115,244])
p('Sorties originales : evidence/streaming-console.txt et results/jalon2/progress-proof.json. Plusieurs lots successifs sont conservés, avec leurs entrées, offsets, watermarks et durées. Les captures écran de l’interface doivent être actualisées manuellement si le navigateur automatisé est indisponible.',True)

page();title('Partie 5 | Reprise et watermark')
h('Expérience d’arrêt réellement exécutée')
exp=[json.loads(x) for x in (ROOT/'results/jalon2/restart-experiment.jsonl').read_text().splitlines()]
restart=next(x for x in exp if x['phase']=='after_restart');during=next(x for x in exp if x['phase']=='producer_during_stop')
table(['Étape','Résultat observé'],[['Avant arrêt',f"{sv['before_stop']} clés distinctes en Silver ; 20 doublons écartés."],['Producteur pendant arrêt',f"{during['produced']} événements en {during['seconds']:.3f} s, soit 20/s."],['Après reprise',f"{sv['after_restart']} lignes : 600 + 2400, récupération en {restart['recovery_seconds']:.3f} s."],['Validation finale',f"Bronze {sv['bronze_rows']} ; Silver {sv['silver_rows']} (3600 métier + 2 contrôles)."],['Sonde tardive','Une clé unique à -15 minutes : présente en Bronze, absente de Silver.']],[145,354])
p('La comparaison finale porte sur <b>les ensembles de clés (order_id, status)</b> et leurs effectifs, pas seulement sur un nombre global. Les 20 doublons et la sonde tardive expliquent exactement la différence Bronze/Silver. L’arrêt est volontaire et propre ; ce test ne simule pas une panne matérielle arbitraire en milieu de commit.')
h('Offsets : checkpoint Spark et groupes Kafka')
for label in ['before','after']:
    entries=load('results/jalon2/'+label+'-offsets.json')
    last=max(entries,key=lambda x:int(x['key'].rsplit('/',1)[-1]))
    offset=last['content'].strip().splitlines()[-1]
    code(label+' : offsets/'+last['key'].rsplit('/',1)[-1]+'  '+offset)
p('La reprise conserve le même checkpoint et restaure ses offsets, indépendamment de startingOffsets. Spark ne valide pas les offsets comme un consommateur classique : kafka-consumer-groups --all-groups --describe ne fournit ici aucun groupe avec offsets validés. Le relevé de fin du topic est conservé séparément. Aucun résultat de groupe Kafka n’est inventé.')
h('Interprétation du retard')
p('Le watermark est calculé à partir du maximum event_time traité moins dix minutes, et non de l’heure du PC. Après progression de ce maximum, une sonde à -15 minutes est trop ancienne pour Silver, tandis que Bronze conserve le message. Un événement situé au-delà de la tolérance n’est pas toujours rejeté immédiatement : le watermark doit déjà avoir progressé.')
p('La clé dropDuplicates(order_id,status) respecte la consigne. Sous Spark 3.5, sans temps dans la clé, le watermark ne borne pas automatiquement cet état. Cette limite mémoire est documentée ; dropDuplicatesWithinWatermark est une alternative de production à évaluer selon la politique de répétition.',True)

page();title('Partie 5 | Fenêtres et Delta Lake')
table(['Fenêtre','Calcul / sens'],[['Tumbling 10 min','Nombre de CREATED et DELIVERED ; somme des USD uniquement pour DELIVERED.'],['Sliding 10 min / pas 5 min','CANCELLED / (DELIVERED + CANCELLED), selon le temps de l’état terminal.'],['Session, inactivité 5 min','Par tenant et courier_id ; durée observée = dernière - première activité.']],[150,349])
p(f"Résultats : <b>{sv['gold_rows']} fenêtres métier tumbling</b>, {sv['sliding_rows']} fenêtres sliding et {sv['session_rows']} sessions. Les deux événements de contrôle du watermark sont identifiés séparément ; les KPI métier affichés excluent leur tenant spécial.")
table(['Marché','Début de fenêtre','Livrées','CA USD'],[[x['city'],x['window']['start'],x['orders_delivered'],x['revenue_usd']] for x in gold if x['tenant_id']!='__control__'][:6],[105,190,75,129])
h('Historique transactionnel observé')
table(['Version','Opération','Mode / époque','Lignes ajoutées'],[[x['version'],x['operation'],x['operationParameters'].get('outputMode','')+' / '+str(x['operationParameters'].get('epochId','')),x.get('operationMetrics',{}).get('numOutputRows','')] for x in history],[65,150,140,144])
p('DESCRIBE HISTORY est relu après écriture. Le sink Delta associe les lots à des transactions ; les fichiers Parquet Silver possèdent leur propre journal de fichiers validés. Gold lit Silver comme une source de fichiers append-only, ce qui sépare le dédoublonnage et l’agrégation avec état.')
p('<b>Exactly-once :</b> source Kafka rejouable, offsets et état durables, sink natif idempotent/transactionnel. Ces propriétés supposent la conservation des messages, checkpoints et journaux, et un usage compatible des writers. La console peut réafficher des lignes après un redémarrage : elle ne fournit pas cette garantie.')
p('Les 36 fenêtres métier correspondent exactement à une agrégation batch indépendante de la table Silver. Un test distinct contenant des annulations vérifie des fenêtres partagées et deux sessions séparées par un intervalle de six minutes. Le taux nul du dataset ne décrit pas la fréquence réelle des annulations DoorDash.',True)

page();title('Partie 6 | Modèle et GraphFrames')
p('Les 200 restaurants Kaggle fournissent les ressources. La politique de droits est générée pour l’exercice : utilisateurs et rôles simulés, provenance explicite. Chaque tenant est un marché ; aucun lien non autorisé entre tenants ne peut passer le validateur.')
table(['Objet','Schéma'],[['Sommet','id, vertex_id: Long unique, tenant_id, type, label, provenance'],['Arête','src, dst, tenant_id, relationship, provenance'],['Relations','HAS_ROLE : utilisateur -> rôle ; CAN_ACCESS : rôle -> ressource ; ACCESSED : accès de démonstration.']],[90,409])
table(['Tenant','Sommets','Arêtes','Ressources'],[[x['tenant_id'],x['vertices'],x['edges'],x['resources']] for x in gf['tenants']],[130,123,123,123])
story.append(diagram());md.append('![Sous-graphe réel de la politique simulée](../jalon-2/evidence/graph-subgraph.svg)\n')
code('g.find("(u)-[ur]->(r); (r)-[rr]->(s)")\n  .filter(HAS_ROLE, CAN_ACCESS, meme tenant)')
p(f"GraphFrames : <b>{gf['vertices']} sommets, {gf['edges']} arêtes et {gf['motifs']} motifs</b>. Degrés en {gf['degree_seconds']:.3f} s ; motifs en {gf['motif_seconds']:.3f} s. Les identifiants incluent tenant et type, puis un VertexId numérique est attribué sans collision.")

page();title('Partie 6 | GraphX et contrôles')
p('Les mêmes tables de sommets/arêtes sont exportées en Parquet. GraphX construit des RDD de couples (VertexId, attribut) et des Edge, puis exécute degrees, connectedComponents et pageRank(tol=1e-6) séparément pour chaque tenant.')
table(['Tenant','Degrés (s)','Composantes (s)','PageRank (s)'],[[x['tenant_id'],f"{x['degree_seconds']:.3f}",f"{x['connected_components_seconds']:.3f}",f"{x['pagerank_seconds']:.3f}"] for x in sorted(times,key=lambda r:r['tenant_id'])],[130,110,130,129])
p(f"<b>{gv['matched_vertices']} sommets comparés ; zéro différence de degré.</b> Les composantes GraphX sont aussi comparées à un parcours Python indépendant : {gv['components']} composantes, correspondant aux six tenants. PageRank est positif sur tous les sommets.")
table(['Sommet (extrait)','Tenant','Degré','PageRank'],[[x['id'].replace('tenant-','t-'),x['tenant_id'],x['degree'],f"{x['pagerank']:.5f}"] for x in sorted(gx,key=lambda r:-r['pagerank'])[:6]],[250,85,65,99])
p('GraphFrames manipule des DataFrames et des motifs déclaratifs, avec l’optimiseur SQL. GraphX utilise Scala/RDD et des algorithmes de graphe distribués. Les degrés sont identiques sur le même graphe. Les chronométrages ont des périmètres différents : une mesure globale GraphFrames contre six graphes GraphX ; ils ne prouvent pas qu’une API est toujours plus rapide.')
h('Alertes explicites et tests négatifs')
rules=Counter(x['rule'] for x in gf['anomalies'])
table(['Règle','Cas'],[[k,v] for k,v in rules.items()]+[['Liens inter-tenant acceptés',0]],[370,129])
p('Un opérateur simulé cumulant au moins trois rôles et une ressource accessible par au moins trois rôles sont signalés pour revue. Les droits étendus sont construits par le scénario : ils ne constituent pas des fraudes réelles. Les tests négatifs rejettent séparément collision d’identifiant, arête orpheline et lien inter-tenant.')
p('Exports partitionnés par tenant dans lake/gold/graph_analysis/ : vertices, edges, motifs, anomalies, graphframes_degrees et graphx. Next.js affiche les alertes et les premiers scores PageRank du tenant connecté.',True)

page();title('Reproductibilité, preuves et limites')
h('Lancer et présenter')
code('git clone https://github.com/MohamedAk123/SaaS-MultiTenant.git\ncd SaaS-MultiTenant\n.\\run-jalon2.ps1 -Build\n# Images deja construites : .\\run-jalon2.ps1\n# Streaming seul prepare : .\\run-jalon2.ps1 -SkipBatch')
p('Notebooks : jalon1.ipynb et jalon2.ipynb. Les secrets sont générés dans .env et exclus de Git. Le téléchargement source est épinglé à la version 1 et vérifié par SHA-256 ; la sélection et les perturbations utilisent la graine 2026. Les traces détaillées de chaque run restent locales, les preuves compactes sont versionnées.')
table(['Dépendance','Version'],[['Spark / Scala / Python','3.5.6 / 2.12 / 3.8.10'],['Kafka / connecteur Spark','3.9.1 / 3.5.6'],['Delta Lake','3.3.2 (Spark 3.5)'],['GraphFrames','0.8.4-spark3.5-s_2.12, API Python du même JAR'],['MinIO','Source RELEASE.2025-04-22T22-12-26Z']],[150,349])
h('Preuves à consulter')
p('results/validation.json : batch ; results/jalon2/latest.json : streaming ; before/after-offsets.json : reprise ; delta-history.json : transactions ; progress-proof.json : micro-batches ; graph_validation.json : degrés et composantes. evidence/streaming-console.txt et lake-jalon2.txt conservent les sorties ; les liens History Server donnent accès aux DAG.')
h('Limites à défendre à l’oral')
p('L’échantillon favorise les restaurants les plus actifs ; le dataset est historique et bruité. Les marchés ne sont pas de vraies villes identifiées. Les montants sont en USD, les distances inconnues. Le flux est une relecture ; son horloge accélérée de test ne mesure pas une latence de livraison réelle. Sessions et droits reposent sur des identifiants simulés. Une seule machine, un broker et un Master ne démontrent pas une haute disponibilité de production.')
p('La reprise testée est un arrêt propre de requêtes. Les durées de trigger décrivent la latence de calcul du micro-batch, pas la latence bout en bout pour chaque message. Les captures historiques du jalon 1 ne sont pas utilisées comme preuves des nouvelles valeurs Kaggle. Les captures UI actualisées restent à réaliser manuellement si nécessaire ; les journaux, résultats et traces Spark de cette exécution sont conservés.')
h('Sources')
p('Kaggle : <link href="https://www.kaggle.com/datasets/dharun4772/doordash-eta-prediction">DoorDash ETA Prediction v1</link>. Apache Spark : <link href="https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html">Structured Streaming 3.5.6</link>, <link href="https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html">intégration Kafka</link>, <link href="https://spark.apache.org/docs/3.5.6/graphx-programming-guide.html">GraphX</link>. <link href="https://docs.delta.io/releases/">Compatibilité Delta</link> et <link href="https://docs.delta.io/delta-streaming/">sink streaming Delta</link>. <link href="https://graphframes.io/">GraphFrames</link>. Cahier des charges fourni : parties 1 à 6.',True)

page();title('Application SaaS | Tableau de bord')
code('Next.js -> transaction PostgreSQL : activity + outbox\n        -> Kafka saas-events (3 partitions)\n        -> Spark Structured Streaming -> Delta sur MinIO\n        -> vue par tenant -> API Next.js -> navigateur')
p('La session PostgreSQL associe un cookie HTTP-only à un tenant. Les routes déduisent le tenant de la session et filtrent les fenêtres de commandes, les indicateurs d’activité et les graphes côté serveur. Le tableau de bord affiche le chiffre d’affaires DoorDash, les actions, les erreurs, les accès, PageRank et les alertes de la simulation.')
p('La transaction PostgreSQL enregistre l’activité et le message à publier. Le service outbox attend la confirmation Kafka. Une répétition éventuelle utilise le même identifiant. Spark dédoublonne avec un watermark de 10 minutes, utilise le couple application/lot pour la transaction Delta et remplace la vue locale après le commit.')
h('Mesures de bout en bout')
trials=[]
for label,name in [('Premier traitement','saas-end-to-end.json'),('Après démarrage','saas-warm-run.json')]:
    proof=E/name
    if proof.exists():
        recorded=json.loads(proof.read_text(encoding='utf-8-sig'))
        trials.append([label,len(recorded['events']),f"{recorded['median_ms']/1000:.2f}",f"{recorded['max_ms']/1000:.2f}"])
table(['Essai','Événements','Médiane (s)','Maximum (s)'],trials,[200,85,107,107])
p('Temps mesuré du created_at PostgreSQL à la première réponse authentifiée de l’API qui contient l’événement, avec une interrogation chaque seconde. Le navigateur interroge l’API toutes les trois secondes et affiche aussi son propre délai. Le test vérifie les trois catégories et l’absence de fuite entre deux sessions de tenants.')
h('Lancement et preuves')
code('.\\run-dashboard.ps1\nhttp://localhost:3000')
p('Next.js 16.4.6, PostgreSQL 16 et KafkaJS 2.2.4. Les preuves API sont dans jalon-2/evidence/saas-end-to-end.json et saas-warm-run.json. mc-ls-recursive.txt contient la sortie officielle des couches Bronze, Silver et Gold. kafka-consumer-groups.txt montre la commande et le résultat du broker.')

def footer(c,doc):
    c.setStrokeColor(teal);c.line(48,35,547,35);c.setFont('Helvetica',7);c.setFillColor(navy)
    c.drawString(48,23,'SaaS multi-tenant | Kaggle DoorDash | 14/11/2026 UTC');c.drawRightString(547,23,str(doc.page))
doc=SimpleDocTemplate(str(OUT/'rapport-projet.pdf'),pagesize=A4,leftMargin=48,rightMargin=48,topMargin=38,bottomMargin=48,title='SaaS multi-tenant - Kaggle - Jalons 1 et 2',author='Binome a renseigner')
doc.build(story,onFirstPage=footer,onLaterPages=footer)
(OUT/'rapport-projet.md').write_text('\n'.join(md),encoding='utf-8')
print(OUT/'rapport-projet.pdf')
