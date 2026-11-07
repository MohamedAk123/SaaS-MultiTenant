"""Build the milestone report from actual execution results and screenshots.

Usage: python tools/build_report.py (requires reportlab)
"""
import json
from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
    TableStyle, Image, PageBreak, KeepTogether, Preformatted)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'report'
OUT.mkdir(exist_ok=True)
def read(name):
    return json.loads((ROOT / 'results' / name).read_text(encoding='utf-8'))
p1, p2, p3 = read('part1_cluster.json'), read('part2_rdd.json'), read('part3_sql.json')
validation, metrics = read('validation.json'), read('execution_metrics.json')
manifest = json.loads((ROOT / 'data/generation_manifest.json').read_text())
navy, teal, grey = colors.HexColor('#142B46'), colors.HexColor('#007F86'), colors.HexColor('#536479')
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name='TitleLab', fontName='Helvetica-Bold', fontSize=20, leading=24, textColor=navy, spaceAfter=12))
styles.add(ParagraphStyle(name='SectionLab', fontName='Helvetica-Bold', fontSize=12, leading=15, textColor=teal, spaceBefore=10, spaceAfter=6))
styles.add(ParagraphStyle(name='BodyLab', fontName='Helvetica', fontSize=9, leading=12.5, spaceAfter=7, textColor=navy))
styles.add(ParagraphStyle(name='SmallLab', fontName='Helvetica', fontSize=7.4, leading=9.5, spaceAfter=5, textColor=grey))
styles.add(ParagraphStyle(name='CodeLab', fontName='Courier', fontSize=7.1, leading=9, spaceAfter=7, textColor=navy, backColor=colors.HexColor('#F1F5F8'), borderPadding=7))
story, md = [], []
def title(t):
    story.append(Paragraph(escape(t), styles['TitleLab'])); md.append('# '+t+'\n')
def h(t):
    story.append(Paragraph(escape(t), styles['SectionLab'])); md.append('## '+t+'\n')
def para(t, small=False):
    story.append(Paragraph(t, styles['SmallLab' if small else 'BodyLab'])); md.append(t.replace('<b>','**').replace('</b>','**').replace('<br/>','\n')+'\n')
def table(headers, rows, widths=None):
    allrows = [[Paragraph(escape(str(v)), styles['SmallLab']) for v in r] for r in [headers]+rows]
    t = Table(allrows, colWidths=widths, repeatRows=1, hAlign='LEFT')
    t.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#E1EFF1')),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#F5F7FA')]),
        ('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),7),
        ('RIGHTPADDING',(0,0),(-1,-1),7),('TOPPADDING',(0,0),(-1,-1),5),
        ('BOTTOMPADDING',(0,0),(-1,-1),4),('LINEBELOW',(0,0),(-1,0),.7,teal)]))
    story.extend([t,Spacer(1,7)])
    md.append('| '+' | '.join(headers)+' |\n| '+' | '.join(['---']*len(headers))+' |\n'+'\n'.join('| '+' | '.join(map(str,r))+' |' for r in rows)+'\n')
def picture(name, caption, max_height=320):
    path = ROOT/'evidence'/name
    w,hgt = ImageReader(str(path)).getSize()
    scale=min(499/w,max_height/hgt)
    story.append(Image(str(path),width=w*scale,height=hgt*scale,hAlign='LEFT'))
    para(caption,True); md.append(f'![{caption}](../evidence/{name})\n')
def code(t):
    story.append(Preformatted(t,styles['CodeLab'])); md.append('```text\n'+t+'\n```\n')
def page():
    story.append(PageBreak()); md.append('\n---\n')
def money(x):
    return f'{x:,.2f}'.replace(',',' ').replace('.',',')

title('Jalon 1 | Cluster et analyses batch')
para('Écosystème Big Data 2 - EMSI - 2026/2027<br/><b>Binôme : noms à renseigner.</b> Exécution vérifiée le 6 novembre 2026 (UTC).',True)
h('Partie 1 - Déploiement et positionnement')
para('Docker Compose déploie Kafka en KRaft, un Spark Master, deux Workers, MinIO et Jupyter. Un History Server conserve les interfaces des applications terminées. Spark 3.5.6, Scala 2.12 et Python 3.8.10 sont identiques côté Driver et Executors ; Hadoop 3.3.4 fournit le connecteur S3A. MinIO est reconstruit depuis son tag officiel documenté dans le README.')
table(['Élément','Rôle dans ce déploiement'],[
    ['Driver','Programme Python lancé dans Jupyter ; construit le plan et coordonne les tâches.'],
    ['SparkContext','Point de connexion de l’application au cluster ; porte les RDD et la planification.'],
    ['Cluster Manager','Le Master standalone alloue les ressources ; les deux Workers lancent les Executors.'],
    ['Executors','Processus propres à chaque application ; exécutent les tâches, échangent les partitions et stockent les données persistées.'],
], [95,404])
para('<b>Deux limites de MapReduce.</b> (1) Les chaînes de jobs matérialisent souvent leurs intermédiaires sur disque : Spark permet leur réutilisation en mémoire, utile pour les analyses successives et les algorithmes de graphe. (2) Le modèle batch et les coûts de lancement conviennent mal aux indicateurs quasi temps réel : Structured Streaming fournira les micro-batches Kafka du jalon 2. Spark conserve néanmoins des E/S et des shuffles ; la mémoire ne supprime pas tous les coûts.')
para(f'<b>Preuves :</b> {p1["alive_workers"]} Workers actifs, {p1["worker_cores"]} cœurs annoncés, topic orders à 3 partitions et bucket lake. La session {p1["app_name"]} utilise spark://spark-master:7077. La somme distribuée 0..999 vaut {p1["test_sum"]}. sc.defaultParallelism={p1["default_parallelism"]} désigne les partitions par défaut, pas une mesure matérielle.')
picture('01-spark-master.png','Figure 1 - Capture fournie : deux Workers ALIVE, quatre cœurs disponibles et applications terminées.',230)
para('Référence : documentation Apache Spark 3.5.6, Cluster Mode Overview (lien dans le README).',True)

page(); title('Partie 2 | Analyse en RDD')
h('Données et sémantique des indicateurs')
para(f'Le générateur de remplacement, autorisé par l’utilisateur, produit 50 000 événements et 200 restaurants (graine 2026). Ce ne sont pas les fichiers officiels du cours. Il injecte 1 000 doublons exacts et {manifest["late_original_events"]} événements originaux retardés, soit 5,06 %, avec un retard maximal de 15 minutes. Les fichiers sont copiés sous lake/bronze/history/ et lake/bronze/reference/.')
para('Le filtre conserve uniquement DELIVERED. Les montants sont additionnés en centimes entiers ; le panier moyen résulte du couple (somme, nombre) agrégé par reduceByKey. Les doublons restent présents dans les parties 2/3 pour respecter la progression vers le dédoublonnage de la partie 5. Les chiffres ci-dessous sont donc des <b>indicateurs événementiels</b>.')
table(['Ville','CA (MAD)','Livraisons','Panier moyen (MAD)'],[[r['city'],money(r['revenue_mad']),r['delivered_events'],money(r['average_mad'])] for r in p2['cities']],[120,125,95,159])
para(f'<b>Total :</b> {validation["delivered_events"]:,} événements DELIVERED, soit {money(validation["revenue_mad"])} MAD.'.replace(',', ' ',1))
h('Top 5 des restaurants')
table(['Restaurant','Événements livrés'],[[r['restaurant_id'],r['delivered_events']] for r in p2['top5_restaurants']],[250,249])
para('takeOrdered trie par nombre décroissant puis identifiant croissant pour départager les ex aequo de façon reproductible.')
h('Transformations, actions et tolérance aux pannes')
para('<b>Transformations différées :</b> textFile définit la source, puis map(json.loads), filter, map et reduceByKey construisent le lineage. <b>Actions :</b> collect, count et takeOrdered déclenchent les jobs. toDebugString décrit les dépendances sans calculer les résultats.')
para('reduceByKey regroupe les mêmes villes entre partitions : la redistribution provoque un shuffle. Une combinaison locale réduit les données transférées avant l’agrégation finale. Le lineage mémorise la suite des transformations ; une partition perdue peut être recalculée depuis ses dépendances, tant que les sources sont encore accessibles. Ce mécanisme ne rend pas le Master unique hautement disponible.')
code('textFile -> map(parse JSON) -> filter(DELIVERED)\n         -> map(city, (centimes, 1))\n         -> reduceByKey(somme des couples) -> collect')

page(); title('Plans et exécution distribuée')
h('DAG RDD réellement observé')
picture('04-rdd-dag.png','Figure 2 - Capture fournie du Job 0 RDD : deux stages et quatre tâches par stage.',260)
para(f'Le job 0 comporte 2 stages : lecture/transformations et écriture du shuffle, puis agrégation/collecte. L’interface indique 858 octets écrits et relus par le shuffle. Le script de la partie 2 totalise {metrics["part2"]["number_of_jobs"]} jobs et {metrics["part2"]["number_of_completed_stage_attempts"]} stages terminés : CA, panier moyen, top 5 et deux comptages. Ces nombres proviennent des journaux, pas d’une estimation.')
h('DAG du premier job DataFrame')
picture('05-dataframe-dag.png','Figure 3 - Capture fournie du Job 0 DataFrame : Scan json, WholeStageCodegen et Exchange.',230)
para('Ce job comporte un stage et trois tâches, avec 1 042 octets écrits dans le shuffle. Ce nombre décrit le job affiché, pas toute la requête SQL. Le lien Associated SQL Query mène au plan complet, présenté en page 6. AQE peut réutiliser un stage déjà calculé : « skipped » ne signifie pas « failed ».')
para(f'Applications documentées par ces captures : {p2["application_id"]} (RDD) et {p3["application_id"]} (DataFrames/SQL).',True)

page(); title('Partie 3 | SQL, jointures et Parquet')
h('Schémas explicites et qualité')
para('Les JSON sont lus avec StructType et mode FAILFAST, sans inférence. Les identifiants, ville et statut sont StringType ; amount_mad est DecimalType(12,2), items_count IntegerType, distance_km DoubleType et event_time TimestampType en UTC. Le référentiel déclare identifiant, nom, ville, catégorie, latitude et longitude. Les schémas complets sont dans src/common.py.')
para('Le contrôle du référentiel vérifie 200 identifiants distincts, aucune commande orpheline, aucune multiplication de lignes et la cohérence des villes après jointure. Silver écarte les valeurs requises manquantes et les valeurs métier invalides, ajoute distance_band et conserve les doublons. Toutes les lignes synthétiques satisfont ces règles : <b>50 000 en entrée, 50 000 jointes et 50 000 relues en Parquet</b>.')
h('CA par catégorie et par ville (MAD)')
cats=['burger','marocain','pizza','sushi','tacos']
amounts={(r['city'],r['category']):float(r['revenue_mad']) for r in p3['revenue_by_category']}
table(['Ville']+cats,[[r['city']]+[money(amounts[(r['city'],cat)]) for cat in cats] for r in p2['cities']],[85,83,83,83,83,82])
para('Le CA est recalculé avec groupBy/agg puis avec createOrReplaceTempView et spark.sql. Les deux résultats sont exactement égaux aux RDD et au contrôle indépendant Python, au centime près.')
h('Stockage Silver partitionné')
para('Chemin : s3a://lake/silver/orders_history/. Format Parquet compressé Snappy, partitionnement city. Une relecture vérifie le nombre de lignes, les cinq villes et le CA. Une relance remplace uniquement cette sortie batch du projet ; les entrées Bronze et les autres conteneurs sont conservés.')
picture('06-minio-partitions.jpg','Figure 4 - Cinq partitions city=... et marqueur _SUCCESS. Capture MinIO de l’exécution initiale conservée ; mêmes données synthétiques.',255)

page(); title('Mesures, comparaison et validation')
h('Protocole et temps observés')
para('Mesures sur le même JSON dans MinIO, quatre cœurs d’Executors, sans cache() explicite. Une chauffe précède trois actions chronométrées ; l’ordre est décalé entre répétitions. Les caches système/JVM/S3 peuvent rester chauds. Les coûts initiaux de démarrage ne sont pas confondus avec ces mesures. Le faible volume et le cluster local ne permettent pas une extrapolation industrielle.')
table(['Calcul','3 mesures (secondes)','Médiane (s)'],[[name, ' / '.join(f'{v:.3f}' for v in b['seconds']),f'{b["median_seconds"]:.3f}'] for name,b in p3['benchmarks'].items()],[115,275,109])
ranking = sorted(('RDD','DataFrame','SQL'), key=lambda name:p3['benchmarks'][name]['median_seconds'])
para(f'Pour cette exécution ({p3["application_id"]}), l’ordre des médianes croissantes est : {", ".join(ranking)}. DataFrame et SQL partagent le moteur Spark SQL ; ce classement local ne garantit pas un ordre universel. La première action (RDD : {p2["first_revenue_action_seconds"]:.3f} s ; DataFrame : {p3["first_dataframe_action_seconds"]:.3f} s) inclut des coûts initiaux différents et ne constitue pas une comparaison équitable des API.')
h('RDD, DataFrame et Dataset')
table(['API','Typage et optimisation','Utilisation ici'],[
    ['RDD','Objets Python dynamiques ; contrôle explicite des transformations ; pas d’optimisation relationnelle Catalyst.','Parsing, filter, reduceByKey et lineage.'],
    ['DataFrame / SQL','Colonnes avec schéma ; Catalyst optimise le plan. Tungsten et génération de code optimisent l’exécution JVM.','Jointures, agrégations, Parquet ; mesures ci-dessus.'],
    ['Dataset[T]','API typée Scala/Java avec encodeurs, utilisant le moteur SQL ; API Dataset typée absente de PySpark.','Comparaison théorique : aucun temps Dataset inventé.'],
],[95,265,139])
h('UDF Python et expression native')
para('Règles vérifiées : court si distance &lt; 2 km ; moyen de 2 à 5 km inclus ; long au-delà de 5 km ; null reste null. Tests sur null, 1,99, 2, 5 et 5,01 km. Les deux méthodes donnent 6 480 courts, 10 695 moyens et 32 825 longs. La version native évite les échanges Python/JVM et reste visible par l’optimiseur. Le test agrège réellement la colonne produite : un simple count() pourrait la supprimer du plan.')
h('Validation et limites du jalon')
para(f'<b>PASS :</b> montants exacts, paniers moyens, top 5, CA par catégorie, classes de distance, intégrité du référentiel et relecture Parquet. Les scripts/notebook et captures accompagnent ce rapport. L’exécution complète de la partie 3 comprend {metrics["part3"]["number_of_jobs"]} jobs et {metrics["part3"]["number_of_completed_stage_attempts"]} stages terminés, mesures et contrôles inclus. Aucune mesure de panne n’est revendiquée au jalon 1. Kafka streaming, Gold, Delta Lake et graphes restent au jalon 2.')
para('Références : cahier des charges fourni, sections 3.1 à 3.3 ; documentation Apache Spark 3.5.6 (RDD Programming Guide, Cluster Mode Overview, Spark SQL Guide et Cloud Integration). Liens et versions détaillés dans README.md. Les captures proviennent du cluster exécuté le 06/11/2026.',True)

page(); title('Historique et plan SQL complet')
para('Les captures fournies montrent la dernière série terminée : parties 1, 2 et 3 aux identifiants se terminant par 0007, 0008 et 0009. Le plan ci-dessous concerne le chiffre d’affaires par ville ; la jointure avec les restaurants est documentée séparément par explain(True).')
sql_path = ROOT/'evidence/07-sql-plan.png'
history_path = ROOT/'evidence/03-spark-history.png'
sql_width, sql_height = ImageReader(str(sql_path)).getSize()
history_width, history_height = ImageReader(str(history_path)).getSize()
sql_image = Image(str(sql_path), width=148, height=148*sql_height/sql_width, hAlign='LEFT')
left = [sql_image, Spacer(1,6), Paragraph('Figure 5 - Plan SQL fourni. Source de faible résolution ; les opérateurs sont expliqués à droite.',styles['SmallLab'])]
right = [
    Image(str(history_path),width=329,height=329*history_height/history_width,hAlign='LEFT'),
    Paragraph('Figure 6 - History Server fourni : les dernières applications sont terminées.',styles['SmallLab']),
    Paragraph('Lecture du plan d’agrégation',styles['SectionLab']),
    Paragraph('<b>Scan json / Filter :</b> lecture et sélection des événements DELIVERED. <b>HashAggregate partiel :</b> calcul local des sommes. <b>Exchange :</b> redistribution des résultats par ville. <b>AQEShuffleRead :</b> lecture adaptative du shuffle. <b>HashAggregate final :</b> cinq résultats, un par ville. WholeStageCodegen regroupe des opérateurs dans du code JVM généré.',styles['BodyLab']),
    Paragraph('Lecture du plan de jointure',styles['SectionLab']),
    Paragraph('<b>Parsed / Analyzed :</b> résolution de la jointure sur restaurant_id et des types. <b>Optimized :</b> fusion des projections et filtres isnotnull sur les clés. <b>Physical :</b> BroadcastHashJoin avec BuildRight diffuse les 200 restaurants. Le broadcast est explicitement demandé et justifié par la taille vérifiée du référentiel.',styles['BodyLab']),
    Preformatted('AdaptiveSparkPlan isFinalPlan=false\n+- BroadcastHashJoin [...], Inner, BuildRight\n   :- FileScan json [orders...]\n   +- BroadcastExchange (...)\n      +- FileScan json [restaurants...]',styles['CodeLab']),
    Paragraph('Extrait abrégé du plan de jointure avant action. Sortie complète : results/part3_explain_join.txt. Les cinq nouvelles captures originales sont conservées dans evidence/ au format PNG.',styles['SmallLab']),
]
panel = Table([[left, right]],colWidths=[158,341],hAlign='LEFT')
panel.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),12),('TOPPADDING',(0,0),(-1,-1),0),('BOTTOMPADDING',(0,0),(-1,-1),0)]))
story.append(panel)
md.extend([
    '![Figure 5 - Plan SQL complet fourni](../evidence/07-sql-plan.png)\n',
    '![Figure 6 - History Server fourni](../evidence/03-spark-history.png)\n',
    'Le plan du CA suit : Scan JSON / Filter -> HashAggregate partiel -> Exchange -> AQEShuffleRead -> HashAggregate final (5 villes). WholeStageCodegen regroupe les opérateurs dans du code JVM généré.\n',
    'La capture SQL fournie est de faible résolution (192 x 716 pixels) ; les petites métriques peuvent nécessiter la consultation de Spark History.\n',
    'Pour la jointure, Parsed/Analyzed résout les colonnes et types ; Optimized fusionne les projections et ajoute isnotnull ; Physical utilise BroadcastHashJoin, BuildRight et BroadcastExchange. Voir results/part3_explain_join.txt pour la sortie complète.\n',
])

def footer(canvas, doc):
    canvas.saveState()
    canvas.setStrokeColor(teal); canvas.line(48,35,547,35)
    canvas.setFont('Helvetica',7); canvas.setFillColor(grey)
    canvas.drawString(48,23,'Jalon 1 - Parties 1, 2 et 3 | Données synthétiques | 06/11/2026 UTC')
    canvas.drawRightString(547,23,str(doc.page))
    canvas.restoreState()

doc=SimpleDocTemplate(str(OUT/'rapport-jalon1.pdf'),pagesize=A4,rightMargin=48,leftMargin=48,topMargin=36,bottomMargin=48,
    title='Jalon 1 - Cluster, RDD et Spark SQL',author='Binôme à renseigner')
doc.build(story,onFirstPage=footer,onLaterPages=footer)
(OUT/'rapport-jalon1.md').write_text('\n'.join(md),encoding='utf-8')
print(OUT/'rapport-jalon1.pdf')
