# SaaS MultiTenant - Projet Big Data, jalon 1

Parties 1, 2 et 3 exécutées sur un cluster Docker standalone le **6 novembre 2026 (UTC)**.
Le notebook a été exécuté entièrement ; la validation indépendante est `PASS`.

Le [rapport PDF](report/rapport-jalon1.pdf) contient les cinq captures Spark actualisées. Sa [version Markdown](report/rapport-jalon1.md) est directement consultable sur GitHub.

## Cloner et lancer le projet

Prérequis : Git, Docker Desktop démarré avec des conteneurs Linux et PowerShell. Python, Spark et Jupyter sont fournis par les images Docker.

```powershell
git clone https://github.com/MohamedAk123/SaaS-MultiTenant.git
cd SaaS-MultiTenant
.\run.ps1 -Build
```

Le script construit les images, démarre les services, génère les données et exécute les parties 1, 2 et 3, puis leur validation. La première construction peut prendre plusieurs minutes. Les 50 000 événements sont régénérés localement ; le dépôt conserve le référentiel synthétique et le manifeste de l'exécution de référence.

## Démarrer sur ce poste

Depuis ce dossier dans PowerShell :

```powershell
# Vérifier les services du projet
docker compose ps

# Relancer les traitements avec les images déjà construites
./run.ps1

# Sur une autre machine, construire les images puis tout exécuter
./run.ps1 -Build
```

`run.ps1` crée un `.env` local avec un mot de passe MinIO et un jeton Jupyter aléatoires s'il manque.
Les identifiants sont dans ce fichier ; il est exclu de Git. `.env.example` contient uniquement des valeurs de remplacement.
Les interfaces sont limitées à `127.0.0.1`. Ce déploiement est un laboratoire local.

| Interface | Adresse | Accès |
|---|---|---|
| Spark Master | http://localhost:8080 | Deux Workers, quatre cœurs au total |
| JupyterLab | http://localhost:8888/lab | Champ `JUPYTER_TOKEN` du `.env` |
| MinIO Console | http://localhost:9001 | Champs `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Spark History | http://localhost:18080 | DAG, stages et SQL après la fin des applications |
| Workers | http://localhost:8081 et http://localhost:8082 | État des Workers |
| Spark UI active | http://localhost:4040 | Seulement pendant une application |

Dans Jupyter, ouvrir **`notebooks/jalon1.ipynb`**. Les cellules conservent leurs sorties validées.
Pour refaire la démonstration : Kernel > Restart Kernel and Run All Cells. Ne pas lancer simultanément le notebook et `run.ps1` : ils partagent les mêmes chemins de résultats.

Pour arrêter puis redémarrer sans effacer les volumes :

```powershell
docker compose stop
docker compose start
```

Le projet utilise les noms Compose `saas-jalon1-*`. Les anciens conteneurs Spark/PostgreSQL ne sont pas modifiés. Ne pas démarrer l'ancien Master en même temps : il utilise aussi 7077 et 8080.

## Organisation

```text
docker-compose.yml       cluster et volumes persistants
docker/                  images Spark/Jupyter et MinIO
src/producer.py          générateur synthétique, mode fichier du jalon 1
src/common.py            SparkSession, schémas et fonctions partagées
src/bootstrap.py         topic orders, bucket lake, chargement Bronze
src/part1_cluster.py     contrôle du cluster et action distribuée
src/part2_rdd.py         RDD : CA, panier moyen, top 5, lineage
src/part3_sql.py         DataFrames, SQL, jointure, UDF, mesures, Parquet
src/validate_results.py  contrôle indépendant par Python standard
src/collect_evidence.py  jobs/stages issus des journaux, inventaire MinIO
notebooks/jalon1.ipynb   guide exécutable avec sorties
data/                   données synthétiques et manifeste SHA-256
results/                résultats et plans réels ; journaux générés localement
evidence/               captures des interfaces et preuves techniques
report/                 rapport PDF et version Markdown
tools/build_report.py   régénération du rapport (reportlab requis)
```

## Versions et ressources

- Spark **3.5.6**, Scala **2.12**, Hadoop **3.3.4**, Python **3.8.10**, dans l'image officielle `apache/spark:3.5.6-python3` utilisée par tous les composants Spark et Jupyter.
- JupyterLab **4.3.6**, boto3 **1.37.21**, kafka-python **2.0.2** ; liste résolue dans `evidence/python-freeze.txt`.
- Kafka **3.9.1**, mode KRaft, un broker/controller et topic `orders` à trois partitions, facteur de réplication 1.
- MinIO construit depuis le tag officiel **RELEASE.2025-04-22T22-12-26Z**, commit **0d7408fc9969caf07de6a8c3a84f9fbb10a6739e**. Les anciennes images officielles testées n'étaient plus disponibles. Le Dockerfile reconstruit le serveur et sa console ; sa commande `--version` peut indiquer une compilation de développement, la source exacte est celle du tag documenté.
- Connecteur `hadoop-aws:3.3.4` et SDK AWS `1.12.262`, alignés sur Hadoop embarqué.
- Deux Workers : 2 cœurs et 1536 MiB annoncés chacun ; chaque Executor reçoit 2 cœurs et 1 GiB. Docker Desktop disposait d'environ 7,6 GiB de RAM. La première construction télécharge plusieurs dépendances et compile MinIO.

Les versions sont choisies pour reproduire le cahier des charges Spark 3.5, pas comme une recommandation pour un service exposé en production.

## Données et règles de calcul

Les fichiers officiels n'étant pas disponibles, les données de remplacement ont été créées avec l'accord de l'utilisateur. **Ne pas les présenter comme les données de l'enseignant.**

- `producer.py --mode file --n 50000`, graine 2026 : exactement 50 000 lignes, 49 000 couples `(order_id, status)` distincts et 1 000 doublons exacts.
- Référentiel de 200 restaurants synthétiques dans Casablanca, Fes, Marrakech, Rabat et Tanger.
- 2 480 des 49 000 événements originaux sont retardés (5,06 %), au plus de 900 secondes. L'ordre des lignes simule l'arrivée ; `event_time` conserve le temps métier. Le manifeste indique la provenance et les empreintes des fichiers.
- Les états d'une commande suivent CREATED puis CANCELLED, ou CREATED/PICKED_UP/DELIVERED. L'historique fini peut contenir des commandes encore en cours.
- Le CA est la somme de `amount_mad` pour **les événements DELIVERED, doublons inclus**, conformément à la séquence pédagogique des parties 2/3. Ce n'est donc pas encore un CA dédoublonné de production.
- Les montants sont calculés en centimes entiers dans les RDD et en `DecimalType(12,2)` dans les DataFrames. Les arrondis d'affichage n'affectent pas les comparaisons.
- Silver conserve les doublons pour ces mêmes raisons ; contrôle des champs requis, valeurs positives, statut valide et enrichissement du référentiel. Le dédoublonnage streaming est prévu en partie 5.

## Résultats vérifiés

15 259 événements DELIVERED ; somme analytique **2 852 691,92 MAD**. Égalité exacte RDD = DataFrame = SQL = contrôle Python indépendant.

| Ville | CA événementiel (MAD) | Événements livrés | Panier moyen (MAD) |
|---|---:|---:|---:|
| Casablanca | 560 360,22 | 2 997 | 186,97 |
| Fes | 577 256,65 | 3 100 | 186,21 |
| Marrakech | 580 136,75 | 3 106 | 186,78 |
| Rabat | 572 186,57 | 3 064 | 186,74 |
| Tanger | 562 751,73 | 2 992 | 188,09 |

Top 5 : R-032 (101), R-070 (97), R-059 (95), R-187 (95), R-097 (94).

Silver contient 50 000 lignes relues, réparties dans cinq répertoires `city=...`. Le plan de jointure contient `BroadcastHashJoin` : diffusion explicitement demandée pour le référentiel de 200 lignes. Le contrôle indépendant vérifie aussi le CA par catégorie, les effectifs, le panier moyen et le top 5.

| Calcul | Médiane de 3 actions après chauffe |
|---|---:|
| RDD - CA par ville | 0,601 s |
| DataFrame - même CA | 0,524 s |
| SQL - même CA | 0,428 s |
| UDF Python - classes de distance | 0,535 s |
| Expression native - mêmes classes | 0,487 s |

Ce sont les mesures de l'exécution `app-20261106120913-0009`, reprises dans le rapport actualisé ; une relance les actualise dans `results/`. Elles incluent l'action et la lecture du JSON, sans `cache()` explicite, après une chauffe et avec ordre alterné. Les caches système/JVM restent possibles. Le petit volume et la machine locale limitent la portée des résultats. La première action n'est pas comparable aux mesures chaudes. Les sorties du notebook correspondent à son exécution initiale, avec les mêmes données ; ses durées peuvent donc différer.

## Preuves et interprétation

- Partie 1 : `evidence/01-spark-master.png`, `evidence/02-minio-lake.jpg`, `evidence/kafka-orders.txt`, `results/part1_cluster.json`.
- Partie 2 : `evidence/04-rdd-dag.png`, `results/part2_lineage.txt`. Job 0 du CA : **2 stages**, 4 tâches chacun, shuffle de 858 octets ; script complet : **5 jobs, 8 stages terminés**.
- Partie 3 : `evidence/05-dataframe-dag.png`, `evidence/07-sql-plan.png`, `results/part3_explain_join.txt`. Avec AQE, le job final peut montrer un stage sauté dont les données ont déjà été produites ; il ne s'agit pas d'un échec. Le script complet, mesures et contrôles inclus, produit **72 jobs et 76 stages terminés**.
- Historique : `evidence/03-spark-history.png`, applications des parties 1, 2 et 3 terminées. Les cinq PNG Spark sont les captures du 6 novembre intégrées au rapport ; les captures MinIO sont conservées de l'exécution initiale.
- Stockage : `06-minio-partitions.jpg`, `08-minio-parquet.jpg`, `minio_inventory.txt`. Bronze et Silver existent ; Gold et checkpoints seront alimentés au jalon 2.
- Validation : `results/validation.json` et sorties enregistrées du notebook.

`sc.defaultParallelism=4` est une valeur de parallélisme par défaut configurée ; le total matériel annoncé est vérifié séparément via l'API du Master (4 cœurs). Les Workers hébergent les Executors propres à chaque application. Le Driver est dans le conteneur Jupyter.

## Commandes manuelles

```powershell
docker compose exec -T jupyter python3 src/producer.py --mode file --n 50000
docker compose exec -T jupyter python3 src/bootstrap.py
docker compose exec -T jupyter python3 src/part1_cluster.py
docker compose exec -T jupyter python3 src/part2_rdd.py
docker compose exec -T jupyter python3 src/part3_sql.py
docker compose exec -T jupyter python3 src/validate_results.py
docker compose exec -T jupyter python3 src/collect_evidence.py
```

Le générateur est limité au mode fichier à ce jalon. La publication Kafka et les connecteurs Structured Streaming, Delta Lake et GraphFrames seront ajoutés au jalon 2. PostgreSQL/Next.js n'interviennent pas dans les exercices batch exigés ici.

## Avant remise

Renseigner les noms du binôme dans le rapport. Relire et comprendre les interprétations pour la soutenance. Confirmer avec l'enseignant l'acceptation des données de remplacement et le périmètre du dashboard, compte tenu du passage des commandes de restaurants au graphe utilisateurs/rôles/ressources dans le sujet.

Références : [RDD et versions](https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html), [architecture Spark](https://spark.apache.org/docs/3.5.6/cluster-overview.html), [Spark SQL](https://spark.apache.org/docs/3.5.6/sql-programming-guide.html), [intégration S3A](https://downloads.apache.org/spark/docs/3.5.6/cloud-integration.html), [Kafka Docker](https://kafka.apache.org/39/getting-started/docker/), [source MinIO](https://github.com/minio/minio/tree/RELEASE.2025-04-22T22-12-26Z).
