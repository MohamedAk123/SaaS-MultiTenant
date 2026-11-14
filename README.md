# SaaS multi-tenant avec Spark, Kafka et MinIO

Projet Écosystème Big Data 2, parties 1 à 6. Le dataset Kaggle DoorDash ETA Prediction fournit les commandes de restaurant. [Provenance et adaptation du dataset](DATASET.md).

Le [rapport de neuf pages](report/rapport-projet.pdf) présente les résultats des deux jalons avec le dataset Kaggle. `report/rapport-jalon1.pdf` et les captures du 6 novembre documentent la première démonstration synthétique.

## Lancer sur Windows

Démarrer Docker Desktop en mode conteneurs Linux. Git et PowerShell doivent être disponibles.

```powershell
git clone https://github.com/MohamedAk123/SaaS-MultiTenant.git
cd SaaS-MultiTenant
.\run-jalon2.ps1 -Build
```

Avec les images déjà construites, utiliser `.\run-jalon2.ps1`. `.\run.ps1` refait le batch seul. Après un batch réussi, `.\run-jalon2.ps1 -SkipBatch` refait le streaming et les graphes. Le lancement complet comprend 120 secondes d’arrêt réel du consommateur. Le script crée un `.env` local au premier lancement du batch et conservent les volumes entre les essais.

| Interface | Adresse |
|---|---|
| Spark Master | http://localhost:8080 |
| Spark History | http://localhost:18080 |
| JupyterLab | http://localhost:8888/lab |
| MinIO | http://localhost:9001 |
| Spark en cours de calcul | http://localhost:4040 |

Consulter les variables `JUPYTER_TOKEN`, `MINIO_ROOT_USER` et `MINIO_ROOT_PASSWORD` dans le fichier local `.env`. Les notebooks `notebooks/jalon1.ipynb` et `notebooks/jalon2.ipynb` présentent les résultats enregistrés.

## Démonstration streaming manuelle

Après la préparation du batch, lancer la requête dans le premier terminal.

```powershell
docker compose exec -T jupyter python3 src/run_streaming.py --run-id manuel-01
```

Dans un deuxième terminal, lancer le producteur avec le même identifiant.

```powershell
docker compose exec -T jupyter python3 src/producer.py --mode kafka --rate 20 --n 50000 --run-id manuel-01
```

Le même identifiant reprend le checkpoint. Un nouvel identifiant crée un essai indépendant. Le producteur rejoue l’historique DoorDash. Pour arrêter les conteneurs, utiliser `docker compose stop`.

## Preuves d’exécution

- `results/validation.json` compare les résultats RDD, DataFrame et SQL à un calcul Python indépendant.
- `evidence/streaming-console.txt` contient les sorties console de plusieurs micro-batches. `results/jalon2/progress-proof.json` détaille la progression.
- `results/jalon2/latest.json`, `restart-experiment.jsonl`, `before-offsets.json` et `after-offsets.json` documentent le redémarrage. `evidence/kafka-end-offsets.txt` correspond au relevé pendant l’arrêt. `results/jalon2/kafka-end-offsets.json` donne le relevé final.
- `results/jalon2/delta-history.json` contient la sortie de `DESCRIBE HISTORY`. `evidence/lake-jalon2.txt` liste les objets MinIO. Le contrôle compare les fenêtres Gold avec une agrégation batch de Silver.
- `results/graph_validation.json` vérifie les degrés et les composantes. `evidence/graph-subgraph.svg` illustre un tenant. Les métriques pour une restitution Next.js sont dans `results/jalon2/dashboard-data.json`.
- Dans Spark History, ouvrir les applications `Jalon1-Partie2-RDD` et `Jalon1-Partie3-DataFrames-SQL`, puis choisir le menu Jobs ou SQL pour consulter les DAG. Les fichiers `results/execution_metrics.json` et `results/part3_explain_join.txt` complètent ces preuves.

## Outils et limites

Spark 3.5.6, Scala 2.12, Python 3.8.10, Hadoop 3.3.4, Kafka 3.9.1 KRaft, Delta Lake 3.3.2, GraphFrames `0.8.4-spark3.5-s_2.12` et MinIO `RELEASE.2025-04-22T22-12-26Z`. GraphX est fourni par Spark. `tools/run_graphx.sh` compile le code Scala et le soumet au cluster.

Les montants restent en USD et les marchés sont anonymisés. Le CSV v1 est téléchargé et vérifié par SHA-256. Le graphe utilise des restaurants issus de la source et des identités et droits simulés. Le taux d’annulation et les sessions sont documentés dans [DATASET.md](DATASET.md). Les offsets assurant la reprise Spark sont dans le checkpoint. `dropDuplicates(order_id, status)` garde un état non borné en l’absence d’un temps dans la clé. Le rapport détaille les garanties et les limites de cet essai local.

Renseigner les noms du binôme dans le rapport avant remise.
