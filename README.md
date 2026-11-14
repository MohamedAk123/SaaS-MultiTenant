# SaaS multi-tenant

Les deux jalons utilisent le dataset DoorDash ETA Prediction. [Provenance et adaptation](DATASET.md).

| Dossier | Contenu | Lancement depuis la racine |
|---|---|---|
| [`jalon-1/`](jalon-1/) | Parties 1 à 3, cluster, batch RDD et Spark SQL | `.\jalon-1\run.ps1 -Build` |
| [`jalon-2/`](jalon-2/) | Parties 4 à 6, streaming Kafka, Delta Lake, GraphFrames et GraphX | `.\jalon-2\run.ps1 -Build` |
| [`report/`](report/) | Rapport commun aux six parties | [Consulter le PDF](report/rapport-projet.pdf) |

Démarrer Docker Desktop avant de lancer un script. Chaque jalon peut préparer ses données et démarrer ses services depuis son propre dossier. Après la première construction des images, retirer `-Build`. Le jalon 2 prépare ses données sans relancer les analyses batch du jalon 1. Son option `-SkipBatch` réutilise un historique déjà préparé dans `jalon-2/data/`.

Les deux dossiers utilisent le même projet Docker Compose `saas-jalon1`, les mêmes volumes et les mêmes ports. Lancer un seul jalon à la fois. Le lancement d’un jalon rattache le cluster à son dossier. Sur un poste déjà initialisé, utiliser les mêmes identifiants MinIO dans les deux fichiers `.env` locaux pour accéder au volume existant.

- Spark Master sur `http://localhost:8080` et History Server sur `http://localhost:18080`.
- JupyterLab sur `http://localhost:8888/lab` et MinIO sur `http://localhost:9001`. Les identifiants figurent dans le `.env` du jalon actif.
- Les notebooks, résultats et preuves sont dans les sous-dossiers `notebooks`, `results` et `evidence` de chaque jalon. `jalon-1/report/rapport-jalon1.pdf` et les captures du 6 novembre documentent la première démonstration synthétique.

Pour rejouer le streaming manuellement, se placer dans `jalon-2` après la préparation. Dans deux terminaux, exécuter les commandes suivantes séparément.

```powershell
docker compose exec -T jupyter python3 src/run_streaming.py --run-id manuel-01
docker compose exec -T jupyter python3 src/producer.py --mode kafka --rate 20 --n 50000 --run-id manuel-01
```

Les preuves Kafka et Delta se trouvent dans `jalon-2/results/jalon2/`. La comparaison des graphes est dans `jalon-2/results/graph_validation.json`. Les contrôles batch sont dans `jalon-1/results/validation.json`.
