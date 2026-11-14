# SaaS multi-tenant

Les deux jalons utilisent le dataset DoorDash ETA Prediction. [Provenance et adaptation](DATASET.md).

## Ouvrir le tableau de bord Next.js

Démarrer Docker Desktop, puis lancer depuis la racine du dépôt. Ajouter `-Build` sur un poste qui ne possède pas encore les images Spark et MinIO.

```powershell
.\run-dashboard.ps1
```

Ouvrir [l’application locale](http://localhost:3000) et choisir un tenant avec le mot de passe affiché dans le terminal. Les secrets locaux restent dans `jalon-2/.env` et `jalon-2/.env.dashboard`; conserver ces fichiers tant que leurs volumes Docker sont utilisés. Utiliser les boutons Action, Erreur et Accès pour générer un événement. La page affiche sa latence lorsqu’il apparaît dans la vue. La connexion détermine côté serveur quelles données le tenant peut consulter.

Les routes Next.js enregistrent l’activité et son message dans une seule transaction PostgreSQL. Le service outbox publie dans `saas-events` et marque les messages confirmés par Kafka. Une publication peut être répétée si le service s’arrête avant cette mise à jour. Spark dédoublonne par identifiant avec un watermark, écrit une transaction Delta par lot dans MinIO et actualise la vue lue par Next.js. Le rejeu des commandes DoorDash conserve le topic `orders`.

Le tableau présente les fenêtres de commandes, les compteurs d’activité, les alertes GraphFrames et les principaux sommets GraphX. Les valeurs historiques sont lues dans les exports calculés par le jalon 2. Les indicateurs d’activité sont actualisés automatiquement. Le [test de bout en bout](jalon-2/evidence/saas-end-to-end.json) vérifie trois catégories d’événements et l’isolation des sessions.

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

## Préparer la soutenance

1. Présenter le dataset DoorDash et les champs adaptés (1 min).
2. Montrer les deux workers et le DAG RDD (1 min).
3. Expliquer Bronze, Silver et Gold, le watermark et la reprise sur checkpoint (3 min).
4. Déclencher une activité dans Next.js et vérifier sa propagation et son tenant (2 min).
5. Montrer un motif GraphFrames, les degrés comparés et PageRank (2 min).
6. Présenter les limites des données et des mesures (1 min).

Les sorties à remettre se trouvent dans `jalon-2/evidence/` : inventaire `mc-ls-recursive.txt`, groupes Kafka et extraits console. Dans Spark History, consulter les applications batch et leurs DAG. Les captures d’interface à joindre doivent correspondre à l’exécution Kaggle. Renseigner les noms du binôme sur le rapport.
