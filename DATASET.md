# Source et adaptation au cahier des charges

**Dataset retenu : [DoorDash ETA Prediction, version 1](https://www.kaggle.com/datasets/dharun4772/doordash-eta-prediction)**, publié par dharun4772 sur Kaggle. La fiche décrit des livraisons DoorDash du début de 2015 avec du bruit ajouté pour masquer des informations commerciales. Ce sont des données externes publiées, et non des données brutes garanties sans transformation. La licence indiquée par Kaggle est MIT ; conserver cette attribution. Le projet télécharge la source à la demande et ne redistribue pas l'archive originale.

Le CSV contient **197 428 livraisons**. Son SHA-256 vérifié est `b87ae94bd8bf99accc8fd1873dde1ef5f5e1ce76074f22abdb734c54a1bc8d94`. Le téléchargement est épinglé à la version 1 ; toute divergence de ce SHA arrête la préparation.

## Correspondance des champs

| Champ du projet | Origine / limite |
|---|---|
| `order_id` | Identifiant stable construit à partir du numéro de ligne source ; pas un identifiant DoorDash fourni. |
| `restaurant_id` | Couple `market_id` / `store_id`, préfixé `DD-`. |
| `city` | `Market-<market_id>` : marché anonymisé. Aucune ville marocaine ou américaine n'est inventée. |
| `category` | Catégorie majoritaire observée pour le restaurant ; absence = `unknown`. |
| `amount_usd` | `subtotal / 100`, montant original en dollars. Le champ initial `amount_mad` est remplacé pour ne pas présenter des dollars comme des dirhams. Aucun taux de conversion fictif. |
| `items_count` | `total_items` de la source. |
| `CREATED`, `DELIVERED` | Horaires `created_at`, `actual_delivery_time`, interprétés en UTC conformément à la fiche. |
| `PICKED_UP`, `CANCELLED` | Non observés dans la source. Le schéma les accepte ; le dataset principal n'en invente pas. |
| `distance_km`, latitude, longitude | Inconnus, donc `null`. Les seuils de l'UDF sont vérifiés sur des cas de test distincts. La bande de distance streaming est `unknown`. |
| `customer_id`, `courier_id` | Identifiants **simulés** préfixés `SIM-`. Les sessions ne décrivent pas le travail de vrais livreurs. |
| `tenant_id` | Partition pédagogique par marché, pas une organisation SaaS réelle. |

## Sélection reproductible et relecture

L'adaptateur exclut 7 lignes invalides et 3 livraisons de plus de 24 heures. Il retient les 200 couples marché/restaurant les plus fréquents, puis les 24 500 premières commandes complètes chronologiquement. Deux événements par commande donnent 49 000 événements ; 1 000 répétitions exactes portent le fichier à 50 000 lignes. La graine 2026 ajoute un délai d'arrivée entre 1 et 900 secondes à 2 450 événements originaux (5 %). Les données sources et ces perturbations pédagogiques restent distinguées.

`producer.py --mode file --n 50000` reconstruit l'historique et son manifeste. `--mode kafka --rate 20` rejoue ces données historiques dans Kafka ; il ne collecte pas des commandes en direct.

L'expérience automatisée de reprise utilise un sous-ensemble réel sur une horloge de test d'une seconde par événement. Le timestamp source reste dans `source_event_time`. Elle ajoute 20 doublons, une sonde de retard de 15 minutes et deux événements de contrôle du watermark, tous identifiés par `scenario`. Les contrôles sont exclus des KPI métier présentés.

## Graphe et limites métier

La partie 6 demande de générer des utilisateurs, rôles et ressources. Les ressources sont liées aux restaurants du dataset ; utilisateurs, politiques `HAS_ROLE` / `CAN_ACCESS` / `ACCESSED` sont simulés et étiquetés. Les alertes servent à tester les règles, pas à accuser de vrais utilisateurs.

Le taux d'annulation observé est nul car la source ne contient que des livraisons. Ce zéro ne prouve pas l'absence d'annulations chez DoorDash. Un test séparé vérifie le calcul sur des statuts `CANCELLED`. Les sessions et analyses de droits dépendent des identifiants et politiques simulés. Le passage MAD → USD, les villes anonymisées et les champs absents sont des adaptations nécessaires à cette source Kaggle, à présenter au professeur.
