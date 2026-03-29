# ️ Yelp Data Platform — M1 Data Engineering EFREI

> Architecture médaillon complète sur données Yelp Open Dataset : ingestion HDFS, traitement Spark, datamarts PostgreSQL, API REST sécurisée JWT et visualisation.

**Dépôt GitHub** : https://github.com/ErvinGoby23/big-data-efrei.git

---

##  Sommaire

- [Problématique business](#problématique-business)
- [Architecture](#architecture)
- [Stack technique](#stack-technique)
- [Datasets](#datasets)
- [Structure du projet](#structure-du-projet)
- [Lancer le projet](#lancer-le-projet)
- [Pipeline Spark](#pipeline-spark)
- [Datamarts](#datamarts)
- [API REST](#api-rest)
- [Choix techniques](#choix-techniques)

---

##  Problématique business

**Quels restaurants et quelles villes offrent la meilleure expérience client aux États-Unis, et comment cette qualité évolue-t-elle dans le temps ?**

À partir des avis Yelp, on construit une plateforme permettant d'analyser :
- L'attractivité des villes par score composite (notes + avis + check-ins)
- La performance des catégories de restaurants
- L'évolution temporelle de la satisfaction client
- La tendance (montante / stable / déclinante) de chaque établissement

## ️ Stack technique

| Composant | Technologie |
|---|---|
| Data Lake | HDFS (Hadoop 3.2.1) |
| Traitement | Apache Spark 3.0.0 (Standalone) |
| Metastore | Apache Hive 2.3.2 |
| Base relationnelle | PostgreSQL 15 |
| API | FastAPI + JWT (python-jose) |
| Orchestration | Docker Compose |
| Langage | Python 3 (PySpark) |

---

##  Datasets

Source : **Yelp Open Dataset** — https://www.yelp.com/dataset

| Fichier | Description | Taille |
|---|---|---|
| `yelp_academic_dataset_business.json` | Informations sur les établissements | ~119 MB |
| `yelp_academic_dataset_checkin.json` | Historique des check-ins | ~287 MB |
| `yelp_academic_dataset_review.json` | Avis clients (réduit à ~1 GB) | ~1 GB |

**Volume total après ingestion** : > 200 000 lignes 

---

##  Structure du projet

```
big-data-efrei/
├── pipeline/
│   ├── feeder.py                  # Ingestion → /raw
│   ├── processor.py               # Traitement → /silver + Hive
│   ├── datamart.py                # Création des datamarts → PostgreSQL
│   ├── load_review_to_postgres.py # Chargement reviews dans PG
│   └── postgresql-42.6.0.jar     # Driver JDBC
├── api/
│   ├── main.py                    # API FastAPI + JWT
│   ├── requirements.txt
│   ├── Dockerfile
│   └── test_api.sh                # Script de test des endpoints
├── docker-compose.yml             # Infrastructure complète
├── hadoop.env                     # Config Hadoop
├── hadoop-hive.env                # Config Hive
└── README.md
```

---

##  Lancer le projet

### Prérequis

- Docker Desktop (16 GB RAM recommandé)
- Python 3 (pour trim_json.py)
- Fichiers Yelp JSON téléchargés

### 1. Démarrer l'infrastructure

```bash
docker-compose up -d
```

Services démarrés : Namenode, Datanode, ResourceManager, NodeManager, Spark Master, 2 Workers, Hive, PostgreSQL, API.

Interfaces disponibles :

| Interface | URL |
|---|---|
| Spark UI | http://localhost:8080 |
| HDFS Namenode | http://localhost:9870 |
| Resource Manager | http://localhost:8088 |
| Spark Job UI | http://localhost:4040 |
| API Swagger | http://localhost:8000/docs |

### 2. Charger les données dans HDFS

```bash
# Copier depuis la machine vers le container namenode
docker cp yelp_academic_dataset_business.json namenode:/tmp/
docker cp yelp_academic_dataset_checkin.json namenode:/tmp/
docker cp yelp_academic_dataset_review.json namenode:/tmp/

# Depuis le namenode, pousser vers HDFS
docker exec -it namenode hdfs dfs -mkdir -p /data/yelp
docker exec -it namenode hdfs dfs -put /tmp/yelp_academic_dataset_business.json /data/yelp/
docker exec -it namenode hdfs dfs -put /tmp/yelp_academic_dataset_checkin.json /data/yelp/
docker exec -it namenode hdfs dfs -put /tmp/yelp_academic_dataset_review.json /data/yelp/
```

### 3. Charger les reviews dans PostgreSQL

```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-cores 2 --total-executor-cores 6 --executor-memory 3g --conf spark.executor.memoryOverhead=512m --conf spark.sql.shuffle.partitions=12 --packages org.postgresql:postgresql:42.6.0 --conf spark.app.hdfs.review=hdfs://namenode:9000/data/yelp/yelp_academic_dataset_review.json --conf spark.app.pg.url=jdbc:postgresql://postgres-yelp:5432/yelp_dw --conf spark.app.pg.user=yelp --conf spark.app.pg.password=yelp123 /opt/pipeline/load_review_to_postgres.py
```

### 4. Feeder

```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-cores 2 --total-executor-cores 6 --executor-memory 3g --conf spark.executor.memoryOverhead=512m --conf spark.sql.shuffle.partitions=12 --packages org.postgresql:postgresql:42.6.0 --conf spark.app.hdfs.business=hdfs://namenode:9000/data/yelp/yelp_academic_dataset_business.json --conf spark.app.hdfs.checkin=hdfs://namenode:9000/data/yelp/yelp_academic_dataset_checkin.json --conf spark.app.raw.output=hdfs://namenode:9000/data/raw --conf spark.app.pg.url=jdbc:postgresql://postgres-yelp:5432/yelp_dw --conf spark.app.pg.user=yelp --conf spark.app.pg.password=yelp123 /opt/pipeline/feeder.py
```

### 5. Processor

```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-cores 2 --total-executor-cores 6 --executor-memory 3g --conf spark.executor.memoryOverhead=512m --conf spark.sql.shuffle.partitions=12 --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict --conf spark.app.raw.business=hdfs://namenode:9000/data/raw/business --conf spark.app.raw.review=hdfs://namenode:9000/data/raw/review --conf spark.app.raw.checkin=hdfs://namenode:9000/data/raw/checkin --conf spark.app.silver.path=hdfs://namenode:9000/datalake/silver /opt/pipeline/processor.py
```

### 6. Datamart

```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-cores 2 --total-executor-cores 6 --executor-memory 3g --conf spark.executor.memoryOverhead=512m --conf spark.sql.shuffle.partitions=12 --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --conf spark.app.pg.url=jdbc:postgresql://postgres-yelp:5432/yelp_dw --conf spark.app.pg.user=yelp --conf spark.app.pg.password=yelp123 --conf spark.app.silver.path=hdfs://namenode:9000/datalake/silver --packages org.postgresql:postgresql:42.6.0 /opt/pipeline/datamart.py
```

---

## ️ Pipeline Spark

### feeder.py — Ingestion (couche Bronze/Raw)

- Lit `business.json` et `checkin.json` depuis HDFS
- Lit les reviews depuis PostgreSQL via JDBC
- Ajoute les colonnes `year`, `month`, `day` pour le partitionnement
- Utilise `cache()` sur business et checkin, `persist(DISK_ONLY)` sur review
- Écrit en Parquet partitionné `year/month/day` dans `/raw`
- Logs dans `feeder.txt`

### processor.py — Traitement (couche Silver)

**5 règles de validation :**
1. `business_id` non null
2. Catégorie contient "Restaurants"
3. Étoiles entre 1 et 5
4. `review_id` non null
5. Texte de l'avis non vide

**Transformations :**
- Nettoyage : normalisation city (lowercase), state (uppercase), cast des types
- Jointure : business × review × checkin (left joins)
- Window function : `rank_in_city` — classement des restaurants par ville (PARTITION BY city, ORDER BY stars DESC)
- `cache()` sur business, `persist(DISK_ONLY)` sur le dataset jointé
- Écriture Silver en Parquet + création tables Hive externes
- Logs dans `processor.txt`

### datamart.py — Datamarts

Lit depuis Silver, écrit 5 tables dans PostgreSQL via JDBC.
Logs dans `datamart.log`.

---

##  Datamarts

| Table | Description |
|---|---|
| `dm1_top_villes` | Top 50 villes par score d'attractivité |
| `dm2_performance_concepts` | Top 100 catégories par note moyenne |
| `dm3_features_prediction` | Features par restaurant (ML-ready) |
| `dm4_evolution_temporelle` | Volume d'avis et notes par mois |
| `dm4_tendances_business` | Tendance montant/stable/déclinant par restaurant |
| `dm5_voix_client` | Satisfaction et utilité des avis par restaurant |

---

##  API REST

**Base URL** : `http://localhost:8000`

**Documentation** : `http://localhost:8000/docs`

### Authentification

```bash
POST /auth/login
Content-Type: application/x-www-form-urlencoded
Body: username=admin&password=admin123
```

Retourne un Bearer JWT (expire après 60 min).

**Comptes disponibles :**
- `admin` / `admin123`
- `student` / `efrei2024`

### Endpoints

| Méthode | Endpoint | Description |
|---|---|---|
| POST | `/auth/login` | Obtenir un token JWT |
| GET | `/auth/me` | Infos utilisateur connecté |
| GET | `/health` | Statut API + DB |
| GET | `/debug/db` | Liste les tables PostgreSQL |
| GET | `/stats` | Nombre de lignes par datamart |
| GET | `/datamarts/top-villes` | DM1 — filtrable par `state` |
| GET | `/datamarts/categories` | DM2 |
| GET | `/datamarts/restaurants` | DM3 — filtrable par `city`, `state`, `is_open` |
| GET | `/datamarts/evolution-temporelle` | DM4 mensuel |
| GET | `/datamarts/tendances` | DM4 — filtrable par `trend` |
| GET | `/datamarts/voix-client` | DM5 — filtrable par `satisfaction` |

**Pagination** : tous les endpoints acceptent `?page=1&page_size=10` (max 100).

---

##  Choix techniques

### Partitionnement

Les couches raw et silver sont partitionnées par `year/month/day`. La table `yelp_joined` est partitionnée par `state/city` car les requêtes business filtrent majoritairement par localisation.

### Configuration Spark

Environnement Spark Standalone avec 2 workers.

### Pagination API

Pagination offset-based (`page` + `page_size`) implémentée côté PostgreSQL avec `LIMIT/OFFSET`. Simple et standard, suffisant pour les volumes de datamarts (50 à 10 000 lignes).

---

##  Auteurs

Ervin GOBY et Rayan TOUMERT
