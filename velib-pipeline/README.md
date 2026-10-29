# 🚲 Vélib' Métropole — Plateforme de données temps réel (architecture médaillon)

Pipeline de données de bout en bout qui ingère le flux temps réel officiel
**GBFS Vélib' Métropole**, le nettoie, l'enrichit et expose des indicateurs
analytiques prêts à consommer (taux de remplissage, stations en tension,
tendances par zone).

## 🎯 Problème résolu

Les données de disponibilité des stations Vélib' sont publiées chaque minute
dans un flux GBFS brut, non structuré et non historisé. Ce projet construit
une plateforme qui **capture ce flux en continu**, le **nettoie et le
déduplique** (couche Silver) et le **transforme en KPI métier** (couche Gold)
pour répondre à des questions concrètes : *quelles stations sont souvent
vides ? à quelles heures la demande est-elle la plus forte ? quelles zones
sont en tension ?*

## 🏗️ Architecture

```
API Vélib' (GBFS)
   │  (toutes les 60 s)
   ▼
Producer Python ──► Kafka (2 topics : station_status, station_information)
   │
   ▼
Spark Structured Streaming ──► BRONZE (Delta Lake sur MinIO, données brutes + horodatage)
   │
   ▼
PySpark batch (nettoyage, typage, dédoublonnage, jointure) ──► SILVER (Delta, partitionné par date)
   │
   ▼
Export JDBC ──► PostgreSQL
   │
   ▼
dbt (modèles Gold + 13 tests qualité) ──► GOLD (dans PostgreSQL)
   │
   ▼
Grafana (dashboard)  |  Airflow (orchestration @hourly)  |  GitHub Actions (CI)
```

**Orchestration** : Airflow (DAG `velib_pipeline` = `run_silver` → `export_to_postgres` → `dbt_run` → `dbt_test`, 2 retries).
**Qualité** : 13 tests dbt (`not_null`, `unique`, `accepted_values`) + tests unitaires `pytest`.
**CI/CD** : GitHub Actions (ruff + pytest + dbt compile).

## 📊 Chiffres clés

| Indicateur | Valeur |
|---|---|
| Topics Kafka | 2 (`velib.station_status`, `velib.station_information`) |
| Messages Bronze `station_information` | 3 076 lignes |
| Messages Bronze `station_status` | 13 716 lignes |
| Lignes Silver (après nettoyage/jointure) | 62 320 lignes |
| Doublons résiduels (station_id + ingested_at) | **0** |
| Valeurs aberrantes (`num_bikes < 0`) | **0** |
| Lignes Gold `gold_remplissage_horaire` | 2 992 |
| Lignes Gold `gold_stations_tension` | 1 496 |
| Lignes Gold `gold_tendances_arrondissement` | 20 |
| Tests dbt | **13 / 13 PASS** |
| Tests unitaires pytest | 13 PASS |
| Latence pipeline (bout en bout) | ~2-3 min (micro-batch 60 s + run Airflow) |
| Fréquence d'ingestion | 60 s (temps réel GBFS officiel) |
| Fréquence d'orchestration | `@hourly` |

## 🧠 Choix techniques justifiés

**Pourquoi Delta Lake ?**
Delta apporte les transactions ACID, le time travel et le `MERGE` (upsert)
sur des fichiers Parquet stockés sur S3/MinIO. C'est indispensable pour
gérer les mises à jour de Silver sans dupliquer les données, et pour pouvoir
rejouer un pipeline sur un état antérieur. Contrairement au Parquet brut,
Delta garantit qu'une lecture ne voit jamais un état partiel d'une écriture.

**Pourquoi dbt ?**
dbt permet de versionner les transformations SQL (modèles `gold_*.sql`),
de générer automatiquement le lineage (`dbt docs`) et surtout d'ajouter
des **tests de qualité déclaratifs** (`not_null`, `unique`,
`accepted_values`) directement dans le CI. La logique métier Gold est
isolée des jobs Spark, ce qui la rend lisible et testable par un analyste.

**Pourquoi Kafka + Spark Structured Streaming ?**
Kafka découple le producer de l'API (si l'API tombe, les données déjà
ingérées ne sont pas perdues) et absorbe les pics. Spark Structured
Streaming offre un modèle de micro-batch (trigger 60 s) qui est le bon
compromis entre latence (near real-time) et simplicité (pas de gestion
d'état complexe). C'est un standard en entreprise.

**Pourquoi PostgreSQL pour Gold ?**
Les tables Gold sont des agrégats de petite taille (quelques milliers de
lignes). Les stocker dans PostgreSQL permet à Grafana (et à tout outil BI)
de les interroger en SQL standard, sans passer par un connecteur Delta
spécifique. C'est plus simple et plus rapide pour du dashboarding.

**Pourquoi MinIO en local ?**
MinIO expose une API compatible S3. Le code Spark utilise donc
`fs.s3a.*` exactement comme en production sur AWS S3. Le passage
local → cloud ne nécessite que de changer l'endpoint et les credentials.

## 🚀 Lancer le projet

**Prérequis** : Docker Desktop, Python 3.11.

```bash
# 1. Démarrer toute la stack
docker compose up -d

# 2. Lancer le producer (terminal séparé, laisser tourner)
python -m venv .venv
.venv\Scripts\Activate.ps1        # Windows
pip install requests kafka-python
python producer/velib_producer.py

# 3. Télécharger les JARs Spark (non versionnés, ~300 Mo)
#    Ils sont exclus du repo car ils dépassent la limite GitHub de 100 Mo/fichier.
New-Item -ItemType Directory -Force -Path spark/jars
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" -OutFile "spark/jars/delta-spark_2.12-3.2.0.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar" -OutFile "spark/jars/delta-storage-3.2.0.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -OutFile "spark/jars/hadoop-aws-3.3.4.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -OutFile "spark/jars/aws-java-sdk-bundle-1.12.262.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar" -OutFile "spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" -OutFile "spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" -OutFile "spark/jars/kafka-clients-3.4.1.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.0/commons-pool2-2.11.0.jar" -OutFile "spark/jars/commons-pool2-2.11.0.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" -OutFile "spark/jars/postgresql-42.7.4.jar"

# 4. Lancer le streaming Bronze (terminal séparé, laisser tourner)
docker compose exec spark /opt/spark/bin/spark-submit --master "local[*]" `
  --jars /opt/spark/jars-extra/delta-spark_2.12-3.2.0.jar,/opt/spark/jars-extra/delta-storage-3.2.0.jar,/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars-extra/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars-extra/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars-extra/kafka-clients-3.4.1.jar,/opt/spark/jars-extra/commons-pool2-2.11.0.jar `
  /opt/spark-apps/bronze_streaming.py

# 5. Orchester Silver + Gold via Airflow
#    → http://localhost:8082 (admin / admin), trigger le DAG velib_pipeline
```

**Interfaces** :

| Service | URL | Identifiants |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Spark UI | http://localhost:4040 | — |
| Airflow | http://localhost:8082 | `admin` / `admin` |
| Grafana | http://localhost:3000 | `admin` / `admin` |

## 📈 Dashboard

Le dashboard Grafana « Vélib - Gold » expose 3 panels alimentés par les
tables Gold :

1. **Taux de remplissage moyen par heure** (courbe 24 h) — `gold_remplissage_horaire`
2. **Top 10 stations souvent vides** (barres) — `gold_stations_tension`
3. **Taux de remplissage par zone et par jour** (série temporelle) — `gold_tendances_arrondissement`

> 📸 *Ajouter ici une capture d'écran du dashboard Grafana et du lineage dbt
> (`dbt docs generate && dbt docs serve`).*

## ✅ Qualité et tests

- **dbt tests** : 13 tests (`not_null`, `unique`, `accepted_values`) sur les
  modèles Gold — tous PASS.
- **pytest** : 13 tests unitaires sur les fonctions de transformation
  (`compute_taux_remplissage`, `is_valid_coordinates`, `parse_ingested_at`).
- **ruff** : lint propre (`All checks passed!`).
- **GitHub Actions** : workflow `ci.yml` exécute ruff + pytest + `dbt compile`
  à chaque push sur `main`.

![CI](https://github.com/VOTRE_USER/velib-pipeline/actions/workflows/ci.yml/badge.svg)

## 📁 Structure du repo

```
velib-pipeline/
├── producer/            # Producer Python → Kafka
├── spark/               # Jobs Bronze (streaming), Silver (batch), export PG
│   ├── jars/            # JARs Delta, Kafka, hadoop-aws, JDBC
│   └── tests/           # Tests unitaires pytest
├── dbt/                 # Projet dbt (modèles Gold + tests)
│   ├── velib/
│   └── profiles/
├── airflow/dags/        # DAG velib_pipeline
├── .github/workflows/   # CI GitHub Actions
├── docker-compose.yml
└── README.md
```

## 🔭 Améliorations possibles

- Ajouter Debezium (CDC sur une table Postgres) comme étape bonus.
- Déployer la couche Silver/Gold sur AWS S3 + Athena (Terraform).
- Passer d'un micro-batch à un vrai streaming continu (Kafka Connect + Flink).
- Ajouter Great Expectations sur la couche Silver.

## 📝 Ligne CV

> **Plateforme de données Vélib' (architecture médaillon)** — Kafka, Spark
> Structured Streaming, Delta Lake, dbt, Airflow, Docker, PostgreSQL,
> Grafana, GitHub Actions : pipeline temps réel de bout en bout avec
> tests de qualité (13 tests dbt, 13 tests pytest) et CI/CD.
