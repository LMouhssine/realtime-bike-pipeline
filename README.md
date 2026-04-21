# Pipeline CityBikes France en Temps Reel

Projet de streaming de bout en bout pour ingerer la telemetrie des stations de velos depuis l'[API CityBikes](https://api.citybik.es/v2/), la publier dans Kafka, la traiter avec PySpark Structured Streaming, la stocker dans PostgreSQL et Parquet, puis exposer des indicateurs de mobilite urbaine dans Streamlit.

La configuration par defaut cible cinq grandes villes francaises toutes les 60 secondes :

- Paris
- Lyon
- Marseille
- Toulouse
- Bordeaux

`CITYBIKES_API_KEY` est fortement recommande. L'API publique CityBikes documente une limite de `300 requetes/heure`, et un polling de cinq villes toutes les minutes laisse tres peu de marge pour les retries.

## Architecture

```text
                  +----------------------+
                  |  API CityBikes v2    |
                  |  /networks + feeds   |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  | Producteur Python    |
                  | requests + retries   |
                  | decouverte reseaux   |
                  | FRA dynamique        |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  | Topic Kafka          |
                  | bike-stations        |
                  +----------+-----------+
                             |
                             v
                  +----------------------+
                  | Spark Structured     |
                  | Streaming            |
                  | schema + nettoyage   |
                  | metriques usage      |
                  +----+------------+----+
                       |            |
                       v            v
         +-------------------+   +----------------------+
         | PostgreSQL        |   | Archive Parquet      |
         | faits + tables    |   | historique partition |
         | analytiques       |   | data lake local      |
         +---------+---------+   +----------------------+
                   |
                   v
         +----------------------+
         | Dashboard Streamlit  |
         | cartes, pics, alertes|
         +----------------------+
```

## Structure du projet

```text
citybike-stream-analytics/
├── config/
│   ├── __init__.py
│   └── settings.py
├── dashboard/
│   ├── __init__.py
│   └── app.py
├── data_ingestion/
│   ├── __init__.py
│   └── producer.py
├── docker/
│   └── spark/
│       └── Dockerfile
├── storage/
│   ├── __init__.py
│   ├── db_writer.py
│   └── init.sql
├── streaming/
│   ├── __init__.py
│   └── spark_streaming.py
├── tests/
│   ├── conftest.py
│   ├── test_dashboard_helpers.py
│   ├── test_producer.py
│   └── test_streaming_transformations.py
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements-dev.txt
├── requirements.txt
└── README.md
```

## Ce que produit le pipeline

Chaque message Kafka contient :

- `station_id`
- `station_name`
- `latitude`
- `longitude`
- `bikes_available`
- `free_slots`
- `timestamp`
- `network_id`
- `city`
- `station_key`
- `ingested_at`

Spark enrichit ensuite chaque enregistrement avec :

- `capacity`
- `utilization_rate`
- `zone_id`
- `event_date`
- `event_hour`

PostgreSQL stocke les objets suivants :

- `station_status_facts`
- `latest_station_status`
- `top_utilization_stations`
- `low_availability_zones`
- `daily_usage_peaks`
- `geographic_imbalance`
- `critical_stations`
- `station_alerts`

L'historique Parquet est ecrit sous `data/parquet/station_status/`.

## Installation

### 1. Prerequis

- Docker Desktop avec Compose
- Python 3.11 ou 3.12 recommande pour les composants lances en local
- Java n'est pas necessaire en local si Spark est execute dans Docker

Remarque :

- Python 3.13 peut poser probleme avec `kafka-python` sur certaines machines. Si c'est votre cas, utilisez un conteneur Python dedie pour le producteur ou le dashboard.

### 2. Configurer l'environnement

Copiez le fichier d'exemple :

```powershell
Copy-Item .env.example .env
```

Ajoutez votre cle API si vous en avez une :

```dotenv
CITYBIKES_API_KEY=your_api_key_here
```

Ports exposes sur l'hote :

- Kafka : `localhost:29092`
- PostgreSQL : `localhost:55432`
- UI Spark Master : `localhost:8080`
- UI Spark Worker : `localhost:8081`
- Dashboard Streamlit : `localhost:8501` une fois lance

### 3. Installer les dependances Python

Dependances runtime pour le producteur et le dashboard :

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Dependances de test en option :

```powershell
python -m pip install -r requirements-dev.txt
```

### 4. Demarrer l'infrastructure

Construisez l'image Spark personnalisee puis lancez la plateforme :

```powershell
docker compose build spark-master spark-worker
docker compose up -d
```

Interfaces disponibles :

- Spark Master UI : [http://localhost:8080](http://localhost:8080)
- Spark Worker UI : [http://localhost:8081](http://localhost:8081)

## Execution du pipeline

Demarrez les composants dans cet ordre.

### 1. Demarrer le producteur

Depuis la racine du projet :

```powershell
python -m data_ingestion.producer
```

Ce qu'il fait :

- decouvre dynamiquement les reseaux francais CityBikes via `/networks`
- resout les villes cibles configurees
- cree le topic Kafka `bike-stations` s'il n'existe pas
- recupere les snapshots de stations toutes les 60 secondes
- publie un message JSON par station

### 2. Demarrer le job Spark Structured Streaming

Executez le job dans le conteneur Spark master :

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.postgresql:postgresql:42.7.3 `
  /opt/project/streaming/spark_streaming.py
```

Ce qu'il fait :

- lit le topic Kafka `bike-stations`
- applique un schema explicite
- supprime les enregistrements invalides ou impossibles
- deduplique sur `station_key + snapshot_timestamp`
- calcule `capacity` et `utilization_rate`
- archive les donnees nettoyees dans Parquet
- ecrit les faits et les tables analytiques dans PostgreSQL

### 3. Demarrer le dashboard

Dans un autre shell :

```powershell
streamlit run dashboard/app.py
```

Onglets du dashboard :

- Vue d'ensemble
- Carte des stations
- Carte de chaleur
- Pics d'usage
- Desequilibre geographique
- Alertes

### 4. Interroger PostgreSQL

Depuis l'hote :

```powershell
psql -h localhost -p 55432 -U citybike -d citybike
```

Ou directement dans le conteneur :

```powershell
docker exec -it realtime-bike-pipeline-postgres-1 psql -U citybike -d citybike
```

## Logique metier implemente

### Taux d'utilisation maximal

- table : `top_utilization_stations`
- definition : dernier snapshot classe par `utilization_rate DESC`

### Zones avec disponibilite insuffisante

- table : `low_availability_zones`
- definition : groupes `zone_id` du dernier snapshot avec une moyenne `bikes_available < LOW_AVAILABILITY_THRESHOLD`

### Pics d'usage journaliers

- table : `daily_usage_peaks`
- definition : proxy horaire d'activite calcule a partir des deltas de `bikes_available` et `free_slots`

### Desequilibre geographique

- table : `geographic_imbalance`
- definition : ratio de disponibilite de la zone compare au ratio moyen de la ville

### Stations critiques

- table : `critical_stations`
- definition : `bikes_available <= CRITICAL_BIKES_THRESHOLD` et utilisation glissante sur 15 minutes au-dessus de `CRITICAL_UTILIZATION_THRESHOLD`

### Alertes

- table : `station_alerts`
- emises quand :
  - une station devient vide
  - une station entre dans l'etat critique

## Exemples de requetes

Stations les plus contraintes :

```sql
SELECT city, station_name, bikes_available, free_slots, utilization_rate
FROM top_utilization_stations
ORDER BY utilization_rank
LIMIT 10;
```

Zones susceptibles d'avoir besoin de reequilibrage :

```sql
SELECT city, zone_id, avg_bikes_available, avg_utilization_rate
FROM low_availability_zones
WHERE shortage_flag = TRUE
ORDER BY avg_bikes_available ASC;
```

Heures de pointe par ville :

```sql
SELECT city, usage_date, usage_hour, estimated_activity
FROM daily_usage_peaks
WHERE peak_rank = 1
ORDER BY usage_date DESC, city;
```

Zones les plus desequilibrees :

```sql
SELECT city, zone_id, imbalance_score, zone_availability_ratio, city_availability_ratio
FROM geographic_imbalance
ORDER BY ABS(imbalance_score) DESC
LIMIT 20;
```

Alertes recentes :

```sql
SELECT city, station_name, alert_type, alert_message, snapshot_timestamp
FROM station_alerts
ORDER BY created_at DESC
LIMIT 20;
```

## Tests

Executer les tests unitaires :

```powershell
python -m pytest tests/test_producer.py tests/test_dashboard_helpers.py
```

Executer les tests de transformation Spark si `pyspark` est installe localement :

```powershell
python -m pytest tests/test_streaming_transformations.py
```

## Notes et compromis

- Ce projet est une vitrine locale, pas un deploiement de production.
- La demande est deduite de l'evolution de l'etat des stations, car CityBikes expose des snapshots de disponibilite et non des evenements de trajet.
- Le dashboard est volontairement en lecture seule et gere proprement les etats de demarrage a vide.
- Airflow n'est pas inclus dans cette v1 afin de garder un stack simple a lancer localement.
- Un historique CityBikes existe deja sur [data.citybik.es](https://data.citybik.es/), mais ce projet reconstruit son propre historique local a partir du flux temps reel.

## Attribution

- Source de donnees live : [CityBikes API v2](https://api.citybik.es/v2/)
- Documentation API et limites : [docs.citybik.es/api](https://docs.citybik.es/api/)
- Reference dataset historique : [data.citybik.es](https://data.citybik.es/)
