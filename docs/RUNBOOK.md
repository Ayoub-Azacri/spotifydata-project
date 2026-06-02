# RUNBOOK SPOTIFY — Procédures incidents

> Ce document doit être complété par votre groupe au fur et à mesure de la semaine.
> Un bon runbook = ce dont vous auriez eu besoin pendant la panne.

---

## Incidents Phase 1 — Airflow / Batch

### INC-01 — DAG bloqué en "running" depuis > 30 minutes

**Symptômes :** Une tâche reste en état `running` dans l'UI Airflow.

**Diagnostic :**
```bash
# Voir les logs de la tâche
docker compose logs airflow-worker -f

# Lister les tâches actives
docker exec airflow-scheduler airflow tasks states-for-dag-run <dag_id> <run_id>
```

**Résolution :**
```bash
# Marquer la tâche comme failed manuellement
docker exec airflow-scheduler airflow tasks clear <dag_id> -t <task_id> --yes

# Ou tuer le worker et le relancer
docker compose restart airflow-worker
```

**Cause probable :** Souvent causé par un `ExternalTaskSensor` qui attend une tâche qui a échoué silencieusement, ou par une fuite de connexion à la base de données.

---

### INC-02 — Saturation de la Dead Letter Queue (DLQ)

**Symptômes :** Le `dlq_reprocessing_pipeline` tourne mais le nombre d'événements dans `dead_letter_events` avec le statut `abandoned` ou `pending` augmente rapidement.

**Diagnostic :**
```sql
SELECT status, error_type, count(*) FROM dead_letter_events GROUP BY status, error_type;
```

**Résolution :**
```bash
# 1. Identifier la cause (ex: changement de format JSON dans le simulateur P2P).
# 2. Stopper l'ingestion si la corruption est massive.
docker compose exec airflow-scheduler airflow dags pause streaming_events_pipeline

# 3. Corriger le code de validation (src/transformations/events.py).
# 4. Remettre les événements abandonnés en 'pending' pour un nouveau test.
docker compose exec postgres psql -U airflow -d spotify -c "UPDATE dead_letter_events SET status='pending', retry_count=0 WHERE status='abandoned';"
```

**Prévention :** Mettre en place des alertes Slack/Email si la DLQ dépasse un certain seuil.

---

### INC-03 — MinIO inaccessible depuis Airflow

**Symptômes :** Les tâches de lecture/écriture Parquet (`extract_from_minio` ou `store_to_parquet`) échouent avec `Connection refused` ou `NoSuchKey`.

**Diagnostic :**
```bash
docker compose ps minio
curl http://localhost:9000/minio/health/live
```

**Résolution :**
```bash
docker compose restart minio
# Attendre 10s puis relancer le DAGRun (Airflow le fera automatiquement via les 'retries')
```


---

## Incidents Phase 2 — Kafka / Spark

### INC-04 — Consumer lag Kafka qui explose

**Symptômes :** Kafka UI → consumer group `spark-streaming-trends` → lag > 10 000

**Diagnostic :**
```bash
# Vérifier le throughput Spark
docker logs spark-master -f | grep "Batch Duration"

# Vérifier les ressources
docker stats spark-worker-1
```

**Résolution :**
→ À compléter par votre groupe

---

### INC-05 — Job Spark crash avec OutOfMemory

**Symptômes :** `java.lang.OutOfMemoryError: GC overhead limit exceeded`

**Diagnostic :**
```bash
docker logs spark-master -f | grep -i "error\|exception\|oom"
```

**Résolution :**
```bash
# Augmenter la mémoire du worker dans docker-compose
# SPARK_WORKER_MEMORY: 4G

# Réduire le state store : ajouter un TTL sur flatMapGroupsWithState
# GroupState.setTimeoutDuration("1 hour")
```

---

### INC-06 — Spark ne reprend pas depuis le checkpoint

**Symptômes :** Après redémarrage, le job repart de zéro au lieu du checkpoint.

**Diagnostic :**
```bash
# Vérifier que le checkpoint est sur MinIO
docker exec minio mc ls local/spotify-checkpoints/streaming_trends/

# Vérifier les logs Spark au démarrage
docker logs spark-master | grep "checkpoint"
```

**Résolution :**
→ À compléter par votre groupe

---

## Chaos Engineering — Résultats

> Compléter pendant l'issue #25 (vendredi)

### Scénario 1 : Arrêt d'un broker Kafka

**Commande :** `docker compose stop kafka-2`

**Comportement observé :** ...

**Recovery automatique :** oui / non — détails : ...

**Temps de recovery :** ...

---

### Scénario 2 : Kill du driver Spark

**Commande :** `docker compose kill spark-master`

**Comportement observé :** ...

**Recovery depuis checkpoint :** oui / non — détails : ...

**Doublons introduits :** 0 / N — vérification : ...

---

### Scénario 3 : Coupure PostgreSQL

**Commande :** `docker compose stop postgres` (2 minutes) → `docker compose start postgres`

**Comportement observé (Airflow) :** ...

**Comportement observé (Spark) :** ...

**Données perdues :** oui / non — détails : ...
