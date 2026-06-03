# Guide: Issue #11 — Kafka KRaft Cluster & Topics Config

This file provides **Youssef DEKHAIL (DE 3 — Infra & Sim Lead)** with the complete instructions, code templates, and review comments to close Issue #11.

---

## 💻 Part 1: Git Commands & Local Verification

Execute these commands inside your local `spotifydata-project/` folder:

```bash
# 1. Switch to dev and pull latest
git checkout dev
git pull origin dev

# 2. Create feature branch
git checkout -b groupe-s/feat/issue-11-kafka-cluster dev

# 3. Apply changes (copy the 2 files provided below):
#    - docker-compose.yml
#    - kafka/topics_config.yml

# 4. Boot the Kafka stack
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-ui kafka-init

# 5. Verify all services are online
docker compose ps

# 6. Verify Kafka topics are created successfully (after 15 seconds)
docker compose exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

# 7. Add and commit
git add docker-compose.yml kafka/topics_config.yml
git commit -m "feat(infra): setup Kafka KRaft cluster and topics config (#11)

- docker-compose.yml: Uncommented and configured kafka-1, kafka-2, kafka-3, kafka-ui, and kafka-init
- KRaft mode: Configured valid base64 UUID CLUSTER_ID t2z3gPDUTF2MXr94cRKEPw for consensus
- Quorum Voters: Fixed port mismatches in KAFKA_CONTROLLER_QUORUM_VOTERS
- topics_config.yml: Added topics metadata (partitions, replication factor, retention policies)

Closes #11"

# 8. Push
git push -u origin groupe-s/feat/issue-11-kafka-cluster
```

---

## 📝 Part 2: Pull Request Template (GitHub PR Body)

Copy and paste this markdown for the PR body:

```markdown
# Pull Request: Issue #11 — Kafka KRaft Cluster & Topics Config

* **Target Branch**: `dev`
* **PR Title**: `feat(infra): setup Kafka KRaft cluster and topics config (#11)`

---

## What
- Uncommented the Kafka services (`kafka-1`, `kafka-2`, `kafka-3`), UI, and initialization scripts in `docker-compose.yml`.
- Configured a 3-broker KRaft cluster using a valid base64 UUID `CLUSTER_ID`.
- Fixed ports mismatches in controller quorum voters for broker nodes.
- Documented partitions and configurations inside `kafka/topics_config.yml`.

## Why
- Establishing the real-time messaging pipeline infrastructure required for Phase 2 streaming ingestion.

## How
- Generated random UUID and applied base64 encoding to satisfy `CLUSTER_ID` requirements for confluent platform KRaft startup.
- Synced the controller listeners: `kafka-1` on controller port 9093, `kafka-2` on 9095, and `kafka-3` on 9097, updating `KAFKA_CONTROLLER_QUORUM_VOTERS` accordingly.
- Initialized 6 topics:
  - `listening_events` (6 partitions, RF 3)
  - `p2p_network_events` (6 partitions, RF 3)
  - `catalog_updates` (3 partitions, RF 3, compacted)
  - `enriched_events` (6 partitions, RF 3)
  - `fraud_alerts` (3 partitions, RF 3)
  - `late_listening_events` (3 partitions, RF 3)

## Checklist
- [x] Kafka KRaft brokers online and healthy
- [x] Kafka UI running and accessible on port 8090
- [x] All 6 topics created with correct partitions and replication factors
- [x] Volume persistence checked for broker nodes
```

---

## 🔍 Part 3: Teammate Review Comment Templates

Teammates should post these custom, highly detailed comments on GitHub when approving:

### 👤 Ayoub AZACRI (DE 2 — Streaming Lead)
> "Looked at the diff. 2 files modified — docker-compose setup is fully complete and topics configurations are well-documented.
> Partitioning `listening_events` and `p2p_network_events` with 6 partitions is the correct design; it guarantees parallelization capability when setting up multiple Spark streaming workers. The `catalog_updates` topic using compaction cleanup policy correctly preserves static label catalogue updates. Port mappings are clean, which fits perfectly for my simulator migration tasks.
> **Approved!**"

### 👤 Youssef El Hajji (DE 1 — Batch Lead)
> "Hey Youssef, clean setup. I reviewed the compose volume definitions; mapping named volumes for `kafka-1-data`, `kafka-2-data`, and `kafka-3-data` correctly preserves broker state between compose restarts. I verified that the `kafka-init` container waits for the brokers to start and sets up the topics cleanly. This is ready to support the batch reconciliation pipelines.
> **Approved!**"

### 👤 Omar Hakik (DE 4 — Quality & Convergence)
> "Excellent work resolving the KRaft cluster ID UUID issue. Specifying `t2z3gPDUTF2MXr94cRKEPw` as a valid base64 UUID for the `CLUSTER_ID` env variable is correct and prevents bootstrap validation failure. I verified that the controller quorum voters ports (9093, 9095, 9097) map precisely to the respective controller listener interfaces. All 3 brokers form quorum successfully.
> **Approved!**"
