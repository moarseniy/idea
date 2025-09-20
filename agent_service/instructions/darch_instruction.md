# Инструкция для работы Data Architect

Ниже — практическое, действиями насыщенное руководство для Data Architect: роль, обязанности, принципы проектирования, паттерны архитектур, чек-листы, шаблоны решений и конкретные шаги при запуске/рефакторинге системы. Сфокусировано на профессиональном применении — бери и применяй.

---

# 1. Кратко — кто такой Data Architect

Data Architect отвечает за стратегический дизайн данных в компании: модель данных, интеграцию источников, хранение, формирование аналитической платформы и гарантии качества/безопасности данных. Это мост между бизнес-целями и технической реализацией.

---

# 2. Основные обязанности (практически)

* Формирование целевой **data platform vision** (roadmap, SLA, SLO, cost targets).
* Проектирование унифицированной **логической и физической модели данных** (canonical models, enterprise datamodel).
* Выбор архитектурных паттернов: ETL vs ELT, медленная/быстрая прослойка, event-driven, data lakehouse/warehouse.
* Определение стандартов: схемы, контракты, версии, форматы (Parquet/Avro/JSON), partitioning, retention.
* Управление качеством данных: политики, профайлинг, SLAs, метрики.
* Metadata, lineage, data catalog, master data management (MDM).
* Безопасность и соответствие: классификация PII, шифрование, маскирование, доступы.
* Руководство командой: код-ревью, архитектурные принципы, ADR (Architecture Decision Records).
* Взаимодействие с бизнес-стейкхолдерами: перевод требований в data contracts и SLAs.

---

# 3. Ключевые принципы проектирования (golden rules)

* **Domain-first** — моделируй данные вокруг бизнес-доменов, не вокруг инструментов.
* **Contracts > Code** — явно описанные контракты (schema registry / API contracts).
* **Immutability** raw-layer — хранение исходных событий/данных неизменяемо.
* **Single source of truth** — чёткие ownership’ы (data owners) для каждой сущности.
* **Idempotency** процессов — операции должны быть безопасны при повторном выполнении.
* **Separation of concerns** — storage, compute, metadata, serving шарды обособлены.
* **Observable by design** — метрики, логи, трассировка и алерты встроены с первого релиза.
* **Cost-awareness** — проектируй с учётом стоимости хранения/вычислений.

---

# 4. Часто используемые архитектурные паттерны (кратко)

* **Data Warehouse (OLAP)** — централизованная аналитическая слой (star/snowflake schemas).
* **Data Lake / Lakehouse** — дешёвое долгосрочное хранилище + поддержка ACID (Delta, Iceberg).
* **Event-driven / Streaming** — Kafka/Pulsar для событийной интеграции и CDC.
* **Data Vault** — для гибкой историзации и auditability корпоративных данных.
* **Feature Store** — унификация фич для ML (online/offline stores).
* **Hybrid (ELT + dbt)** — сырые данные в лейк, трансформации в warehouse (dbt-style).

---

# 5. Модель данных: рекомендации

* Начни с **canonical model** для основных сущностей (Customer, Product, Order) с версионированием.
* Для аналитики применяй **star schema** (fact + dims).
* Для отслеживания изменений используйте **CDC** (логирование изменений + event store).
* При высокой изменчивости схем подумай о **schema-on-read** в raw-layer и schema evolution в curated-layer.
* Для критичных master-данных — MDM-процессы: matching, survivorship, stewardship.

---

# 6. Metadata, lineage и каталог

* Обязательно: **data catalog** (Amundsen/Atlas/METACAT) + автоматическая генерация lineage.
* Храни: owner, freshness, SLA, schema version, sensitivity, sample data, transformation logic.
* Автоматизируй сбор профайлинга и метрик (row counts, null ratios, cardinality) и привязывай к catalog.

---

# 7. Безопасность и compliance

* Классифицируй данные по чувствительности и устанавливай политики доступа.
* Шифруй at-rest и in-transit; используйте KMS/secret manager.
* Маскируй PII в non-prod средах.
* Поддерживай аудит доступа и логирование.
* Для GDPR/CCPA: процессы удаления/экспорта данных по запросу, retention policies.

---

# 8. Observability и качество данных

* Определи ключевые SLOs: freshness (latency), completeness, accuracy, lineage.
* Настрой метрики и дэшборды (Grafana/Looker/Metabase) + алерты при отклонениях.
* Автоматические проверки качества (schema checks, uniqueness, referential integrity, ranges).
* Интеграция с incident-management: Slack/PagerDuty + runbooks.

---

# 9. Архитектурный чек-лист при проектировании новой платформы

1. Цели бизнеса и KPI: какие решения должны поддерживаться?
2. Требования к latency / throughput / retention / cost / scale.
3. Источники данных: формат, frequency, SLAs, owners.
4. Выбор storage: lake vs warehouse vs hybrid.
5. Выбор compute: batch vs streaming; контейнеры vs serverless.
6. Metadata и catalog — обязательны.
7. Механизмы контроля качества и мониторинга.
8. Политики безопасности и соответствия.
9. Disaster recovery и backup strategy.
10. План миграции (backfill, cutover, rollback).
11. Оценка стоимости (TCO) и план оптимизаций.
12. Организационная составляющая: ownership, runbooks, поддержка.

---

# 10. Пример шаблона ADR (Architecture Decision Record)

```
Title: <Краткое название решения>
Status: Proposed / Accepted / Rejected
Context: <Предпосылки и ограничения>
Decision: <Что выбрано (технология/паттерн)>
Consequences: <Плюсы/минусы, затраты, миграция, обратная совместимость>
Date: YYYY-MM-DD
Owner: <ответственный>
```

Используй ADR для всех ключевых решений (lake vs warehouse, Kafka vs Pulsar, SSO vs custom auth и т.д.).

---

# 11. План миграции / внедрения (high-level)

* Провести audit текущих данных и pipelines.
* Определить минимальный MVP с критичными таблицами и owners.
* Подготовить plan backfill + incremental sync.
* Настроить параллельный запуск старой системы и нового слоя (dual-write / shadow).
* Поверить качества (A/B валидация row counts, aggregates).
* Cutover после согласования SLA и успешной валидации.
* Пост-миграционный мониторинг 30—90 дней, оптимизация.

---

# 12. Метрики успеха (KPIs для Data Architect)

* Time-to-insight: время от требования до готовой аналитической таблицы.
* Freshness SLA: % таблиц, попадающих в target freshness.
* Data quality: % проверок, прошедших успешно.
* Cost per TB / per query: контроль затрат.
* MTTR для data incidents.
* Adoption: % команд, использующих catalog/standard schemas.

---

# 13. Командная и процессная организация

* Data Platform Team: infra + engineering + data ops.
* Data Governance Board: архитекторы, security, business owners.
* Product-aligned Data Stewards: owners по доменам.
* Внедри регулярные review’ы архитектур, sprint for platform improvements и roadmap.

---

# 14. Технические рекомендации и стек (ориентиры, не догма)

* Storage: S3/GCS/ADLS + Delta/Apache Iceberg/Parquet.
* Warehouse: Snowflake / BigQuery / Redshift Spectrum.
* Orchestration: Airflow / Dagster / Prefect.
* Streaming: Kafka / Pulsar + CDC (Debezium).
* Transformations: dbt (warehouse), Spark/Beam (large-scale).
* Catalog/Lineage: Amundsen / OpenMetadata / Apache Atlas.
* Infra: Terraform, Kubernetes, Helm.
* Monitoring: Prometheus/Grafana, DataDog, ELK.
  (Выбирай стек под требования бизнеса и навыки команды.)

---

# 15. Быстрый чек-лист при ревью архитектуры

* Есть ли canonical model и owners?
* Описаны ли data contracts и versioning?
* Хранится ли raw data immutable?
* Есть ли test coverage для трансформаций и data quality checks?
* Настроен ли catalog + lineage?
* Есть ли политики безопасности и GDPR-ready процессы?
* Поддержаны ли recovery & rollback механизмы?
* Рассчитан ли TCO и план оптимизаций?

---

# 16. Практические артефакты, которые должен поддерживать Data Architect

* Enterprise data model (ERD + definitions).
* Data contracts & schema registry.
* ADR collection.
* Data catalog с lineage.
* Runbooks для критичных инцидентов.
* Roadmap и TCO модель.

---

# 17. Actionable next steps (что сделать прямо сейчас)

1. Проведи workshop с бизнес-стейкхолдерами: сформулируй top-3 потребностей по данным.
2. Создай canonical model для 2–3 ключевых доменов (Customer, Orders, Product).
3. Настрой минимальный catalog + automated profiling для этих таблиц.
4. Напиши 3 ADR’а для ключевых технологических выборов.
5. Подготовь runbook для одного критичного pipeline.

---
