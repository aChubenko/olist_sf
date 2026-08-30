# Production troubleshooting: Astro, Cosmos, dbt и Snowflake

Документ описывает типичные production-сбои для используемого в проекте стека:

- Astronomer Astro Runtime и Apache Airflow;
- Astronomer Cosmos;
- `dbt-core` и `dbt-snowflake`;
- Snowflake ingestion через `PUT` / `COPY INTO`;
- `chardet` для определения кодировок.

Цель документа — быстро связать наблюдаемый симптом с вероятной причиной и безопасным способом исправления. Рекомендации относятся к production-пайплайну; для текущего небольшого статического Olist-датасета некоторые риски осознанно приняты ради простоты.

## Приоритеты для текущего проекта

| Приоритет | Риск | Текущее состояние | Рекомендуемое изменение перед production |
|---|---|---|---|
| Закрыт | Сбой между очисткой Bronze и завершением `COPY` | Реализованы временные таблицы, row-count validation и единая транзакция публикации | Добавить защищённый integration test на реальном Snowflake |
| P0 | Повторная загрузка создаёт дубли после перехода на append | Используются случайный `_LOAD_ID` и `FORCE = TRUE` | Ввести детерминированные `batch_id`/`record_id`, load ledger и `MERGE` |
| P1 | Dev-запуск перезаписывает общие `SILVER`/`GOLD` | `generate_schema_name` возвращает точное custom schema | Изолировать схемы по окружению или пользователю |
| P1 | Production-права слишком широкие | В dbt profile роль по умолчанию — `SYSADMIN` | Создать отдельную service role с минимальными grants |
| P1 | Freshness зелёный для фактически старых бизнес-данных | Freshness проверяет ingestion `_LOADED_AT` | Отдельно контролировать source event/update timestamp |
| P2 | Медленный DAG parsing при росте dbt-проекта | Cosmos cache отключён, используется `dbt ls` | Включить cache либо публиковать `manifest.json` из CI |
| P2 | CI не обнаруживает проблемы подключения и grants | CI использует фиктивные Snowflake credentials | Добавить отдельный защищённый integration/canary job |
| P3 | Лишняя зависимость и ложное чувство защиты кодировок | `chardet` установлен, но не вызывается | Удалить после проверки зависимостей либо использовать с явным порогом confidence |

## 1. Идемпотентность и загрузка Bronze

### 1.1. Атомарная публикация вместо `TRUNCATE` и прямого `COPY INTO`

**Симптомы**

- Bronze-таблица внезапно пустая;
- часть таблиц содержит новый batch, а часть — старый;
- повторный запуск обычно восстанавливает данные, но потребители успевают увидеть неконсистентное состояние.

**Реализация в проекте**

Загрузчик больше не очищает целевые таблицы во время подготовки. Он использует следующий алгоритм:

1. Загрузить данные в отдельную временную таблицу.
2. Сверить количество загруженных строк с результатом локальной проверки CSV.
3. После успешной подготовки всех девяти файлов открыть явную транзакцию.
4. Выполнить для каждой целевой таблицы `INSERT OVERWRITE ... SELECT` из временной таблицы.
5. В той же транзакции записать file-level audit и статус запуска `SUCCESS`.
6. При любой ошибке выполнить `ROLLBACK` и записать статус `FAILED`.
7. Удалить временные таблицы и файлы из internal stage.

Snowflake выполняет `INSERT OVERWRITE` как DML, поэтому он участвует в транзакции. DDL временных таблиц выполняется до `BEGIN TRANSACTION`, чтобы не вызвать неявный commit внутри публикации.

Официальная документация: [Snowflake transactions](https://docs.snowflake.com/en/sql-reference/transactions) и [`INSERT OVERWRITE`](https://docs.snowflake.com/en/sql-reference/sql/insert).

Для инкрементальной загрузки вместо полной замены использовать `MERGE` по стабильному ключу записи.

### 1.2. `FORCE = TRUE` отключает дедупликацию файлов Snowflake

**Симптомы**

- один файл повторно загружается после retry;
- после перехода с full refresh на append появляются дубли;
- штатная load history Snowflake не предотвращает повтор.

**Причина**

`COPY INTO ... FORCE = TRUE` заставляет Snowflake загружать файл независимо от сохранённой истории загрузок. Для текущего full refresh это допустимо, потому что файл загружается по уникальному пути во временную таблицу, а целевая таблица затем атомарно заменяется через `INSERT OVERWRITE`. Перенос такого кода в append-сценарий опасен.

**Решение**

- не использовать `FORCE = TRUE` для штатной append-загрузки;
- давать объектам в stage неизменяемые уникальные пути;
- хранить долгоживущий собственный load ledger, поскольку встроенная load history ограничена;
- использовать checksum содержимого, если одинаковый файл может появиться под другим именем.

Официальная документация: [Snowflake loading considerations](https://docs.snowflake.com/en/user-guide/data-load-considerations-load).

### 1.3. Сервисная таблица без защиты целевой таблицы

**Симптомы**

- в control table нет `SUCCESS`, но данные уже присутствуют;
- retry добавляет второй экземпляр batch;
- два параллельных процесса одновременно считают batch новым.

**Причина**

Последовательность «проверить ledger → append → записать SUCCESS» имеет окно сбоя между append и обновлением ledger. Сервисная таблица помогает управлять процессом, но сама по себе не делает запись идемпотентной.

**Решение**

Использовать оба уровня защиты:

- детерминированный `batch_id`, например SHA-256 содержимого файла;
- детерминированный `record_id`, например SHA-256 от `batch_id + source_row_number`;
- `MERGE` целевых строк по `record_id`;
- статусы ledger `RUNNING`, `SUCCESS`, `FAILED`;
- запись данных и финального статуса в одной транзакционной границе, где это возможно;
- single-writer либо атомарный claim batch при конкурентных запусках.

Случайный UUID полезен для трассировки отдельной попытки, но не определяет идентичность источника. При необходимости хранить оба поля: стабильный `batch_id` и случайный `attempt_id`.

### 1.4. Повторный Airflow retry не является безопасным

**Симптомы**

- после автоматического retry меняется количество строк;
- повторяются audit-записи, уведомления или внешние side effects;
- ручной rerun даёт другой результат.

**Причина**

Airflow гарантирует повтор выполнения задачи, а не exactly-once запись во внешнюю систему.

**Решение**

Проектировать задачи как at-least-once delivery + idempotent consumer:

- одна и та же logical date и один и тот же источник должны давать тот же `batch_id`;
- DML должен использовать `MERGE`, replace partition либо stage-and-swap;
- внешние side effects должны иметь idempotency key;
- audit должен различать logical batch и отдельные attempts.

## 2. Ошибки `dbt-core`

### 2.1. `unique_key` не уникален или содержит `NULL`

**Симптомы**

- incremental-модель создаёт дубли;
- Snowflake сообщает о недетерминированном `MERGE`;
- результат зависит от порядка входных строк.

**Причина**

Одна целевая строка совпадает с несколькими source-строками либо `NULL` не сопоставляется ожидаемым образом.

**Решение**

- определить grain модели до выбора ключа;
- добавить `unique` и `not_null` tests;
- для составного grain использовать список колонок, например `['order_id', 'order_item_id']`;
- дедуплицировать source до `MERGE`:

```sql
qualify row_number() over (
    partition by order_id
    order by updated_at desc, source_row_number desc
) = 1
```

Официальная документация: [dbt `unique_key`](https://docs.getdbt.com/reference/resource-configs/unique_key) и [Snowflake `MERGE`](https://docs.snowflake.com/en/sql-reference/sql/merge).

### 2.2. Неправильный incremental watermark

**Симптомы**

- поздние записи навсегда пропускаются;
- строки с одинаковым timestamp обрабатываются не полностью;
- каждый запуск повторно сканирует всю таблицу;
- обновления старых записей не попадают в target.

**Причина**

- фильтр использует `>` вместо безопасного перекрытия;
- watermark основан на ingestion time, который пересоздаётся при каждом full reload;
- source не предоставляет надёжный update timestamp;
- часы источника и warehouse используют разные timezone.

**Решение**

- применять lookback window и разрешать `MERGE` нейтрализовать повторы;
- хранить watermark отдельно от максимального значения бизнес-таблицы, если оно может уменьшаться;
- использовать CDC offset, object version или стабильный `updated_at`;
- обрабатывать `NULL` и пустой target через `coalesce`;
- регулярно запускать reconciliation или ограниченный backfill.

Пример:

```sql
{% if is_incremental() %}
where updated_at >= dateadd(
    hour,
    -24,
    (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
)
{% endif %}
```

Официальная документация: [dbt incremental models](https://docs.getdbt.com/docs/build/incremental-models).

### 2.3. Новая SQL-логика не пересчитала историю

**Симптомы**

- новые строки рассчитаны по новой формуле, старые — по старой;
- после релиза одна модель содержит две версии бизнес-логики;
- тесты проходят только на свежих периодах.

**Причина**

Incremental run обрабатывает только выбранный диапазон данных. Изменение SQL само по себе не пересчитывает старые строки.

**Решение**

- классифицировать изменения моделей на backward-compatible и требующие backfill;
- выполнять контролируемый `dbt run --full-refresh --select model+`;
- для больших таблиц пересчитывать затронутые partitions;
- сравнивать row counts и контрольные агрегаты до переключения consumers.

### 2.4. Изменение схемы ломает incremental-модель

**Симптомы**

- после добавления колонки target её не содержит;
- после удаления колонки dbt падает;
- старые строки получают `NULL` в новой колонке.

**Причина**

`on_schema_change` по умолчанию не выполняет полный historical backfill.

**Решение**

- явно выбрать `on_schema_change`: `fail`, `append_new_columns` или `sync_all_columns`;
- миграции типов и удаление колонок проводить отдельно;
- после добавления вычисляемой колонки планировать backfill существующих строк;
- тестировать schema contract в CI.

### 2.5. Скрытые зависимости между моделями

**Симптомы**

- dbt запускает модели не в ожидаемом порядке;
- Cosmos-граф не показывает реальную зависимость;
- модель случайно работает только потому, что нужная таблица уже существует.

**Причина**

SQL обращается к физическому имени таблицы вместо `ref()` или `source()`.

**Решение**

- все dbt-зависимости выражать через `{{ ref(...) }}` и `{{ source(...) }}`;
- для динамических ссылок использовать `-- depends_on:`;
- проверять lineage в `manifest.json` и CI.

### 2.6. Tests существуют, но не защищают бизнес-смысл

**Симптомы**

- `dbt test` зелёный, но отчёт содержит неверные суммы;
- freshness проходит для старого набора данных;
- relationships test слишком дорогой или нестабилен.

**Причина**

- проверяются только технические ограничения;
- freshness основан на времени загрузки, а не времени изменения источника;
- отсутствуют reconciliation-тесты и допустимые диапазоны метрик.

**Решение**

- сочетать `not_null`, `unique`, `relationships`, `accepted_values` и singular tests;
- сравнивать суммы, количество заказов и ключевые KPI между слоями;
- разделять ingestion freshness и business freshness;
- назначать severity `warn`/`error` осознанно;
- сохранять результаты тестов и алертить по критическим нарушениям.

## 3. Ошибки `dbt-snowflake` и Snowflake

### 3.1. Неправильная role/warehouse/database/schema

**Симптомы**

- `Object does not exist or not authorized`;
- `No active warehouse selected`;
- dbt создаёт объекты не в той базе или схеме;
- `dbt debug` работает локально, но task падает на worker.

**Причина**

В Snowflake отсутствие объекта и отсутствие прав часто выглядят одинаково. Кроме того, scheduler, worker и локальная машина могут получать разные env.

**Решение**

- использовать отдельного service user и отдельную pipeline role;
- выдавать минимальные `USAGE`, `SELECT`, `CREATE`, `INSERT`, `UPDATE`, `DELETE`;
- явно задавать role, warehouse, database и schema в каждом environment;
- запускать `dbt debug` и маленький canary query из production execution environment;
- не использовать `SYSADMIN` как постоянное production-значение по умолчанию.

### 3.2. Dev и prod используют одну схему

**Симптомы**

- разработчик случайно заменяет production-таблицу;
- параллельные CI jobs мешают друг другу;
- объекты периодически исчезают или меняют структуру.

**Причина**

Custom `generate_schema_name` возвращает одинаковые `SILVER`/`GOLD` для всех targets.

**Решение**

- включать имя environment или пользователя в dev/CI schema;
- оставлять стабильные имена только для production target;
- использовать отдельные базы или transient schemas для CI;
- запрещать dev-роли на production schemas.

### 3.3. Избыточный параллелизм и рост стоимости

**Симптомы**

- запросы долго стоят в очереди;
- увеличение `threads` не ускоряет run;
- warehouse не успевает перейти в suspend;
- стоимость резко растёт после включения Cosmos.

**Причина**

Итоговая параллельность складывается из Airflow tasks, dbt threads и нескольких DAG runs. Warehouse может стать узким местом.

**Решение**

- ограничить Airflow pools, task concurrency и `max_active_runs`;
- настраивать `DBT_THREADS` по измерениям, а не по числу CPU worker;
- включить query tags, resource monitors и budget alerts;
- следить за queue time, spill и длительностью warehouse resume;
- разделять ingestion и transformation workloads при необходимости.

### 3.4. Case-sensitive identifiers

**Симптомы**

- объект виден в UI, но dbt сообщает, что его нет;
- `MY_TABLE` и `"my_table"` ведут себя как разные объекты;
- tests работают только для части моделей.

**Причина**

Смешение quoted и unquoted identifiers.

**Решение**

- выбрать единую политику quoting;
- избегать mixed-case физических имён без необходимости;
- явно настроить quoting в dbt project, если legacy-схема требует кавычек.

### 3.5. Соединения и секреты нестабильны

**Симптомы**

- периодические authentication/OCSP/network timeout;
- credentials истекли после ротации;
- scheduler парсит DAG, но worker не может подключиться.

**Решение**

- хранить секреты в Astro Environment Manager или secrets backend;
- использовать PAT/key-pair/OAuth в соответствии с политикой организации;
- ротировать credentials без пересборки кода;
- настроить retries только для действительно временных ошибок;
- не печатать profile и env в логах.

## 4. Ошибки Astronomer Cosmos

### 4.1. `dbt ls` замедляет DAG parsing

**Симптомы**

- DAG долго появляется после deploy;
- scheduler/DAG processor потребляет много CPU и памяти;
- задачи задерживаются ещё до постановки на worker;
- в логах много повторных запусков `dbt ls`.

**Причина**

`LoadMode.DBT_LS` запускает dbt во время разбора DAG. При выключенном cache это повторяется на каждом parse cycle.

**Решение**

- для небольших проектов измерить parse time и оставить `DBT_LS`, если он приемлем;
- для крупных проектов включить Cosmos cache;
- либо генерировать versioned `manifest.json` в CI и использовать `LoadMode.DBT_MANIFEST`;
- заранее устанавливать `dbt_packages`;
- мониторить DAG processor duration.

Официальная документация: [Cosmos caching](https://astronomer.github.io/astronomer-cosmos/configuration/caching.html) и [parsing methods](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html).

### 4.2. Устаревший manifest или cache

**Симптомы**

- новая dbt-модель не появилась в Airflow;
- удалённая модель остаётся в графе;
- selectors дают неожиданный набор задач.

**Причина**

Scheduler использует артефакт от другой версии кода либо cache не был корректно инвалидирован.

**Решение**

- собирать manifest из того же Git commit, что и Docker image/DAG bundle;
- включать commit SHA в metadata артефакта;
- выполнять `dbt parse`/`dbt ls` после изменения packages, vars, profile или selectors;
- иметь документированную процедуру очистки cache.

### 4.3. Scheduler и worker видят разные dbt-файлы

**Симптомы**

- DAG содержит одну версию графа, а task выполняет другую;
- `profiles.yml` найден во время parse, но отсутствует во время task execution;
- абсолютный путь существует только в одном контейнере.

**Причина**

Airflow 3 DAG bundles, DAG-only deploy и распределённые workers не гарантируют одинаковый локальный путь без явной упаковки assets.

**Решение**

- включать dbt project, profiles template и packages в immutable image;
- проверять пути отдельно на scheduler и worker;
- не применять DAG-only deploy для изменений dbt assets, находящихся только в image;
- предпочитать versioned artifact paths вместо неявного общего диска.

### 4.4. Конфликт Python-зависимостей в `ExecutionMode.LOCAL`

**Симптомы**

- `ImportError`, `ModuleNotFoundError`;
- adapter не зарегистрирован;
- DAG import ломается сразу после обновления одного пакета;
- локально всё работает, а Astro image не собирается.

**Причина**

Airflow, Cosmos, dbt-core и adapter работают в одном Python environment и могут требовать несовместимые версии транзитивных зависимостей.

**Решение**

- обновлять Astro Runtime, Cosmos, dbt-core и dbt-snowflake совместимым набором;
- фиксировать версии и проверять `pip check`;
- выполнять smoke tests в собранном image;
- при неразрешимом конфликте изолировать dbt через virtualenv, Docker или Kubernetes execution mode.

Официальная документация: [Cosmos local execution mode](https://astronomer.github.io/astronomer-cosmos/guides/run_dbt/airflow-worker/local-execution-mode.html).

### 4.5. Слишком большой Airflow-граф

**Симптомы**

- тысячи task instances на один run;
- metadata DB и scheduler перегружены;
- UI медленно открывает Grid/Graph;
- накладные расходы превышают время коротких dbt-моделей.

**Причина**

Cosmos создаёт отдельные Airflow tasks для моделей и тестов. Это улучшает наблюдаемость, но имеет цену.

**Решение**

- группировать dbt-граф по доменам;
- объединять очень короткие модели там, где отдельный retry не нужен;
- ограничивать одновременно активные tasks;
- не запускать все несущественные tests после каждой модели;
- измерять scheduler и metadata DB до масштабирования графа.

## 5. Ошибки `chardet`

### 5.1. Результат detection принимается за гарантию

**Симптомы**

- кириллица или специальные символы повреждены;
- один и тот же формат определяется по-разному на коротких файлах;
- pipeline завершается успешно, но строки содержат символы замены.

**Причина**

`chardet` возвращает предположение и confidence, а не подтверждённый контракт. Особенно ненадёжны короткие, ASCII-only, смешанные и повреждённые файлы.

**Решение**

- предпочитать явный контракт UTF-8/UTF-8-SIG;
- декодировать с `errors="strict"`;
- при detection проверять минимальный confidence;
- неизвестную кодировку отправлять в quarantine;
- валидировать заголовки и критические текстовые поля после декодирования;
- не использовать `errors="replace"` для production ingestion без отдельного отчёта об ошибках.

Официальное описание: [chardet](https://pypi.org/project/chardet/5.2.0/).

### 5.2. Пакет установлен, но не используется

**Симптомы**

- команда считает, что кодировки уже защищены;
- dependency scanner сообщает о пакете, который не даёт функции;
- обновление лишней зависимости ломает build.

**Решение**

- подтвердить прямой или транзитивный use case;
- удалить пакет, если CSV всегда обязаны быть UTF-8-SIG;
- если автоматическое определение действительно нужно, реализовать его явно вместе с confidence threshold и тестовыми файлами разных кодировок.

## 6. Версии и сборка

### 6.1. Несогласованные версии верхнеуровневых пакетов

**Симптомы**

- `dbt --version` показывает несовместимый adapter;
- новая версия Cosmos не импортируется с текущим Airflow;
- resolver устанавливает другой набор транзитивных пакетов на следующей сборке.

**Решение**

- фиксировать совместимые версии Astro Runtime, Cosmos, dbt-core и dbt-snowflake;
- хранить lock/constraints или hashes для воспроизводимой production-сборки;
- собирать image один раз и продвигать тот же digest между средами;
- не устанавливать зависимости при старте worker;
- запускать после сборки:

```bash
pip check
dbt --version
dbt parse --project-dir /usr/local/airflow/dbt
astro dev parse
astro dev pytest
```

### 6.2. Парсинг проходит, реальное выполнение падает

**Симптомы**

- pull request зелёный;
- первый production run падает на grants, network policy, SQL dialect или реальных данных.

**Причина**

DAG integrity tests и `dbt parse` не обязаны подключаться к Snowflake и не проверяют production-sized данные.

**Решение**

Разделить проверки:

1. На каждый PR: dependency check, DAG import, dbt parse/compile, unit и schema tests.
2. В защищённом environment: `dbt debug`, canary query и build на небольшой изолированной схеме.
3. Перед production promotion: проверка grants, row-count reconciliation и оценка стоимости/плана тяжёлых запросов.

## 7. Наблюдаемость и восстановление

Минимальный production-набор:

- query tags с DAG, task, run и model identifiers;
- сохранение `manifest.json`, `run_results.json` и dbt logs для каждого deploy/run;
- audit с logical `batch_id`, `attempt_id`, checksum, row count и status;
- алерты на DAG failure, freshness, test failures, queue time и warehouse cost;
- reconciliation Bronze → Silver → Gold;
- документированный backfill и `--full-refresh` процесс;
- runbook для очистки Cosmos cache и отката image;
- проверка, что retry отдельной Cosmos task не нарушает идемпотентность downstream-моделей.

## 8. Production checklist

### Перед deploy

- [ ] `pip check` завершается успешно.
- [ ] Версии dbt core и adapter согласованы.
- [ ] `dbt parse`, `dbt compile`, DAG import и integrity tests проходят в собранном image.
- [ ] dbt project и manifest относятся к одному Git commit.
- [ ] Secrets отсутствуют в Git и image layers.
- [ ] Dev/CI/prod используют разные schemas или databases.

### Перед первым production run

- [ ] Service role имеет только необходимые grants.
- [ ] `dbt debug` проходит из worker environment.
- [ ] Warehouse concurrency и Airflow pools ограничены.
- [ ] Определены grain и unique tests всех incremental-моделей.
- [ ] Определено поведение late-arriving данных.
- [ ] Проверен recovery после падения между ingestion steps.
- [ ] Настроены audit, query tags и алерты.

### После deploy

- [ ] Row counts сопоставлены между слоями.
- [ ] Ключевые бизнес-агрегаты сверены с предыдущей версией.
- [ ] Нет неожиданных full scans и queueing.
- [ ] Cosmos DAG parse time находится в допустимом диапазоне.
- [ ] Повторный запуск одного batch не изменяет итоговое количество уникальных записей.

## 9. Применимость к текущему Olist-проекту

Текущая реализация остаётся разумной для учебного статического датасета:

- Bronze делает полный reload вместо сложного CDC;
- Silver/Gold полностью пересоздаются как `table`;
- `max_active_runs=1` исключает конкурирующие DAG runs;
- Cosmos раскрывает небольшой граф из 18 моделей, поэтому отключённый cache пока не критичен;
- явная кодировка `utf-8-sig` надёжнее автоматического detection для известного набора CSV.

Перед использованием того же дизайна для регулярно обновляемых production-данных следует в первую очередь внедрить безопасную публикацию Bronze, детерминированные batch/record identifiers, изоляцию схем и защищённые integration tests.
