# Assignment 2: Simple Search Engine using Hadoop MapReduce

**Course:** Big Data - IU
**Deadline:** April 3
**Template repo:** https://github.com/firas-jolha/big-data-assignment2

---

## Суть задания

Реализовать простой поисковый движок с использованием **Hadoop MapReduce**, **Cassandra/ScyllaDB** и **Spark RDD**.
Движок должен: индексировать текстовые документы, ранжировать их по **BM25** и возвращать top-10 релевантных документов по запросу.

**Формула BM25:**
```
BM25(q, d) = SUM over t in q:
  log(N / df(t)) * ((k1 + 1) * tf(t,d)) / (k1 * [(1 - b) + b * dl(d) / dl_avg] + tf(t,d))
```
- k1 = 1, b = 0.75 (дефолтные гиперпараметры)
- N = кол-во документов, df(t) = кол-во документов с термом t
- tf(t,d) = частота терма в документе, dl(d) = длина документа, dl_avg = средняя длина

---

## Важные ограничения

- **НЕЛЬЗЯ использовать Pandas** или любые пакеты для single machine. Только PySpark.
- Hadoop кластер минимум 2 ноды (master + slave).
- Все запускается через **Docker Compose** (`docker compose up`).
- Формат имен документов: `<doc_id>_<doc_title>.txt` (пробелы -> `_`).

---

## Data Collection & Preparation

1. Скачать один parquet файл с Kaggle (Wikipedia dataset)
2. Извлечь минимум **1000 документов** (id, title, text) через PySpark
3. Создать .txt файлы в формате `<doc_id>_<doc_title>.txt`, положить в `/data` в HDFS
4. Подготовить данные для MapReduce: через PySpark RDD прочитать файлы из `/data`, трансформировать и сохранить в `/input/data` как одну партицию:
   ```
   <doc_id>\t<doc_title>\t<doc_text>
   ```

Дан готовый скрипт `app/prepare_data.py` как пример.

---

## [50 points + 10 extra] Indexer Tasks

### [20 points] MapReduce пайплайны
- Построить MapReduce пайплайны для индексации документов
- Вход: `/input/data`, выход: `/indexer` (одна партиция)
- Файлы: `mapper1.py`, `reducer1.py` (и далее mapper2/reducer2 если нужно)
- Промежуточные данные: `/tmp/indexer`
- **Минимально нужно хранить:** vocabulary термов, index документов, статистики для BM25
- Скрипт: **`create_index.sh`** (принимает путь к данным, по умолчанию `/input/data`)

### [25 points] Сохранение индекса в Cassandra/ScyllaDB
- Скрипт: **`store_index.sh`**
- Читает индекс из HDFS и загружает в таблицы Cassandra/ScyllaDB
- Можно использовать `cassandra-driver` или PySpark + Spark-ScyllaDB Connector
- **Минимально таблицы для:** vocabulary, index, BM25 статистики

### [5 points] Объединяющий скрипт
- Скрипт: **`index.sh`** — запускает `create_index.sh`, затем `store_index.sh`

### [10 extra points] Добавление нового документа
- Скрипт: **`add_to_index.sh`** — принимает файл из локальной FS и индексирует его
- Нужно корректно обновить индекс в Cassandra/ScyllaDB

---

## [50 points] Ranker Tasks

### [15 points] PySpark приложение
- Файл: **`query.py`**
- Читает запрос из stdin, вычисляет BM25 для всех термов по всем документам
- Возвращает top-10 релевантных документов
- Использует PySpark RDD API (DataFrame API тоже допустим)
- Читает индекс и vocabulary из Cassandra/ScyllaDB

### [35 points] Shell-обертка для запуска на YARN
- Скрипт: **`search.sh`** — принимает запрос как аргумент
- Запускает PySpark на YARN кластере (fully-distributed)
- Выводит document ids и titles top-10

---

## Report (report.pdf)

### Methodology
- Описание design choices и подходов к реализации
- Детали по всем компонентам поискового движка

### Demonstration
- Гайд как запустить репозиторий
- **FULLSCREEN скриншоты** успешного индексирования **100 документов**
- **FULLSCREEN скриншоты** успешного выполнения поиска (2-3 запроса)
- Объяснение результатов поиска и рефлексия

---

## Файлы для сдачи (минимум)

| Файл | Назначение |
|---|---|
| `mapreduce/mapper1.py` | Mapper для MR пайплайна |
| `mapreduce/reducer1.py` | Reducer для MR пайплайна |
| `create_index.sh` | Создание индекса в HDFS |
| `store_index.sh` | Загрузка индекса в Cassandra/ScyllaDB |
| `index.sh` | Полный пайплайн индексации |
| `query.py` | PySpark приложение для поиска |
| `search.sh` | Запуск поиска на YARN |
| `app.sh` | Старт сервисов + индексация + поиск |
| `start-services.sh` | Запуск сервисов Hadoop |
| `requirements.txt` | Python зависимости |
| `report.pdf` | Отчет |

---

## Как будут грейдить

1. Клонируют репо
2. `docker compose up`
3. Ожидают что сервисы стартуют и зависимости установятся автоматически
4. Могут менять `app.sh` для добавления тестовых запросов и индексации новых файлов
