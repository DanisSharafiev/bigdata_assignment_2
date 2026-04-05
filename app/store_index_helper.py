import sys
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

cluster = Cluster(["cassandra-server"])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace("search_engine")

session.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        doc_id int PRIMARY KEY, title text, doc_length int)
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS vocabulary (
        term text PRIMARY KEY, df int)
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS inverted_index (
        term text, doc_id int, tf int, PRIMARY KEY (term, doc_id))
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS collection_stats (
        id int PRIMARY KEY, num_docs int, avg_dl float)
""")

ins_doc = session.prepare("INSERT INTO documents (doc_id, title, doc_length) VALUES (?, ?, ?)")
ins_vocab = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
ins_idx = session.prepare("INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)")
ins_stats = session.prepare("INSERT INTO collection_stats (id, num_docs, avg_dl) VALUES (?, ?, ?)")

batch = BatchStatement()
batch_size = 0

def flush():
    global batch, batch_size
    if batch_size > 0:
        session.execute(batch)
        batch = BatchStatement()
        batch_size = 0

def add(stmt, params):
    global batch, batch_size
    batch.add(stmt, params)
    batch_size += 1
    if batch_size >= 50:
        flush()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    kind = parts[0]

    if kind == "DOC":
        add(ins_doc, (int(parts[1]), parts[3], int(parts[2])))

    elif kind == "STATS":
        flush()
        session.execute(ins_stats, (1, int(parts[1]), float(parts[2])))

    elif kind == "TERM":
        term, df = parts[1], int(parts[2])
        add(ins_vocab, (term, df))
        for posting in parts[3].split(","):
            doc_id, tf = posting.split(":")
            add(ins_idx, (term, int(doc_id), int(tf)))

flush()
cluster.shutdown()
print("Index stored in Cassandra")
