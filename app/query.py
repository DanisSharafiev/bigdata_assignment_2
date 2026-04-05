import sys
import re
import math
from pyspark import SparkContext
from cassandra.cluster import Cluster

query = " ".join(sys.argv[1:])
terms = list(set(re.findall(r"[a-z0-9]+", query.lower())))

if not terms:
    print("No query terms")
    sys.exit(0)

cluster = Cluster(["cassandra-server"])
s = cluster.connect("search_engine")

row = s.execute("SELECT num_docs, avg_dl FROM collection_stats WHERE id = 1").one()
if row is None:
    print("Index is empty. Run index.sh first.")
    cluster.shutdown()
    sys.exit(1)
N = row.num_docs
avg_dl = row.avg_dl

vocab = {}
for t in terms:
    r = s.execute("SELECT df FROM vocabulary WHERE term = %s", (t,)).one()
    if r:
        vocab[t] = r.df

postings = []
for t in terms:
    if t not in vocab:
        continue
    for r in s.execute("SELECT doc_id, tf FROM inverted_index WHERE term = %s", (t,)):
        postings.append((t, r.doc_id, r.tf))

doc_info = {}
doc_ids = set(d for _, d, _ in postings)
for did in doc_ids:
    r = s.execute("SELECT title, doc_length FROM documents WHERE doc_id = %s", (did,)).one()
    if r:
        doc_info[did] = (r.title, r.doc_length)

cluster.shutdown()

if not postings:
    print("No results found")
    sys.exit(0)

sc = SparkContext(appName="BM25Search")

k1, b = 1.0, 0.75
bc_vocab = sc.broadcast(vocab)
bc_docs = sc.broadcast(doc_info)
bc_N = sc.broadcast(N)
bc_avg = sc.broadcast(avg_dl)

def bm25(posting):
    t, did, tf = posting
    df = bc_vocab.value[t]
    dl = bc_docs.value[did][1]
    idf = math.log(bc_N.value / df)
    score = idf * ((k1 + 1) * tf) / (k1 * ((1 - b) + b * dl / bc_avg.value) + tf)
    return (did, score)

results = sc.parallelize(postings) \
    .map(bm25) \
    .reduceByKey(lambda a, b: a + b) \
    .takeOrdered(10, key=lambda x: -x[1])

for did, score in results:
    title = doc_info.get(did, ("Unknown", 0))[0]
    print(f"{did}\t{title}\t{score:.4f}")

sc.stop()
