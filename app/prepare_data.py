from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
import os
import shutil
import sys


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

parquet_path = sys.argv[1] if len(sys.argv) > 1 else "/y.parquet"

if not parquet_path.startswith("hdfs://") and not parquet_path.startswith("file://"):
    parquet_path = "file://" + parquet_path

if os.path.isdir("data"):
    shutil.rmtree("data")
os.makedirs("data", exist_ok=True)

df = spark.read.parquet(parquet_path).select(["id", "title", "text"]).dropna(subset=["id", "title", "text"])
n = int(os.getenv("SAMPLE_SIZE", "200"))
total = df.count()
fraction = 1.0 if total <= n else min(1.0, (n * 1.5) / total)
df = df.sample(withReplacement=False, fraction=fraction, seed=0).limit(n)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)

spark.stop()


# df.write.csv("/index/data", sep = "\t")