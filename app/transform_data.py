from pyspark import SparkContext

sc = SparkContext(appName="transform_data")

files = sc.wholeTextFiles("hdfs:///data")

def parse(file_tuple):
    path, content = file_tuple
    fname = path.split("/")[-1].replace(".txt", "")
    doc_id = fname.split("_", 1)[0]
    title = fname.split("_", 1)[1].replace("_", " ") if "_" in fname else ""
    text = content.replace("\t", " ").replace("\n", " ").replace("\r", " ")
    return f"{doc_id}\t{title}\t{text}"

files.map(parse).coalesce(1).saveAsTextFile("hdfs:///input/data")

sc.stop()
