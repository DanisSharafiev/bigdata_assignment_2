#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue
    doc_id, title, text = parts
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    dl = len(tokens)
    print(f"!META\t{doc_id}\t{dl}\t{title}")
    tf = {}
    for t in tokens:
        tf[t] = tf.get(t, 0) + 1
    for term, count in tf.items():
        print(f"{term}\t{doc_id}\t{count}")
