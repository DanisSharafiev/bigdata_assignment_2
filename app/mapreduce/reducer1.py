#!/usr/bin/env python3
import sys

n = 0
sum_dl = 0
cur = None
postings = []
meta_done = False

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    key = parts[0]

    if key == "!META":
        doc_id, dl, title = parts[1], parts[2], parts[3]
        n += 1
        sum_dl += int(dl)
        print(f"DOC\t{doc_id}\t{dl}\t{title}")
        continue

    if not meta_done:
        avg_dl = sum_dl / n if n > 0 else 0
        print(f"STATS\t{n}\t{avg_dl}")
        meta_done = True

    term = key
    if term != cur:
        if cur is not None:
            df = len(postings)
            p = ",".join(f"{d}:{t}" for d, t in postings)
            print(f"TERM\t{cur}\t{df}\t{p}")
        cur = term
        postings = []
    postings.append((parts[1], parts[2]))

if cur is not None:
    df = len(postings)
    p = ",".join(f"{d}:{t}" for d, t in postings)
    print(f"TERM\t{cur}\t{df}\t{p}")
