import glob
import os.path
import subprocess
import sys
import time
from timeit import default_timer as timer

tables = []
for csv in glob.glob(os.path.join(sys.argv[1], "*.csv")):
    tables.append(csv)

stdout_f = open("proc_stdout", "w")
stderr_f = open("proc_stderr", "w")
proc = subprocess.Popen(
    ["/bin/bash", "run.sh"],
    encoding="utf-8",
    stdin=subprocess.PIPE
)

proc.stdin.write(",".join(tables) + "\n")
proc.stdin.flush()

time.sleep(1)

with open(os.path.join(sys.argv[1], "queries.sql")) as f:
    sql = f.read()

num_queries = sql.count(";")
proc.stdin.write(f"{num_queries}\n")
proc.stdin.write(sql)
proc.stdin.flush()

start = timer()
proc.wait(timeout=60*10 + 60*5)
stop = timer()

print("Total time:", (stop - start))
