import glob
import os.path
import subprocess
import sys
import time
from timeit import default_timer as timer

setup_seconds = int(sys.argv[1])
timeout = int(sys.argv[2])
path = sys.argv[3]

tables = []
for csv in glob.glob(os.path.join(path, "*.csv")):
    tables.append(csv)

stdout_f = open("proc_stdout", "w")
stderr_f = open("proc_stderr", "w")
proc = subprocess.Popen(
    ["/bin/bash", "run.sh"],
    encoding="utf-8",
    stdin=subprocess.PIPE,
    stdout=stdout_f,
    stderr=stderr_f
)

proc.stdin.write(",".join(tables) + "\n")
proc.stdin.flush()

time.sleep(setup_seconds)

with open(os.path.join(path, "queries.sql")) as f:
    sql = f.read()

num_queries = sql.count(";")
proc.stdin.write(f"{num_queries}\n")
proc.stdin.write(sql)
proc.stdin.flush()

failed = False
start = timer()
try:
    proc.wait(timeout=timeout)
except subprocess.TimeoutExpired:
    failed = True
    print("Timeout!")
    pid = proc.pid
    proc.kill()
    time.sleep(2)
    subprocess.run(["kill", str(pid)])
    time.sleep(2)
    
stop = timer()

stdout_f.close()
stderr_f.close()

with open("proc_stdout", "r") as f:
    results = f.read().split("\n")

with open(os.path.join(path, "answers.txt")) as f:
    answers = f.read().split("\n")


for idx, (res, ans) in enumerate(zip(results, answers)):
    if res == ans:
        pass
    else:
        print("FAILED", idx)
        failed = True

if len(results) != len(answers):
    print("Incorrect number of results!")
    failed = True


print("Failed:", failed)
print("Total time:", (stop - start))
