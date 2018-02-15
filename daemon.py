import time
from workers.github_worker import GitStats


workers = [
    GitStats(),
]

for p in workers:
    p.run()

while True:
    for p in workers:
        p.ping()

    time.sleep(10)
