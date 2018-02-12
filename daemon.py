import time
from workers.github_worker import GitStats

links = [
		'https://github.com/hcmc-project',
		'https://github.com/ksanderer/hcmc-job-application',
		'https://github.com/ksanderer'
	]

workers = [
    GitStats(links),
]

for p in workers:
    p.run()

while True:
    for p in workers:
        p.ping()

    time.sleep(10)