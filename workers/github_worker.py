import aiohttp
import asyncio
import random
from lib.data_worker import DataWorker

HEADERS = {
	'Accept': 'application/vnd.github.v3+json',
	'User-Agent': 'saninstein',
	# 'Authorization': 'token 5a2ad16f27d4ae114e96b96ab31142175ac0d8d1'
}

BASE_URL = 'https://api.github.com/{}'


class types:
	USR = 1
	ORG = 2
	REP = 3


class GitStats(DataWorker):

	update_frequency = 60 * 10

	def __init__(
		self,
		links,
		loop=asyncio.get_event_loop()):

		self.loop = loop
		self.links = links

	def fetch_data(self):
		self.loop.run_until_complete(self._run())
		self.save(1, self.info)

	def save(self, coin_id, data):
		if data is None:
			return
		print(data)

	async def _run(self):
		info = {
			'people': set(),
			'stars': 0,
			'commits': 0,
			'issues': {
				'open': 0,
				'closed': 0
			},
			'branches': 0,
			'last_commit': None
		}
		async with asyncio.Semaphore(6, loop=self.loop):
			self.session = aiohttp.ClientSession(headers=HEADERS, loop=self.loop)

			repos = set()

			for link in self.links:
				_type, *val = await self.check_type(link)
				if _type in (types.USR, types.ORG):
					val = val[0]
					_repos = await self.get_account_repos(val)
					for rep in _repos:
						repos.add((val, rep))

					if _type == types.ORG:
						info['people'] |= await self.get_org_people(val)
				else:
					repos.add((val[0], val[1]))

			tasks = []
			for rep in repos:
				task = asyncio.ensure_future(self.get_rep_info(*rep))
				tasks.append(task)

			future = await asyncio.gather(*tasks)
			
			_time = []
			for x in future:
				info['people'] |= x['people']
				info['stars'] += x['stars']
				info['commits'] += x['commits']
				info['branches'] += x['branches']
				info['issues']['open'] += x['issues']['open'] 
				info['issues']['closed'] += x['issues']['closed']

				if x['last_commit'] is not None:
					_time.append(x['last_commit'])

			info['last_commit'] = max(_time)
			info['sum_people'] = len(info['people'])


			await self.session.close()

			self.info = info

	async def fetch(self, url):
		await asyncio.sleep(random.random() / 2)
		async with self.session.get(url) as res:
			return await res.json(), res

	async def get_rep_info(self, owner, repo):
		info = dict()
		url = f'repos/{owner}/{repo}'

		# stars
		resp_rep, _ = await self.fetch(BASE_URL.format(url))
		info['stars'] = resp_rep['stargazers_count']
		created_at = resp_rep['created_at']

		# contributors
		resp_contr, _ = await self.fetch(BASE_URL.format(url + '/stats/contributors'))
		info['people'] = {x['author']['login'] for x in resp_contr}

		# commits info
		async def _stats_get(url, per_page=50, commits=False):
			url += f'&per_page={per_page}'
			resp_json, resp = await self.fetch(BASE_URL.format(url))
			if 'link' in resp.headers:
				link = resp.headers['link']
				last_page_url = link[link.rfind('<') + 1:link.rfind('>')]
				*_, total_pages = last_page_url.split('=')
				total_pages = int(total_pages)
				resp_json, _ = await self.fetch(last_page_url)
				res = (total_pages - 1) * per_page + len(resp_json)
			else:
				res = len(resp_json)
			if commits:
				return res, resp_json[0]['commit']['author']['date'] if len(resp_json) else None
			else:
				return res

		info['commits'], info['last_commit'] = await _stats_get(url + f'/commits?since={created_at}', commits=True)

		# issues
		info['issues'] = dict()
		info['issues']['open'] = await _stats_get(url + '/issues?state=open&filter=all')
		info['issues']['closed'] = await _stats_get(url + '/issues?state=closed&filter=all')

		# branches
		resp_branch, resp = await self.fetch(BASE_URL.format(url + '/branches'))
		info['branches'] = len(resp_branch)
		return info

	async def get_org_people(self, org):
		resp, _ = await self.fetch(BASE_URL.format(f'orgs/{org}/members'))
		return {x['login'] for x in resp}

	async def get_account_repos(self, user):
		url = f'users/{user}/repos'
		resp_repos, _ = await self.fetch(BASE_URL.format(url))
		return [x['name'] for x in resp_repos]

	async def account_type(self, acc):
		resp, _ = await self.fetch(BASE_URL.format('users/' + acc))
		return types.USR if resp['type'] == 'User' else types.ORG

	async def check_type(self, link):
		gh_url = 'https://github.com/'
		if not link[:len(gh_url)] == gh_url:
			raise ValueError('Wrong url')

		_, val = link.split(gh_url)
		val = val[:-1] if val[-1] == '/' else val
		if val.count('/') == 0:
			return (await self.account_type(val), val)
		else:
			val = val.split('/')
			return (types.REP, val[0], val[1])

	
