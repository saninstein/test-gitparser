import aiohttp
import asyncio
import logging as log
import random
import time
import re
from lib.data_worker import DataWorker


log.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s', level = log.INFO)

BASE_URL = 'https://api.github.com/{}'
ITEM_PATTERN = re.compile(r'https://github.com/([\w\.\-\_]+)(/([\w\.\-\_]+))?$')


class RateLimitException(Exception):
	pass


class types:
	""" Types of source items """

	USR = 1
	ORG = 2
	REP = 3


class GitStats(DataWorker):

	update_frequency = 60 * 10

	def __init__(self, loop=asyncio.get_event_loop()):
		self.loop = loop
		self.session = None
		self.remaining = None

	def fetch_data(self):
		self.loop.run_until_complete(self._fetch_data())
		self.loop.close()

	def save(self, coin_id, data):
		if data is None:
			return
		print(coin_id, data)


	async def _fetch_data(self):

		self.headers = self.headers = {
			'Accept': 'application/vnd.github.v3+json',
			'User-Agent': 'saninstein',
			'content-type': 'application/json'
		}

		projects, _ = await self.fetch('http://saninstein.pythonanywhere.com/static/projects.json', True)
		tokens, _ = await self.fetch('http://saninstein.pythonanywhere.com/static/tokens.json', True)

		await self.close_session()

		self.headers['Authorization'] = ''

		self.token = tokens[0]

		for project in projects:
			items = project['items']
			while True:
				try:
					self.remaining, _ = await self.get_rate_limit()
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

					async with asyncio.Semaphore(30):
						repos = set()  # set of (owner, repository)

						for item in items:
							_type, *val = await self.check_type(item)
							if _type in (types.USR, types.ORG):  # item is not repository
								user = val[0]
								_repos = await self.get_account_repos(user)
								for rep in _repos:
									repos.add((user, rep))

								if _type == types.ORG:
									info['people'] |= await self.get_org_people(user)
							else:
								repos.add(tuple(val))

						tasks = []
						for rep in repos:
							task = asyncio.ensure_future(self.get_rep_info(*rep))
							tasks.append(task)

						results = await asyncio.gather(*tasks)

						last_commit_time = []

						# aggregate data
						for x in results:
							info['people'] |= x['people']
							info['stars'] += x['stars']
							info['commits'] += x['commits']
							info['branches'] += x['branches']
							info['issues']['open'] += x['issues']['open']
							info['issues']['closed'] += x['issues']['closed']

							if x['last_commit'] is not None:
								last_commit_time.append(x['last_commit'])

						info['last_commit'] = max(last_commit_time)  # get latest commit timestamp
						info['sum_people'] = len(info['people'])

						info.pop('people', None)
						self.save(project['id'], info)

						await self.close_session()
						break

				except RateLimitException as e:
					# choose active token
					tokens_info = []
					for token in tokens:
						await self.close_session()
						self.token = token
						token_remaining, token_reset = await self.get_rate_limit()
						tokens_info.append((token, token_remaining, token_reset))

					await self.close_session()

					token_max_remainig = max(tokens_info, key=lambda x: x[1]) # choose token with max remaining

					if not token_max_remainig[1] > 10:
						# choose token with min reset time
						token_min_reset_time = min(tokens_info, key=lambda x: x[2])
						self.token = token_min_reset_time[0]
						sleep_time = abs(time.time() - token_min_reset_time[2]) + 5
						log.info(f'Info: Worker awaits {sleep_time} seconds')
						await asyncio.sleep(sleep_time)
					else:
						self.token = token_max_remainig[0]

	async def close_session(self):
		if self.session:
			await self.session.close()
			self.session = None


	async def fetch(self, url, safe=False):
		""" fetch data from url
			Args:
				url: URL,
				safe: for unlimited requests

			Returns:
				tuple of json data and response
		"""
		if 'Authorization' in self.headers:
			self.headers['Authorization'] = f'token {self.token}'

		if self.session is None:
			self.session = aiohttp.ClientSession(headers=self.headers)

		if not safe:
			self.remaining -= 1

		if self.remaining is not None and self.remaining < 3 and not safe:
			raise RateLimitException()

		async with self.session.get(url) as res:
			json = await res.json()
			log.debug(f"{url}: [{res.status}] {self.headers} \n{json}\n___________")
			return json, res

	async def get_rate_limit(self):
		""" returns reamainig request and token update time  """
		resp_lim, _ = await self.fetch(BASE_URL.format('rate_limit'), True)
		return resp_lim['rate']['remaining'], resp_lim['rate']['reset']

	async def get_rep_info(self, owner, rep):
		""" returns information of repository 
			
			Args:
				owner: name of the repository owner
				rep: repository name
			Returns:
				dict: {
					'stars': int,
					'people': set() - names of contributors,
					'commits': int,
					'issues': {
						'open': int,
						'closed': int
					},
					'branches': int,
					last_commit: str - latest commit timestamp, YYYY-MM-DDTHH:MM:SSZ
				}

		"""
		info = dict()
		url = f'repos/{owner}/{rep}'

		# stars
		resp_rep, _ = await self.fetch(BASE_URL.format(url))
		info['stars'] = resp_rep['stargazers_count']

		# contributors
		resp_contr, _ = await self.fetch(BASE_URL.format(url + '/stats/contributors'))
		info['people'] = {x['author']['login'] for x in resp_contr}

		if resp_rep['fork']:
			commit_url = url + f"/commits?since={resp_rep['created_at']}"
		else:
			commit_url = url + '/commits?'

		info['commits'], info['last_commit'] = await self._stats_get(commit_url, True)

		# issues
		info['issues'] = dict()
		info['issues']['open'] = await self._stats_get(url + '/issues?state=open&filter=all')
		info['issues']['closed'] = await self._stats_get(url + '/issues?state=closed&filter=all')

		# branches
		resp_branch, resp = await self.fetch(BASE_URL.format(url + '/branches'))
		info['branches'] = len(resp_branch)
		return info

	async def _stats_get(self, url, commit=False):
			"""
				the same algorithm for getting the number
				of commits and issues

				&per_page=1 because response may contain 'link' header,
				which include last page number. last page number == number of items
			"""
			url += f'&per_page=1'
			resp_json, resp = await self.fetch(BASE_URL.format(url))

			if 'link' in resp.headers:
				link = resp.headers['link']
				last_page_url = link[link.rfind('<') + 1:link.rfind('>')]
				*_, total_pages = last_page_url.split('=')
				total_pages = int(total_pages)
				res = total_pages
			else:
				res = len(resp_json)

			# get latest commit timestamp
			if commit:
				return res, resp_json[0]['commit']['author']['date'] if res else None
			else:
				return res

	async def get_org_people(self, org):
		""" returns the list of user from block people of the organization """

		resp, _ = await self.fetch(BASE_URL.format(f'orgs/{org}/members'))
		return {x['login'] for x in resp}

	async def get_account_repos(self, username):
		""" returns list user repos """

		url = f'users/{username}/repos'
		resp_repos, _ = await self.fetch(BASE_URL.format(url))
		return [x['name'] for x in resp_repos]

	async def check_user_type(self, username):
		""" account type check
			Args:
				username: str
			Return:
				types.ORG or types.USR 
		"""
		resp, _ = await self.fetch(BASE_URL.format('users/' + username))
		return types.USR if resp['type'] == 'User' else types.ORG

	async def check_type(self, item):
		""" item type check
		Args:
			item: URL string

		Returns:
			tuple:	first element - types.*,
					second - user,
					third - repos (if item is type.REP)

		"""

		user, rep = ITEM_PATTERN.findall(item)[0][0::2] # [0::2] exclude group with '/'
		if rep:
			return (types.REP, user, rep)
		else:
			account_type = await self.check_user_type(user)
			return (account_type, user)

	
