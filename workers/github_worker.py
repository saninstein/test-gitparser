import aiohttp
import asyncio
import json
import logging as log
import random
import time
import re
from lib.data_worker import DataWorker


log.basicConfig(format = u'%(filename)s[LINE:%(lineno)d] %(levelname)-8s [%(asctime)s] %(message)s', level = log.INFO)

BASE_URL = 'https://api.github.com/{}'
ITEM_PATTERN = re.compile(r'github.com/([\w\.\-\_]+)/?([\w\.\-\_]+)?/?$')


class RateLimitException(Exception):
	pass


class NotFoundException(Exception):
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
		self.semaphore = asyncio.Semaphore(5)

	def fetch_data(self):
		t = time.time()
		self.loop.run_until_complete(self._fetch_data())
		print(time.time() - t)
		self.loop.stop()
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

		projects, _ = await self.fetch('http://db.xyz.hcmc.io/data/coins.json', True)
		self.tokens, _ = await self.fetch('http://saninstein.pythonanywhere.com/static/tokens.json', True)

		await self.close_session()

		self.headers['Authorization'] = ''

		self.token = self.tokens[0]

		for project in projects:
			if not ('community' in project and 'github' in project['community']):
				continue
			if project['id'] > 20:
				break

			items = project['community']['github']
			while True:
				try:
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

					info['last_commit'] = max(last_commit_time) if last_commit_time else 0  # get latest commit timestamp
					info['sum_people'] = len(info['people'])

					if not info['sum_people']:  # if no people block and commits
						info['sum_people'] = len({rep[0] for rep in repos})
					info.pop('people', None)
					self.save(project['id'], info)

					await self.close_session()
					break

				except RateLimitException as e:
					# choose active token
					tokens_info = []
					for token in self.tokens:
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

				except NotFoundException as e:
					log.error(f"id {project['id']}: {e}")
					break

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

		async with self.semaphore:
			async with self.session.get(url) as res:
				log.debug(f"{url}: [{res.status}]")
				json = await res.json()

				if url.startswith(BASE_URL[:-2]):  # request to git api
					if res.status in (401, 403):  # 403 - rate limit exceeded, 401 - Bad credentials  
						if res.status == 401:
							log.info(f'Token {self.token} is invalid')
							self.tokens.pop(self.token)
						raise RateLimitException(res['message'])

					if res.status == 404:
						raise NotFoundException(res['message'])
				return json, res

	async def get_rate_limit(self):
		""" returns reamainig request and token update time  """
		resp_lim, _ = await self.fetch(BASE_URL.format('rate_limit'), True)
		rate_info = resp_lim['resources']['core']
		return rate_info['remaining'], rate_info['reset']

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
		if resp_rep['size']:
			resp_contr, _ = await self.fetch(BASE_URL.format(url + '/stats/contributors'))
			info['people'] = {x['author']['login'] for x in resp_contr}
		else:
			info['people'] = set()

		if resp_rep['fork']:
			commit_url = url + f"/commits?since={resp_rep['created_at']}"
		else:
			commit_url = url + '/commits?'

		if resp_rep['size']:
			info['commits'], info['last_commit'] = await self._stats_get(commit_url, True)
		else:
			info['commits'] = 0
			info['last_commit'] = None
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
		return {x['login'] for x in resp if resp}

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

		user, rep = ITEM_PATTERN.findall(item)[0]
		if rep:
			return (types.REP, user, rep)
		else:
			account_type = await self.check_user_type(user)
			return (account_type, user)

	
