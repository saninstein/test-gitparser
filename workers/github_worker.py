import aiohttp
import asyncio
import logging as log
import time
import re
from . import helpers
from .exceptions import *
from lib.data_worker import DataWorker


log.basicConfig(format = u'%(filename)s[LINE:%(lineno)d] %(levelname)-8s [%(asctime)s] %(message)s', level = log.INFO)

BASE_URL = 'https://api.github.com/{}'
TOKENS_URL = 'http://saninstein.pythonanywhere.com/static/tokens.json'
PROJECTS_URL = 'http://db.xyz.hcmc.io/data/coins.json'


ITEM_PATTERN = re.compile(r'github.com/([\w\.\-\_]+)/?([\w\.\-\_]+)?/?$')


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
		self.removed_tokens = []

		self.headers = {
			'Accept': 'application/vnd.github.v3+json',
			'User-Agent': 'saninstein',
			'content-type': 'application/json'
		}

	def fetch_data(self):
		self.loop.run_until_complete(self._fetch_data())

	def save(self, coin_id, data):
		if data is None:
			return
		print(coin_id, data)

	async def _fetch_data(self):

		projects, _ = await self.fetch(PROJECTS_URL)
		await self.set_token()

		for project in projects:
			if not ('community' in project and 'github' in project['community']):
				continue

			items = project['community']['github']
			while True:
				try:
					info = {
						'sum_people': 0,
						'stars': 0,
						'commits': 0,
						'issues': {
							'open': 0,
							'closed': 0
						},
						'branches': 0,
						'last_commit': None,
						'commits_30d': 0
					}

					errors = []
					repos = set()  # set of (owner, repository)
					people = set()
					for item in items:
						try:
							_type, *val = await self.check_type(item)
						except NotFoundException:
							errors.append((item, 'Not Found'))
							continue
						if _type in (types.USR, types.ORG):  # item is not repository
							user = val[0]
							_repos = await self.get_account_repos(user)
							for rep in _repos:
								repos.add((user, rep))

							if _type == types.ORG:
								people |= await self.get_org_people(user)
						else:
							user = val[0]
							repos.add(tuple(val))
						people.add(user)

					results = await self.fetch_repos_data(repos)

					commits = set()  # (sha, date)
					# aggregate data
					for x in results:
						people |= x['people']
						info['stars'] += x['stars']
						commits |= x['commits']
						info['branches'] += x['branches']
						info['issues']['open'] += x['issues']['open']
						info['issues']['closed'] += x['issues']['closed']

					info['commits'] = len(commits)
					info['sum_people'] = len(people)

					ts = helpers.sorted_commits_timestamp(commits)
					filter_ts = helpers.get_ts_30days_ago()
					info['last_commit'] = ts[0] if ts else None
					info['commits_30d'] = len([1 for x in ts if x >= filter_ts])

					if errors:
						info['errors'] = errors

					self.save(project['id'], info)

					await self.close_session()
					break

				except RateLimitException as e:
					await self.set_token()

	async def fetch_repos_data(self, repos):
		tasks = []
		for rep in repos:
			task = asyncio.ensure_future(self.get_rep_info(*rep))
			tasks.append(task)
		return await asyncio.gather(*tasks)

	async def close_session(self):
		if self.session:
			await self.session.close()
			self.session = None

	async def set_token(self):
		self.tokens, _ = await self.fetch(TOKENS_URL)
		self.tokens = [x for x in self.tokens if x not in self.removed_tokens]

		if not self.tokens:
			raise ValueError("No valid tokens")

		tokens_info = []
		for token in self.tokens:  # fetch tokens info
			self.token = token
			try:
				token_remaining, token_reset = await self.get_rate_limit()
				tokens_info.append((token, token_remaining, token_reset))
			except BadCredentials as e:
				log.info(f'Token {self.token} is invalid')
				await self.remove_token()

		# choose token with max remaining
		token_max_remainig = max(tokens_info, key=lambda x: x[1])

		if not token_max_remainig[1] > 10:
			# choose token with min reset time
			token_min_reset_time = min(tokens_info, key=lambda x: x[2])
			self.token = token_min_reset_time[0]
			sleep_time = abs(time.time() - token_min_reset_time[2]) + 5
			log.info(f'Info: Worker awaits {sleep_time} seconds')
			await asyncio.sleep(sleep_time)
		else:
			self.token = token_max_remainig[0]
		await self.get_rate_limit()

	async def fetch(self, url, update_auth=False):
		""" fetch data from url
			Args:
				url: URL,
				safe: for unlimited requests

			Returns:
				tuple of json data and response
		"""
		if update_auth:
			self.headers['Authorization'] = f'token {self.token}'
			await self.close_session()

		if self.session is None:
			self.session = aiohttp.ClientSession(headers=self.headers)

		async with self.semaphore:
			async with self.session.get(url) as res:
				log.debug(f"{url}: [{res.status}]")
				json = await res.json()

				if url.startswith(BASE_URL[:-2]):  # request to git api
					if res.status == 401:
						raise BadCredentials()
					if res.status == 403:
						raise RateLimitException(json['message'])
					if res.status == 404:
						raise NotFoundException(json['message'])
				return json, res

	async def remove_token(self):
		self.tokens.remove(self.token)
		self.removed_tokens.append(self.token)
		self.token = None
		await self.close_session()

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
				info
		"""
		info = {
			'stars': 0,
			'people': set(),
			'commits': set(),  # (sha, date)
			'issues': {
				'open': 0,
				'closed': 0
			},
			'branches': 0
		}
		url = f'repos/{owner}/{rep}'

		# stars
		resp_rep, _ = await self.fetch(BASE_URL.format(url))
		info['stars'] = resp_rep['stargazers_count']

		# branches
		resp_branch, resp = await self.fetch(BASE_URL.format(url + '/branches'))
		branches = [x['name'] for x in resp_branch]
		info['branches'] = len(resp_branch)

		# fork check
		if resp_rep['fork']:
			commit_url = url + f"/commits?since={resp_rep['created_at']}"
		else:
			commit_url = url + '/commits?'

		if resp_rep['size']:
			# fetch and agregate data (sha of commit, author, commit date)
			# from all brenches

			COMMIT_URL = BASE_URL.format(commit_url) + '&sha={}&per_page=100'
			for branch in branches:
				commit_url = COMMIT_URL.format(branch)
				while commit_url:
					commits_part, resp = await self.fetch(commit_url)
					for commit in commits_part:
						sha = commit['sha']
						date = commit['commit']['committer']['date']
						if commit['committer']:
							info['people'].add(commit['committer']['login'])
						info['commits'].add((sha, date))
					commit_url = helpers.get_nextpage_url(resp.headers)

		# issues
		info['issues']['open'] = await self.get_count_by_pagination(url + '/issues?state=open&filter=all')
		info['issues']['closed'] = await self.get_count_by_pagination(url + '/issues?state=closed&filter=all')

		return info

	async def get_count_by_pagination(self, url):
			"""
				&per_page=1 because response may contain 'link' header,
				which include last page number. last page number == number of items
			"""
			url += '&per_page=1'
			resp_json, resp = await self.fetch(BASE_URL.format(url))

			res = helpers.get_lastpage_number(resp.headers)
			if not res:
				res = len(resp_json)
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
			await self.fetch(BASE_URL.format(f'repos/{user}/{rep}'))
			return (types.REP, user, rep)
		else:
			account_type = await self.check_user_type(user)
			return (account_type, user)


	
