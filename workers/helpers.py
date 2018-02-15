from datetime import datetime, timedelta


def get_lastpage_number(headers):
	url = _get_url_by_rel(headers, 'last')
	if url is not None:
		page_part = url.split('&page=')[1]
		return int(page_part)
	return None


def get_nextpage_url(headers):
	return _get_url_by_rel(headers, 'next')


def _get_url_by_rel(headers, rel):
	next_rel = f'rel="{rel}"'
	if 'link' in headers and next_rel in headers['link']:
		s = headers['link']
		for c in "\n <>\t":
			s = s.replace(c, '')
		s = s.replace(',', ';')
		links = s.split(';')
		return links[links.index(next_rel) - 1]
	return None


def iso_to_seconds(t):
	utc_dt = datetime.strptime(t, '%Y-%m-%dT%H:%M:%SZ')
	return (utc_dt - datetime(1970, 1, 1)).total_seconds()


def get_ts_30days_ago():
	today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
	return (today - timedelta(days=30)).timestamp()


def sorted_commits_timestamp(commits):
	"""
		return sorted timestamps for all commit
	"""
	ts = (iso_to_seconds(x[1]) for x in commits)
	return sorted((x for x in ts), reverse=True)
