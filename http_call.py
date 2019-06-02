import asyncio
import aiohttp
import random
from envs import headers
from time import sleep

from aiohttp import ClientOSError,ClientHttpProxyError,ClientResponseError,ClientError


async def fetch_post(session, queue_body, proxy, timeout,url):
	'''
	A assynchronous fetch for a http post request
	:param session: aiohttp session object
	:param queue_body: async queue with the bodys of the post requests
	:param proxy: http proxy to be used
	:param timeout: aiohttp object of timeout
	:return: returns a dict if has some problem or the body response if works
	'''
	body = await queue_body.get()
	try:
		async with session.post(url,headers=headers,proxy=proxy,timeout=timeout,data=body) as response:
			if response.status == 200:
				text = await response.text()
				if text is not None:
					return text
				raise Exception('Invalid text format')
			# Invalid proxy
			elif response.status == 403: # Proxy Bloqueado
				return {'url': body, 'proxy': proxy}
	except Exception as e:
		return {'url': body, 'proxy':''}


async def multiple_posts(bodys, loop, proxies,url):
	'''
	Asynchronous function for calling multiple urls for the same url but multiple bodys
	:param bodys: list of bodys for the post request
	:param loop: loop object of the async library
	:param proxies: async_queue of proxys
	:param url: fixed url of the post request
	:return: a list of the objects that failed and one that worked
	'''
	q_bodys = asyncio.Queue()
	[q_bodys.put_nowait(url) for url in bodys]

	async with aiohttp.ClientSession(loop=loop, connector=aiohttp.TCPConnector(limit=100)) as session:
		tasks = []
		for i in range(len(bodys)):
			task = asyncio.ensure_future(fetch_post(session=session, queue_body=q_bodys,
			                                        proxy=await proxies.get_proxy(), timeout=aiohttp.ClientTimeout(5),url=url))
			tasks.append(task)
		responses = await asyncio.gather(*tasks)

		successes = list(filter(lambda x: not isinstance(x, dict), responses))
		fails = list(filter(lambda x: isinstance(x,dict),responses))
		return successes,fails


def call_posts(body_list, proxy_rotator, url, loop = asyncio.get_event_loop(), data_col=None):
	'''
	A call of http post requests that always calls 200 requests if possible with proxy rotation
	:param body_list: list of bodys for the request
	:param proxy_rotator: class that uses a queue to rotate proxys
	:param loop: event loop from async io
	:param data_col: data collected from the requests
	:return: return the data collected
	'''

	if data_col is None:
		data_col = []

	# Fila de urls sempre igual a 200 para otimizar o uso de varias chamadas de proxy
	body_list, choosen_urls = body_list[200:], body_list[:200]

	# Respostas das chamadas http, retorna o corpo html ou dicionário caso sem sucesso {'proxy':XXXXXX,'url':http://...}
	responses,fails = loop.run_until_complete(multiple_posts(choosen_urls, loop, proxy_rotator,url=url))

	data_col.extend(responses)

	# Removing the proxys that failed
	[proxy_rotator.remove_value(fail_dict['proxy']) for fail_dict in fails]

	# Adding to the list the bodys that weren't able to be got
	body_list.extend([fail_dict['url'] for fail_dict in fails])
	sleep(random.randint(1, 3))


	# Urls que não tiveram sucesso
	if len(body_list) >0:
		print('%s proxys restantes' % str(len(proxy_rotator.proxys)))
		print('%s urls restantes' % len(body_list))
		return call_posts(body_list=body_list, proxy_rotator=proxy_rotator,url=url, loop=loop, data_col=data_col)
	else:
		return data_col

