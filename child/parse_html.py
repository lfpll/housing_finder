from bs4 import BeautifulSoup
import re
from google.cloud import storage,error_reporting
import hashlib
import json
import unidecode
regexp_price = re.compile('^(\w+)(?:\s*)R\$(?:\s*)(\w+\.?(?:\w+)?\,?(?:\w+)?)')
regexp_markers = re.compile('markers=(.+?)\&')

def parse_price(soup_price_block,html_path):
	'''
	Parse block price and return a list of tuples with each value
	:param soup_price_block:
	:return:
	'''
	if len(soup_price_block) != 1:
		raise ValueError('The page has a different size on the price block {}'.format(html_path))
	price_list = soup_price_block[0].text.strip().split('\n')
	price_list = [regexp_price.search(price) for price in price_list if regexp_price.search(price)]
	price_list = [(price_regexp.group(1), float(price_regexp.group(2).replace('.', '').replace(',', '.'))) for
	              price_regexp in price_list]
	return price_list


def parse_attributes(attrs_block,html_path):
	'''
	Parse attributes
	:param attrs_block:
	:return:
	'''
	if len(attrs_block) != 1:
		raise ValueError('The page has a different size on the attributes block {}'.format(html_path))
	attrs_list = attrs_block[0].select('li')
	attrs_list = [(attrs.find('span').text.strip(), unidecode.unidecode(attrs.find('b').text.strip())) for attrs in attrs_list]
	return attrs_list


def parse_additives(addts_block,html_path):
	'''
	Parse de additions of information in list format
	:param addts_block:
	:return:
	'''
	if len(addts_block) <= 1:
		raise ValueError('The page has a different size on the aditions block {}'.format(html_path))
	audits_final_list = []
	addits_list = [additives.find_all('li') for additives in addts_block]
	[audits_final_list.extend(additive) for additive in addits_list]
	audits_final_list = [unidecode.unidecode(auditive.text.strip()) for auditive in audits_final_list]
	return audits_final_list


def parse_get_location(local_block,html_path):
	'''
	Get the latitude and longitude values
	:param local_block:
	:return:
	'''
	if len(local_block) > 1:
		raise ValueError('The page has a different size on the local block {}'.format(html_path))
	image_url = local_block[0].find('img')
	url_parse = regexp_markers.search(image_url['src']).group(1).split(',')
	lat_long = [float(float_val) for float_val in url_parse]
	lat_long = [('latitude', lat_long[0]), ('longitute', lat_long[1])]
	return lat_long


def parse_page(html_data,html_path):
	'''
	Parse the html page from imoveis web
	:param html_page: page of html
	:return: a dict format value of parsed data
	'''
	error_client = error_reporting.Client()
	try:
		soup = BeautifulSoup(html_data,'lxml')

		# Parsing the interesting data
		price_block = soup.select('div.block-price-container')
		attrs_block = soup.select('ul.section-icon-features')
		addts_block = soup.select('ul.section-bullets')
		local_block = soup.select('div.article-map')

		# Transforming data indo a format of interest
		final_tups = []
		try:
			addts_tup = parse_additives(addts_block,html_path)
			final_tups.append(('additions', addts_tup))
		except Exception as e:
			print(e,html_path)
			error_client.report_exception()
		# Transforming into a final tup
		try:
			attrs_tup = parse_attributes(attrs_block,html_path)
			final_tups.extend(attrs_tup)
		except Exception as e:
			print(e,html_path)
			error_client.report_exception()

		try:
			price_tups = parse_price(price_block,html_path)
			final_tups.extend(price_tups)
		except Exception as e:
			print(e,html_path)
			error_client.report_exception()

		try:
			local_tup = parse_get_location(local_block,html_path)
			final_tups.extend(local_tup)
		except Exception as e:
			print(e,html_path)
			error_client.report_exception()
			
		json_file = json.dumps({unidecode.unidecode(key): val for key, val in final_tups})

		m = hashlib.md5()
		m.update(html_path.encode('utf-8'))
		hex_name = str(int(m.hexdigest(), 16))
		storage_client = storage.Client()
		bucket = storage_client.get_bucket('imoveis_data')
		blob = bucket.blob('json_files/{hex_name}.json'.format(hex_name=hex_name))

		blob.upload_from_string(json_file)


	except Exception as error:
		print(error)
		error_client = error_reporting.Client()
		error_client.report_exception()


def parse_all_files():
	storage_client = storage.Client()
	bucket = storage_client.get_bucket('imoveis_data')
	blobs = bucket.list_blobs(prefix='imoveis_web/')
	for blob in blobs:
		path = blob.name.split('/')[-1]
		parse_page(html_data=blob.download_as_string(),html_path=path)

parse_all_files()