import requests
from bs4 import BeautifulSoup
import json
from pymongo import MongoClient
from envs import headers,proxys
from proxy import Proxy_rotate
from http_call import call_posts

def generate_url(pagina_number):
	url = "https://www.zapimoveis.com.br/aluguel/apartamentos/pr+curitiba/?__zt=ad:tm#{%22precomaximo%22:%222147483647%22,%22parametrosautosuggest%22:[{%22Bairro%22:%22%22,%22Zona%22:%22%22,%22Cidade%22:%22CURITIBA%22,%22Agrupamento%22:%22%22,%22Estado%22:%22PR%22}],%22pagina%22:%22"+str(pagina_number)+"%22,%22ordem%22:%22Relevancia%22,%22paginaOrigem%22:%22ResultadoBusca%22,%22semente%22:%221550059717%22,%22formato%22:%22Lista%22}"
	return url

def generate_body(pagina_number):
	return "tipoOferta=1&paginaAtual="+str(pagina_number)+"&ordenacaoSelecionada=Relevancia&pathName=%2Faluguel%2Fapartamentos%2Fpr%2Bcuritiba%2F&hashFragment=%7B%22precomaximo%22%3A%222147483647%22%2C%22parametrosautosuggest%22%3A%5B%7B%22Bairro%22%3A%22%22%2C%22Zona%22%3A%22%22%2C%22Cidade%22%3A%22CURITIBA%22%2C%22Agrupamento%22%3A%22%22%2C%22Estado%22%3A%22PR%22%7D%5D%2C%22pagina%22%3A82%2C%22ordem%22%3A%22Relevancia%22%2C%22paginaOrigem%22%3A%22ResultadoBusca%22%2C%22semente%22%3A%221550059717%22%2C%22formato%22%3A%22Lista%22%7D&formato=Lista"


response = requests.get(generate_url(1), headers=headers)
soup_obj = BeautifulSoup(response.content,'lxml')
last_page = soup_obj.select_one("span.pull-right.num-of").text.split('de')[1]


proxy_rotator = Proxy_rotate(proxys)
bodys = [generate_body(pagina_number=page_num) for page_num in range(1,int(last_page)+1)]
json_resp = call_posts(body_list=bodys, proxy_rotator=proxy_rotator, url="https://www.zapimoveis.com.br/Busca/RetornarBuscaAssincrona/")

jsons_info = []
for html in json_resp:
	jsons_info.extend(json.loads(html)['Resultado']['Resultado'])

client = MongoClient()
coll = client['imoveis']['zap']
coll.insert_many(jsons_info)
