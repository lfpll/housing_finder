import scrapy
import os
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
error_log = open("error.txt",'w')
class ImovelSpider(scrapy.Spider):

	name='imoveis'

	start_urls = ['https://www.imovelweb.com.br/apartamentos-aluguel-curitiba-pr.html']

	def save_html(self,response):
		path = response.url.split('/')[-1]
		if not os.path.isfile('/home/luizfernandolobo/PycharmProjects/parse_imoveis/imoveis/'+path):
			with open('/home/luizfernandolobo/PycharmProjects/parse_imoveis/imoveis/'+path,'wb') as html_handle:
				html_handle.write(response.body)

	def error_handler(self,failure):
		error_log.write(failure.request.url+'\n')

		if failure.check(HttpError):

			# these exceptions come from HttpError spider middleware
			# you can get the non-200 response
			response = failure.value.response
			error_log.write('HttpError on %s\n'%response.url)

		elif failure.check(DNSLookupError):
			# this is the original request
			request = failure.request
			error_log.write('DNSLookupError on %s\n'%request.url)

		elif failure.check(TimeoutError, TCPTimedOutError):
			request = failure.request
			error_log.write('TimeoutError on %s\n'% request.url)

	def parse(self,response):
		for imovel in response.css('li.aviso'):
			yield {
				'id':imovel.css('span.aviso-fav::attr("data-aviso-id")').extract_first(),
				'aluguel':imovel.css('span.aviso-data-price-value::text').extract_first(),
				'condominio':imovel.css('span.aviso-data-expensas-value::text').extract_first(),
				'area':imovel.css('li.aviso-data-features-value::text').extract_first(),

				'local':imovel.css('span.aviso-data-location.dl-aviso-link::text').extract_first(),
				'tempo_anuncio':imovel.css('li.aviso-data-extra-item.aviso-tags-publicacion.dl-aviso-link::text').extract_first(),
				'url':'https://www.imovelweb.com.br'+imovel.css('a.dl-aviso-a::attr("href")').extract_first()
			}
			yield scrapy.http.Request('https://www.imovelweb.com.br'+imovel.css('a.dl-aviso-a::attr("href")').extract_first(),errback=self.error_handler,callback=self.save_html)

		next_page = response.css('li.pagination-action-next a::attr("href")',).extract_first()
		if next_page is not None:
			yield response.follow(next_page, self.parse)

