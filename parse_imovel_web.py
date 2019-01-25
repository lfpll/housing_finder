import scrapy

class ImovelSpider(scrapy.Spider):
	name='imoveis'

	start_urls = ['https://www.imovelweb.com.br/apartamentos-aluguel-curitiba-pr.html']


	def parse(self,response):
		for imovel in response.css('li.aviso'):
			yield {
				'id':imovel.css('span.aviso-fav::attr("data-aviso-id")').extract_first(),
				'aluguel':imovel.css('span.aviso-data-price-value::text').extract_first(),
				'condominio':imovel.css('span.aviso-data-expensas-value::text').extract_first(),
				'area':imovel.css('li.aviso-data-features-value::text').extract_first(),
				'local':imovel.css('span.aviso-data-location.dl-aviso-link::text').extract_first(),
				'tempo':imovel.css('li.aviso-data-extra-item.aviso-tags-publicacion.dl-aviso-link::text').extract_first()
			}
		next_page = response.css('li.pagination-action-next a::attr("href")').extract_first()
		if next_page is not None:
			yield response.follow(next_page, self.parse)