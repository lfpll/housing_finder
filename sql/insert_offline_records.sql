INSERT INTO imoveis_offline 
(
    descricao, imgs, page_url, endereco, 
    bairro, additions, area_total, area_util, 
    quartos, idade_do_imovel, aluguel,
    condominio, iptu, latitude, longitude,
    date_stored, vagas, suites,
    banheiros, venda, temporada, cidade, last_update, delete_date
)
SELECT  descricao, imgs, page_url, endereco, 
        bairro, additions, area_total, area_util, 
        quartos, idade_do_imovel, aluguel,
        condominio, iptu, latitude, longitude,
        date_stored, vagas, suites,
        banheiros, venda, temporada, cidade, last_update, NOW()
FROM imoveis_online as imvs
join tmp_off_urls too 
on imvs.page_url = too.url
where page_url not in (select page_url from imoveis_offline)