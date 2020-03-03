INSERT INTO imoveis_online(
    page_url,descricao,imgs,
    endereco,bairro,additions,
    area_total,area_util,quartos,
    idade_do_imovel,aluguel,condominio,
    iptu,latitude,longitude,
    date_stored,vagas,suites,
    banheiros,venda,temporada,cidade)
SELECT page_url,descricao,imgs,
    endereco,bairro,additions,
    area_total,area_util,quartos,
    idade_do_imovel,aluguel,condominio,
    iptu,latitude,longitude,
    date_stored,vagas,suites,
    banheiros,venda,temporada,cidade
FROM imoveis_stage as sd
WHERE page_url not in (
    select page_url
    from imoveis_online
)
