UPDATE IMOVEIS_ONLINE
SET page_url = sd.page_url,
    descricao = sd.descricao,
    imgs = sd.imgs,
    endereco = sd.endereco,
    bairro = sd.bairro,
    additions = sd.additions,
    area_total = sd.area_total,
    area_util = sd.area_util,
    quartos = sd.quartos,
    idade_do_imovel = sd.idade_do_imovel,
    aluguel = sd.aluguel,
    condominio = sd.condominio,
    iptu = sd.iptu,
    geopoint_location = sd.geopoint_location,
    vagas = sd.vagas,
    suites = sd.suites,
    banheiros = sd.banheiros,
    venda = sd.venda,
    temporada = sd.temporada,
    cidade = sd.cidade,
    last_update = sd.update_date 
FROM STAGE_IMOVEIS_UPDATE as sd
where IMOVEIS_ONLINE.page_url = sd.page_url 

INSERT INTO IMOVEIS_ONLINE (
    page_url,descricao,imgs,
    endereco,bairro,additions,
    area_total,area_util,quartos,
    idade_do_imovel,aluguel,condominio,
    iptu,geopoint_location,
    date_stored,vagas,suites,
    banheiros,venda,temporada,cidade)
SELECT page_url,descricao,imgs,
    endereco,bairro,additions,
    area_total,area_util,quartos,
    idade_do_imovel,aluguel,condominio,
    iptu,geopoint_location,
    date_stored,vagas,suites,
    banheiros,venda,temporada,cidade
FROM STAGE_IMOVEIS_NOVOS as sd
