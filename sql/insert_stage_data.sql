-- Adding records that are really new
INSERT INTO STAGE_IMOVEIS_NOVOS (
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
    iptu,ST_POINT(longitude,latitude)
    date_stored,vagas,suites,
    banheiros,venda,temporada,cidade
FROM IMOVEIS_STAGE as sd
WHERE page_url not in (
    select page_url
    from STAGE_IMOVEIS_UPDATE
)

-- Adding records of updated records
INSERT INTO STAGE_IMOVEIS_UPDATE (
    page_url,descricao,imgs,
    endereco,bairro,additions,
    area_total,area_util,quartos,
    idade_do_imovel,aluguel,condominio,
    iptu,geopoint_location,
    update_date,vagas,suites,
    banheiros,venda,temporada,cidade)
SELECT sd.page_url,sd.descricao,sd.imgs,
        sd.endereco,sd.bairro,sd.additions,
        sd.area_total,sd.area_util,sd.quartos,
        sd.idade_do_imovel,sd.aluguel,
        sd.condominio,sd.iptu,ST_POINT(sd.longitude,sd.latitude),
        sd.date_stored,sd.vagas,sd.suites,
        sd.banheiros,sd.venda,sd.temporada,sd.cidade
FROM IMOVEIS_STAGE as sd
INNER JOIN IMOVEIS_ONLINE
ON sd.page_url = imoveis_online.page_url and (
    sd.descricao != imoveis_online.descricao or
    sd.imgs != imoveis_online.imgs or
    sd.endereco != imoveis_online.endereco or
    sd.bairro != imoveis_online.bairro or
    sd.additions != imoveis_online.additions or
    sd.area_total != imoveis_online.area_total or
    sd.area_util != imoveis_online.area_util or
    sd.quartos != imoveis_online.quartos or
    sd.idade_do_imovel != imoveis_online.idade_do_imovel or
    sd.aluguel != imoveis_online.aluguel or
    sd.condominio != imoveis_online.condominio or
    sd.iptu != imoveis_online.iptu or
    sd.geopoint_location != ST_POINT(imoveis_online.longitude,imoveis_online.latitude)
    sd.vagas != imoveis_online.vagas or
    sd.suites != imoveis_online.suites or
    sd.banheiros != imoveis_online.banheiros or
    sd.venda != imoveis_online.venda or
    sd.temporada != imoveis_online.temporada or
    sd.cidade != imoveis_online.cidade )

INSERT INTO STAGE_IMOVEIS_DELETE 
(
    descricao, imgs, page_url, endereco, 
    bairro, additions, area_total, area_util, 
    quartos, idade_do_imovel, aluguel,
    condominio, iptu, geopoint_location,
    date_stored, vagas, suites,
    banheiros, venda, temporada, 
    cidade, last_update, delete_date
)
SELECT  descricao, imgs, page_url, endereco, 
        bairro, additions, area_total, area_util, 
        quartos, idade_do_imovel, aluguel,
        condominio, iptu, geopoint_location,
        date_stored, vagas, suites,
        banheiros, venda, temporada, 
        cidade, last_update, NOW()
FROM imoveis_online as imvs
join tmp_off_urls TOO
on imvs.page_url = TOO.url