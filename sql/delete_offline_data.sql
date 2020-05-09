delete 
from imoveis_online
where page_url in (
    select page_url
    from STAGE_IMOVEIS_DELETE
);

delete 
from STAGE_IMOVEIS_NOVOS;

delete 
from STAGE_IMOVEIS_UPDATE;

delete 
from IMOVEIS_STAGE;

delete 
from tmp_off_urls;