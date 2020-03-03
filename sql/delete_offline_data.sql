delete 
from imoveis_online
where page_url in (
    select page_url
    from imoveis_offline
);
delete 
from tmp_off_urls;