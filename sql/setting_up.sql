alter database imoveis_db set timezone to 'America/Sao_Paulo'

-- Create a temporary TIMESTAMP column
ALTER TABLE imoveis_stage ADD COLUMN create_time_holder TIMESTAMP without time zone NULL;

-- Copy casted value over to the temporary column
UPDATE imoveis_stage SET create_time_holder = date_stored::TIMESTAMP;

-- Modify original column using the temporary column
ALTER TABLE imoveis_stage ALTER COLUMN date_stored TYPE TIMESTAMP without time zone USING create_time_holder;

-- Drop the temporary column (after examining altered column values)
ALTER TABLE imoveis_stage DROP COLUMN create_time_holder;

-- Adding the columns of date
ALTER TABLE imoveis_online ADD COLUMN last_update TIMESTAMP without time zone NULL;
 

create table  imoveis_online as 
select stage.*
from imoveis_stage stage
inner join 
(
	select max(date_stored) as maximun,page_url 
	from imoveis_stage stage
	group by stage.page_url 
) as max_date
on max_date.maximun = stage.date_stored and max_date.page_url = stage.page_url 


update imoveis_online
set last_update = maximun,
	date_stored = minimun
from
(
	select max(date_stored) as maximun, min(date_stored) as minimun,page_url 
	from imoveis_stage stage
	group by stage.page_url 
	having max(date_stored) != min(date_stored)
) as date_table
where date_table.maximun = imoveis_online.date_stored 
or date_table.page_url = imoveis_online.page_url



