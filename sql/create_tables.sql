drop table if exists STAGE_IMOVEIS_NOVOS;
CREATE TABLE STAGE_IMOVEIS_NOVOS(
page_url	text,
descricao	text,
imgs	text[],
endereco	text,
bairro	text,
additions	text[],
area_total	float4,
area_util	float4,
quartos	int,
idade_do_imovel	text,
aluguel	float4,
condominio	float4,
iptu	float4,
geopoint_location GEOGRAPHY(POINT,4326),
date_stored	timestamp  without time zone NOT NULL
   DEFAULT (current_timestamp AT TIME ZONE  'America/Sao_paulo'),
vagas	int,
suites	int,
banheiros	int,
venda	float4,
temporada	float4,
cidade	text
);

drop table if exists STAGE_IMOVEIS_UPDATE;
CREATE TABLE STAGE_IMOVEIS_UPDATE(
page_url	text,
descricao	text,
imgs	text[],
endereco	text,
bairro	text,
additions	text[],
area_total	float4,
area_util	float4,
quartos	int,
idade_do_imovel	text,
aluguel	float4,
condominio	float4,
iptu	float4,
geopoint_location GEOGRAPHY(POINT,4326),
update_date	timestamp  without time zone NOT NULL
   DEFAULT (current_timestamp AT TIME ZONE  'America/Sao_paulo'),
vagas	int,
suites	int,
banheiros	int,
venda	float4,
temporada	float4,
cidade	text
);

drop table if exists STAGE_IMOVEIS_DELETE;
CREATE TABLE STAGE_IMOVEIS_DELETE(
page_url	text,
descricao	text,
imgs	text[],
endereco	text,
bairro	text,
additions	text[],
area_total	float4,
area_util	float4,
quartos	int,
idade_do_imovel	text,
aluguel	float4,
condominio	float4,
iptu	float4,
geopoint_location GEOGRAPHY(POINT,4326),
vagas	int,
suites	int,
banheiros	int,
venda	float4,
temporada	float4,
cidade	text,
date_stored	timestamp  without time zone NOT NULL
   DEFAULT (current_timestamp AT TIME ZONE  'America/Sao_paulo'),
last_update	timestamp  without time zone
   DEFAULT (current_timestamp AT TIME ZONE  'America/Sao_paulo'),
delete_date	timestamp  without time zone NOT NULL
   DEFAULT (current_timestamp AT TIME ZONE  'America/Sao_paulo')
);

-- drop table if exists IMOVEIS_ONLINE;
CREATE TABLE IMOVEIS_ONLINE
(
page_url	text,
descricao	text,
imgs	text[],
endereco	text,
bairro	text,
additions	text[],
area_total	float4,
area_util	float4,
quartos	int,
idade_do_imovel	text,
aluguel	float4,
condominio	float4,
iptu	float4,
geopoint_location GEOGRAPHY(POINT,4326),
vagas	int,
suites	int,
banheiros	int,
venda	float4,
temporada	float4,
cidade	text,
date_stored	timestamp  without time zone NOT NULL
   DEFAULT (current_timestamp AT TIME ZONE  'America/Sao_paulo'),
last_update	timestamp  without time zone
   DEFAULT (current_timestamp AT TIME ZONE  'America/Sao_paulo')
);