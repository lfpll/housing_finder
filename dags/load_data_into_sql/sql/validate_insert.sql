with CT_DISTINCT AS
(
	SELECT COUNT(*) as count_distinct
	FROM 
	(
	    SELECT DISTINCT *
	    FROM stage_imoveis_novos sin2 
	    UNION
	    SELECT DISTINCT *
	    FROM stage_imoveis_update siu 
	) as tb
),
CT_NORMAL as 
(
SELECT COUNT(*) as count_normal
from
(
    SELECT  *
    FROM stage_imoveis_novos sin2 
    UNION
    SELECT  *
    FROM stage_imoveis_update siu 
)as tb1
)
select count_distinct = count_normal from CT_DISTINCT cross join ct_normal