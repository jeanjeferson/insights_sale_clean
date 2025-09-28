SELECT
    CAST(vendas.datas AS DATE) AS data,
    SUM(CAST(vendas.totas AS DECIMAL(10,3))) AS vendas
FROM sljgdmi AS vendas WITH (NOLOCK)
WHERE vendas.ggrus IN (
    SELECT DISTINCT A.ggrus
    FROM SLJGDMI A
    JOIN SLJGGRP B ON A.ggrus = B.codigos AND B.relgers <> 2
)
AND vendas.datas BETWEEN :date_start AND :date_end AND vendas.tipoops < 90
GROUP BY 
    CAST(vendas.datas AS DATE)
ORDER BY CAST(vendas.datas AS DATE) ASC