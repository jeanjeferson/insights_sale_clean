SELECT
    CAST(vendas.datas AS DATE) AS data,
    RTRIM(vendas.emps) AS Empresa,
    SUM(CAST(vendas.totas AS DECIMAL(10,3))) AS vendas
FROM sljgdmi AS vendas WITH (NOLOCK)
LEFT JOIN sljpro p ON p.cpros = vendas.cpros
WHERE vendas.ggrus IN (
    SELECT DISTINCT A.ggrus
    FROM SLJGDMI A
    JOIN SLJGGRP B ON A.ggrus = B.codigos AND B.relgers <> 2
)
AND vendas.datas BETWEEN :date_start AND :date_end AND vendas.tipoops < 90
GROUP BY 
    CAST(vendas.datas AS DATE),
    RTRIM(vendas.emps)
ORDER BY CAST(vendas.datas AS DATE) ASC