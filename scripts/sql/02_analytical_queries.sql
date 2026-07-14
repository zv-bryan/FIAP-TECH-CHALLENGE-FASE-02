-- ============================================================
-- 1. Municípios abaixo da meta no ano mais recente disponível
-- ============================================================

SELECT
    ano,
    id_municipio,
    rede,
    taxa_alfabetizacao_resultado,
    meta_ano,
    gap_meta,
    status_meta
FROM tc_alfabetizacao.gold_indicador_municipio
WHERE ano = (
    SELECT MAX(ano)
    FROM tc_alfabetizacao.gold_indicador_municipio
)
  AND status_meta = 'ABAIXO_META'
ORDER BY gap_meta ASC
LIMIT 50;


-- ============================================================
-- 2. Evolução de São Paulo/SP
-- Aqui eu useiTRY_CAST para evitar problema se id_municipio estiver como string.
-- ============================================================

SELECT
    ano,
    id_municipio,
    rede,
    taxa_alfabetizacao_resultado,
    meta_ano,
    gap_meta,
    status_meta
FROM tc_alfabetizacao.gold_indicador_municipio
WHERE TRY_CAST(id_municipio AS varchar) = '3550308'
ORDER BY ano, rede;


-- ============================================================
-- 3. Ranking de UFs no ano mais recente da tabela
-- ============================================================

SELECT
    ano,
    sigla_uf,
    rede,
    taxa_alfabetizacao_resultado,
    media_portugues,
    meta_ano,
    gap_meta,
    status_meta
FROM tc_alfabetizacao.gold_indicador_uf
WHERE ano = (
    SELECT MAX(ano)
    FROM tc_alfabetizacao.gold_indicador_uf
)
ORDER BY taxa_alfabetizacao_resultado DESC;


-- ============================================================
-- 4. Resultado calculado pelos microdados
-- Essa consulta ajuda a enxergar municípios com pior resultado calculado
-- diretamente a partir dos alunos.
-- ============================================================

SELECT
    ano,
    sigla_uf,
    id_municipio,
    nome_municipio,
    rede,
    qtd_alunos,
    media_proficiencia_lp,
    taxa_alfabetizacao_microdados,
    taxa_alfabetizacao_calculada_743
FROM tc_alfabetizacao.gold_aluno_municipio
WHERE ano = (
    SELECT MAX(ano)
    FROM tc_alfabetizacao.gold_aluno_municipio
)
ORDER BY taxa_alfabetizacao_calculada_743 ASC
LIMIT 100;


-- ============================================================
-- 5. Maiores gaps negativos contra a meta
-- Aqui eu olho onde o resultado ficou mais distante da meta.
-- ============================================================

SELECT
    ano,
    id_municipio,
    rede,
    taxa_alfabetizacao_resultado,
    meta_ano,
    gap_meta,
    status_meta
FROM tc_alfabetizacao.gold_indicador_municipio
WHERE status_meta = 'ABAIXO_META'
ORDER BY gap_meta ASC
LIMIT 20;


-- ============================================================
-- 6. Média por rede de ensino
-- Ajuda a comparar se a rede municipal, estadual, pública etc.
-- têm comportamentos diferentes.
-- ============================================================

SELECT
    ano,
    rede,
    ROUND(AVG(taxa_alfabetizacao_resultado), 2) AS media_taxa_alfabetizacao,
    COUNT(*) AS qtd_registros
FROM tc_alfabetizacao.gold_indicador_municipio
GROUP BY ano, rede
ORDER BY ano, rede;


-- ============================================================
-- 7. Visão nacional com leitura de atingimento da meta
-- Deixei mais explicativo para virar uma tabela de apresentação.
-- ============================================================

SELECT
    ano,
    rede,
    taxa_alfabetizacao_resultado,
    meta_ano,
    gap_meta,
    status_meta,
    CASE
        WHEN gap_meta >= 0 THEN 'Acima ou dentro da meta'
        WHEN gap_meta BETWEEN -5 AND -0.01 THEN 'Pouco abaixo da meta'
        WHEN gap_meta BETWEEN -15 AND -5.01 THEN 'Atenção'
        ELSE 'Crítico'
    END AS faixa_situacao
FROM tc_alfabetizacao.gold_indicador_brasil
ORDER BY ano, rede;


-- ============================================================
-- 8. Resumo nacional por ano
-- Essa aqui é boa para colocar no README ou em apresentação.
-- Mostra o resultado geral médio, a meta média e o gap médio.
-- ============================================================

SELECT
    ano,
    ROUND(AVG(taxa_alfabetizacao_resultado), 2) AS media_resultado_brasil,
    ROUND(AVG(meta_ano), 2) AS media_meta_brasil,
    ROUND(AVG(gap_meta), 2) AS gap_medio_brasil,
    COUNT(*) AS qtd_redes
FROM tc_alfabetizacao.gold_indicador_brasil
GROUP BY ano
ORDER BY ano;


-- ============================================================
-- 9. Distribuição dos municípios por situação da meta
-- Essa consulta é legal porque transforma o resultado em visão gerencial.
-- ============================================================

SELECT
    ano,
    rede,
    status_meta,
    COUNT(*) AS qtd_municipios,
    ROUND(AVG(taxa_alfabetizacao_resultado), 2) AS media_resultado,
    ROUND(AVG(gap_meta), 2) AS gap_medio
FROM tc_alfabetizacao.gold_indicador_municipio
GROUP BY ano, rede, status_meta
ORDER BY ano, rede, status_meta;


-- ============================================================
-- 10. Faixas de distância da meta por município
-- Aqui criei uma classificação simples para entender o tamanho do problema.
-- ============================================================

WITH base AS (
    SELECT
        ano,
        rede,
        id_municipio,
        taxa_alfabetizacao_resultado,
        meta_ano,
        gap_meta,
        CASE
            WHEN gap_meta >= 0 THEN 'Atingiu ou superou'
            WHEN gap_meta BETWEEN -5 AND -0.01 THEN 'Até 5 p.p. abaixo'
            WHEN gap_meta BETWEEN -10 AND -5.01 THEN 'Entre 5 e 10 p.p. abaixo'
            WHEN gap_meta BETWEEN -20 AND -10.01 THEN 'Entre 10 e 20 p.p. abaixo'
            ELSE 'Mais de 20 p.p. abaixo'
        END AS faixa_gap
    FROM tc_alfabetizacao.gold_indicador_municipio
    WHERE ano = (
        SELECT MAX(ano)
        FROM tc_alfabetizacao.gold_indicador_municipio
    )
)

SELECT
    ano,
    rede,
    faixa_gap,
    COUNT(*) AS qtd_municipios,
    ROUND(AVG(taxa_alfabetizacao_resultado), 2) AS media_resultado,
    ROUND(AVG(gap_meta), 2) AS gap_medio
FROM base
GROUP BY ano, rede, faixa_gap
ORDER BY ano, rede, gap_medio;


-- ============================================================
-- 11. Comparação entre resultado oficial e resultado pelos microdados
-- Aqui eu comparo a tabela oficial municipal com a taxa recalculada a partir
-- do ponto de corte de 743 pontos.
-- ============================================================

SELECT
    m.ano,
    m.id_municipio,
    a.nome_municipio,
    m.rede,
    m.taxa_alfabetizacao_resultado AS taxa_oficial,
    a.taxa_alfabetizacao_calculada_743 AS taxa_calculada_microdados,
    ROUND(
        m.taxa_alfabetizacao_resultado - a.taxa_alfabetizacao_calculada_743,
        2
    ) AS diferenca_oficial_vs_microdados
FROM tc_alfabetizacao.gold_indicador_municipio m
INNER JOIN tc_alfabetizacao.gold_aluno_municipio a
    ON TRY_CAST(m.id_municipio AS varchar) =TRY_CAST(a.id_municipio AS varchar)
   AND m.rede = a.rede
WHERE m.ano = (
    SELECT MAX(ano)
    FROM tc_alfabetizacao.gold_indicador_municipio
)
ORDER BY ABS(m.taxa_alfabetizacao_resultado - a.taxa_alfabetizacao_calculada_743) DESC
LIMIT 50;


-- ============================================================
-- 12. Top 10 melhores e piores municípios no ano mais recente
-- Consulta simples, mas boa para demonstrar análise exploratória.
-- ============================================================

WITH base AS (
    SELECT
        ano,
        id_municipio,
        rede,
        taxa_alfabetizacao_resultado,
        meta_ano,
        gap_meta,
        status_meta,
        ROW_NUMBER() OVER (
            ORDER BY taxa_alfabetizacao_resultado DESC
        ) AS ranking_melhores,
        ROW_NUMBER() OVER (
            ORDER BY taxa_alfabetizacao_resultado ASC
        ) AS ranking_piores
    FROM tc_alfabetizacao.gold_indicador_municipio
    WHERE ano = (
        SELECT MAX(ano)
        FROM tc_alfabetizacao.gold_indicador_municipio
    )
)

SELECT
    ano,
    id_municipio,
    rede,
    taxa_alfabetizacao_resultado,
    meta_ano,
    gap_meta,
    status_meta,
    CASE
        WHEN ranking_melhores <= 10 THEN 'TOP_10_MELHORES'
        WHEN ranking_piores <= 10 THEN 'TOP_10_PIORES'
    END AS tipo_ranking
FROM base
WHERE ranking_melhores <= 10
   OR ranking_piores <= 10
ORDER BY tipo_ranking, taxa_alfabetizacao_resultado DESC;