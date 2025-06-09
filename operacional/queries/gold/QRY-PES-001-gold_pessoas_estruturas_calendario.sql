CREATE VIEW [M7Medallion].[gold].[captacao_liquida_assessor] AS
SELECT 
    -- Campos da dim_calendario
    c.data_ref,
    c.dia,
    c.mes,
    c.ano,
    c.ano_mes,
    c.nome_mes,
    c.trimestre,
    c.numero_da_semana,
    c.dia_da_semana,
    c.dia_da_semana_num,
    c.tipo_dia,
    c.observacoes AS observacoes_calendario,
    
    -- Campos da dim_pessoas
    p.crm_id,
    p.nome_pessoa,
    p.cod_aai,
    p.id_avenue,
    p.id_rd_station,
    p.data_nascimento,
    p.data_inicio_vigencia,
    p.data_fim_vigencia,
    p.email_multisete,
    p.email_xp,
    p.observacoes AS observacoes_pessoa,
    
    -- Campos da fact_estrutura_pessoas
    ep.id_estrutura,
    ep.data_entrada,
    ep.data_saida,
    
    -- Campos calculados
    CASE 
        WHEN ep.data_saida IS NULL OR c.data_ref <= ep.data_saida 
        THEN 'Ativo' 
        ELSE 'Inativo' 
    END AS status_estrutura

FROM 
    [M7Medallion].[silver].[silver_dim_calendario] c
    CROSS JOIN [M7InvestimentosOLAP].[dim].[dim_pessoas] p
    LEFT JOIN [M7Medallion].[silver].[silver_fact_estrutura_pessoas] ep
        ON p.crm_id = ep.crm_id
        AND c.data_ref >= ep.data_entrada
        AND (ep.data_saida IS NULL OR c.data_ref <= ep.data_saida)
        
WHERE 
    c.ano >= 2024
    AND p.data_inicio_vigencia <= c.data_ref
    AND (p.data_fim_vigencia IS NULL OR p.data_fim_vigencia >= c.data_ref)

GO

-- Descrição da view
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'View que combina as informações de calendário, pessoas e estruturas organizacionais para análise temporal a partir de 2024', 
    @level0type=N'SCHEMA',@level0name=N'gold', 
    @level1type=N'VIEW',@level1name=N'captacao_liquida_assessor';
GO