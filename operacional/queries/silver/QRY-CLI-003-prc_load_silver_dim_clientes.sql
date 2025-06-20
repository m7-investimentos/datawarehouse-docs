SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =====================================================
-- PROCEDURE: CARGA DA TABELA SILVER.DIM_CLIENTES
-- Nome: prc_load_silver_dim_clientes
-- Data: 2025-06-04
-- Versão: 1.1 - Corrigida para colunas existentes
-- =====================================================

CREATE PROCEDURE [silver].[prc_load_silver_dim_clientes]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Variáveis
    DECLARE @ultima_data_positivador DATE;
    DECLARE @registros_inseridos INT;
    
    -- Buscar última data do positivador
    SELECT @ultima_data_positivador = MAX(data_ref)
    FROM bronze.xp_positivador;
    
    PRINT 'Última data do positivador: ' + CONVERT(VARCHAR, @ultima_data_positivador, 103);
    
    -- Limpar tabela
    TRUNCATE TABLE silver.dim_clientes;
    
    -- CTEs para organizar os dados
    WITH 
    -- Todos os clientes históricos
    todos_clientes AS (
        SELECT DISTINCT CAST(cod_xp AS INT) as cod_xp
        FROM bronze.xp_positivador
    ),
    -- Dados mais recentes de cada cliente no positivador
    dados_positivador AS (
        SELECT *
        FROM (
            SELECT 
                CAST(cod_xp AS INT) as cod_xp,
                data_nascimento,
                sexo,
                profissao,
                data_cadastro,
                data_ref,
                ROW_NUMBER() OVER (PARTITION BY cod_xp ORDER BY data_ref DESC) as rn
            FROM bronze.xp_positivador
        ) t
        WHERE rn = 1
    )
    -- INSERT principal - APENAS COLUNAS EXISTENTES
    INSERT INTO silver.dim_clientes (
        cod_xp,
        cpf,
        cnpj,
        nome_cliente,
        telefone_cliente,
        email_cliente,
        data_nascimento,
        sexo,
        profissao,
        data_cadastro
    )
    SELECT 
        -- Identificação
        tc.cod_xp,
        
        -- CPF ou CNPJ (separados)
        CASE 
            WHEN LEN(REPLACE(REPLACE(REPLACE(rpa.cpf_cnpj, '.', ''), '-', ''), '/', '')) = 11 
            THEN REPLACE(REPLACE(REPLACE(rpa.cpf_cnpj, '.', ''), '-', ''), '/', '')
            ELSE NULL 
        END as cpf,
        
        CASE 
            WHEN LEN(REPLACE(REPLACE(REPLACE(rpa.cpf_cnpj, '.', ''), '-', ''), '/', '')) = 14 
            THEN REPLACE(REPLACE(REPLACE(rpa.cpf_cnpj, '.', ''), '-', ''), '/', '')
            ELSE NULL 
        END as cnpj,
        
        -- Nome (do RPA ou usa cod_xp)
        ISNULL(rpa.nome_cliente, CAST(tc.cod_xp AS VARCHAR)) as nome_cliente,
        
        -- Dados do RPA
        rpa.telefone_cliente,
        rpa.email_cliente,
        
        -- Dados do Positivador
        dp.data_nascimento,
        UPPER(dp.sexo) as sexo,
        dp.profissao,
        dp.data_cadastro
        
        -- Código CRM (do RPA)
       
        -- Grupo econômico (do RPA)
        
    FROM todos_clientes tc
    LEFT JOIN dados_positivador dp ON tc.cod_xp = dp.cod_xp
    LEFT JOIN bronze.xp_rpa_clientes rpa ON tc.cod_xp = CAST(rpa.cod_xp AS INT);
    
    SET @registros_inseridos = @@ROWCOUNT;
    
    -- Estatísticas básicas
    PRINT '';
    PRINT 'Total de registros inseridos: ' + CAST(@registros_inseridos AS VARCHAR);
    PRINT '';
    
    -- Resumo por tipo de pessoa
    SELECT 
        CASE 
            WHEN cpf IS NOT NULL THEN 'Pessoa Física'
            WHEN cnpj IS NOT NULL THEN 'Pessoa Jurídica'
            ELSE 'Sem CPF/CNPJ'
        END as tipo_pessoa,
        COUNT(*) as quantidade
    FROM silver.dim_clientes
    GROUP BY 
        CASE 
            WHEN cpf IS NOT NULL THEN 'Pessoa Física'
            WHEN cnpj IS NOT NULL THEN 'Pessoa Jurídica'
            ELSE 'Sem CPF/CNPJ'
        END
    ORDER BY quantidade DESC;
    
    PRINT 'Procedure executada com sucesso!';
    
END;
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Procedure de carga completa (TRUNCATE/INSERT) da tabela silver.dim_clientes. Combina dados do RPA com a última data disponível do Positivador. Carrega apenas as colunas existentes na tabela dim_clientes. Execução estimada: menos de 1 minuto.' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'PROCEDURE',@level1name=N'prc_load_silver_dim_clientes'
GO
