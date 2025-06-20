-- ==============================================================================
-- QRY-CLI-003-PRC_LOAD_SILVER_DIM_CLIENTES
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.1.0
-- Última atualização: 2025-06-04
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [etl, procedure, clientes, dimensão, carga]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por realizar a carga completa da dimensão
           de clientes (dim_clientes). Combina dados do RPA com a última data
           disponível do Positivador para criar uma visão unificada de todos
           os clientes históricos.

Casos de uso:
- Carga diária da dimensão de clientes
- Atualização completa da tabela dim_clientes
- Parte do processo ETL bronze -> silver
- Unificação de dados cadastrais de múltiplas fontes

Frequência de execução: Diária
Tempo médio de execução: ~2 minutos
Volume esperado de linhas: ~200.000 registros
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros: Nenhum (full load)

Obs: A procedure realiza carga completa (TRUNCATE/INSERT), 
     não possui parâmetros de data para carga incremental.
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela de destino: [silver].[dim_clientes]

Colunas carregadas:
- cod_xp: Código único do cliente (PK)
- cpf: CPF para pessoa física
- cnpj: CNPJ para pessoa jurídica  
- nome_cliente: Nome ou razão social (usa cod_xp como fallback)
- telefone_cliente: Telefone principal
- email_cliente: E-mail principal
- data_nascimento: Data de nascimento (PF)
- sexo: Sexo (M/F)
- profissao: Profissão declarada
- data_cadastro: Data de cadastro na XP
- grupo_cliente: Grupo econômico (não mapeado atualmente)
- codigo_cliente_crm: Código CRM (não mapeado atualmente)

Retornos:
- Estatísticas de carga (total de registros, tipo de pessoa)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas de origem (INPUT):
- [bronze].[xp_positivador]: Fonte de todos os cod_xp históricos
  * Campos utilizados: cod_xp, data_nascimento, sexo, profissao, data_cadastro, data_ref
  * Volume: ~7M registros (todos os dias)
  * Uso: CTE para pegar última data de cada cliente
  
- [bronze].[xp_rpa_clientes]: Dados cadastrais do RPA
  * Campos utilizados: cod_xp, cpf_cnpj, nome_cliente, telefone_cliente, email_cliente
  * Volume: ~50.000 registros ativos
  * Uso: LEFT JOIN para enriquecer dados

Tabela de destino (OUTPUT):
- [silver].[dim_clientes]: Dimensão de clientes
  * Operação: TRUNCATE seguido de INSERT
  * Modo: Full load (carga completa)

Pré-requisitos:
- Tabela dim_clientes deve existir
- Tabelas bronze devem estar atualizadas
- Usuário deve ter permissões: TRUNCATE, INSERT, SELECT
*/

-- ==============================================================================
-- 5. SCRIPT DA PROCEDURE
-- ==============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [silver].[prc_load_silver_dim_clientes]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- ==============================================================================
    -- 5.1. VARIÁVEIS DE CONTROLE
    -- ==============================================================================
    -- Variáveis
    DECLARE @ultima_data_positivador DATE;
    DECLARE @registros_inseridos INT;
    
    -- Buscar última data do positivador
    SELECT @ultima_data_positivador = MAX(data_ref)
    FROM bronze.xp_positivador;
    
    PRINT 'Última data do positivador: ' + CONVERT(VARCHAR, @ultima_data_positivador, 103);
    
    -- ==============================================================================
    -- 5.2. TRUNCATE DA TABELA DE DESTINO
    -- ==============================================================================
    -- Limpar tabela
    TRUNCATE TABLE silver.dim_clientes;
    
    -- ==============================================================================
    -- 5.3. PROCESSAMENTO DOS DADOS COM CTEs
    -- ==============================================================================
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
    -- ==============================================================================
    -- 5.4. INSERT FINAL NA TABELA SILVER
    -- ==============================================================================
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
        
        -- Colunas não mapeadas atualmente:
        -- Código CRM (do RPA) - grupo_cliente
        -- Grupo econômico (do RPA) - codigo_cliente_crm
        
    FROM todos_clientes tc
    LEFT JOIN dados_positivador dp ON tc.cod_xp = dp.cod_xp
    LEFT JOIN bronze.xp_rpa_clientes rpa ON tc.cod_xp = CAST(rpa.cod_xp AS INT);
    
    SET @registros_inseridos = @@ROWCOUNT;
    
    -- ==============================================================================
    -- 5.5. ESTATÍSTICAS E VALIDAÇÕES
    -- ==============================================================================
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

-- Documentação da procedure
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Procedure de carga completa (TRUNCATE/INSERT) da tabela silver.dim_clientes. Combina dados do RPA com a última data disponível do Positivador. Carrega apenas as colunas existentes na tabela dim_clientes. Execução estimada: menos de 1 minuto.', 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'PROCEDURE',@level1name=N'prc_load_silver_dim_clientes'
GO

-- ==============================================================================
-- 6. TRATAMENTO DE ERROS
-- ==============================================================================
/*
Notas sobre tratamento de erros:
- A procedure não possui tratamento explícito de erros (TRY/CATCH)
- Em caso de erro, a transação será revertida automaticamente
- Logs de erro devem ser verificados no SQL Server Agent

Recomendação: Implementar bloco TRY/CATCH para melhor controle de erros
*/

-- ==============================================================================
-- 7. SCRIPTS DE VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar quantidade de registros carregados
SELECT 
    COUNT(*) as total_clientes,
    COUNT(DISTINCT cpf) as total_pf,
    COUNT(DISTINCT cnpj) as total_pj,
    COUNT(CASE WHEN cpf IS NULL AND cnpj IS NULL THEN 1 END) as sem_documento
FROM silver.dim_clientes;

-- Verificar clientes sem nome do RPA
SELECT COUNT(*) as clientes_sem_nome_rpa
FROM silver.dim_clientes
WHERE nome_cliente = CAST(cod_xp AS VARCHAR);

-- Verificar distribuição por sexo
SELECT 
    sexo,
    COUNT(*) as quantidade,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentual
FROM silver.dim_clientes
GROUP BY sexo
ORDER BY quantidade DESC;
*/

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-15 | [Nome]         | Criação inicial da procedure
1.1.0   | 2025-06-04 | [Nome]         | Corrigida para colunas existentes
                                       | Removidas colunas que não existem na tabela

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Procedure realiza TRUNCATE/INSERT (full load), não incremental
- Une TODOS os cod_xp distintos do positivador
- Usa última data disponível de cada cliente no positivador
- Nome_cliente usa cod_xp como fallback quando não existe no RPA
- CPF/CNPJ são limpos de formatação e separados em colunas distintas
- Colunas grupo_cliente e codigo_cliente_crm não são mapeadas atualmente

Lógica de processamento:
1. Identifica todos os cod_xp distintos do positivador
2. Pega última data de cada cliente no positivador
3. Enriquece com dados do RPA quando disponível
4. Usa fallbacks quando dados não existem

Possíveis melhorias:
1. Implementar carga incremental para novos clientes
2. Adicionar tratamento de erros com TRY/CATCH
3. Mapear colunas grupo_cliente e codigo_cliente_crm
4. Adicionar validações de qualidade de dados
5. Implementar log de execução em tabela de controle

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
