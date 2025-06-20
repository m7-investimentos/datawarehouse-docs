-- ==============================================================================
-- QRY-CDI-003-PRC_BRONZE_TO_SILVER_FACT_CDI_HISTORICO
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [etl, bronze_to_silver, procedure, cdi, taxas]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por realizar a carga de dados da view 
           vw_fact_cdi_historico para a tabela fact_cdi_historico. A view
           realiza todos os cálculos complexos de taxas acumuladas e esta
           procedure apenas materializa os resultados.

Casos de uso:
- Carga diária de dados de CDI processados
- Atualização completa da tabela fact_cdi_historico
- Parte do processo ETL bronze -> silver
- Materialização de cálculos complexos para performance

Frequência de execução: Diária
Tempo médio de execução: 10-30 segundos
Volume esperado de linhas: ~7.000 registros (histórico completo)
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
Tabela de destino: [silver].[fact_cdi_historico]

Colunas carregadas:
- data_ref: Data de referência da taxa CDI
- ano_mes: Ano e mês no formato YYYYMM
- ano: Ano extraído
- mes_num: Número do mês
- trimestre: Identificador do trimestre (Q1-Q4)
- semestre: Identificador do semestre (S1-S2)
- taxa_cdi_dia: Taxa diária em decimal
- taxa_cdi_mes: Taxa acumulada do mês
- taxa_cdi_3_meses: Taxa acumulada 3 meses (janela móvel)
- taxa_cdi_6_meses: Taxa acumulada 6 meses (janela móvel)
- taxa_cdi_12_meses: Taxa acumulada 12 meses (janela móvel)
- taxa_cdi_trimestre: Taxa acumulada do trimestre (período fixo)
- taxa_cdi_semestre: Taxa acumulada do semestre (período fixo)
- taxa_cdi_ano: Taxa acumulada do ano (YTD)

Retorno: Quantidade de registros inseridos
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views de origem (INPUT):
- [silver].[vw_fact_cdi_historico]: View que realiza todos os cálculos
  * Origem: Processa dados de [bronze].[bc_cdi_historico]
  * Cálculos: Taxas acumuladas com juros compostos
  * Volume: Todos os registros históricos de CDI

Tabela de destino (OUTPUT):
- [silver].[fact_cdi_historico]: Tabela fato de CDI histórico
  * Operação: TRUNCATE seguido de INSERT
  * Modo: Full load (carga completa)

Pré-requisitos:
- View vw_fact_cdi_historico deve existir e estar funcional
- Tabela fact_cdi_historico deve existir
- Tabela bronze.bc_cdi_historico deve estar atualizada
- Usuário deve ter permissões: TRUNCATE, INSERT, SELECT
- Transação é utilizada para garantir atomicidade
*/

-- ==============================================================================
-- 5. SCRIPT DA PROCEDURE
-- ==============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_cdi_historico]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- ==============================================================================
        -- 5.1. INÍCIO DA TRANSAÇÃO
        -- ==============================================================================
        -- Inicia transação
        BEGIN TRANSACTION;
        
        -- ==============================================================================
        -- 5.2. TRUNCATE DA TABELA DE DESTINO
        -- ==============================================================================
        -- Limpa a tabela
        TRUNCATE TABLE [silver].[fact_cdi_historico];
        
        -- ==============================================================================
        -- 5.3. CARGA DOS DADOS DA VIEW
        -- ==============================================================================
        -- Insere dados da view
        INSERT INTO [silver].[fact_cdi_historico] (
            [data_ref],
            [ano_mes],
            [ano],
            [mes_num],
            [trimestre],
            [semestre],
            [taxa_cdi_dia],
            [taxa_cdi_mes],
            [taxa_cdi_3_meses],
            [taxa_cdi_6_meses],
            [taxa_cdi_12_meses],
            [taxa_cdi_trimestre],
            [taxa_cdi_semestre],
            [taxa_cdi_ano]
        )
        SELECT 
            [data_ref],
            [ano_mes],
            [ano],
            [mes_num],
            [trimestre],
            [semestre],
            [taxa_cdi_dia],
            [taxa_cdi_mes],
            [taxa_cdi_3_meses],
            [taxa_cdi_6_meses],
            [taxa_cdi_12_meses],
            [taxa_cdi_trimestre],
            [taxa_cdi_semestre],
            [taxa_cdi_ano]
        FROM [silver].[vw_fact_cdi_historico];
        
        -- ==============================================================================
        -- 5.4. CONFIRMAÇÃO DA TRANSAÇÃO
        -- ==============================================================================
        -- Confirma transação
        COMMIT TRANSACTION;
        
        -- Retorna quantidade de registros inseridos
        SELECT @@ROWCOUNT AS RegistrosInseridos;
        
    END TRY
    BEGIN CATCH
        -- ==============================================================================
        -- 5.5. TRATAMENTO DE ERROS
        -- ==============================================================================
        -- Em caso de erro, desfaz a transação
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        -- Relança o erro
        THROW;
    END CATCH
END
GO

-- ==============================================================================
-- 6. TRATAMENTO DE ERROS
-- ==============================================================================
/*
Notas sobre tratamento de erros:
- Procedure possui tratamento de erros com TRY/CATCH
- Usa transação explícita para garantir atomicidade
- Em caso de erro, realiza ROLLBACK automático
- Relança o erro original com THROW para logging

Erros comuns:
1. View não existe: Verificar se vw_fact_cdi_historico foi criada
2. Timeout: View realiza cálculos complexos, aumentar timeout se necessário
3. Dados faltantes: Verificar se bronze.bc_cdi_historico está atualizada
*/

-- ==============================================================================
-- 7. SCRIPTS DE VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar quantidade de registros carregados
SELECT 
    COUNT(*) as total_registros,
    MIN(data_ref) as data_inicial,
    MAX(data_ref) as data_final,
    MAX(taxa_cdi_12_meses) as cdi_anual_atual
FROM [silver].[fact_cdi_historico];

-- Verificar últimas taxas CDI
SELECT TOP 10
    data_ref,
    ano_mes,
    taxa_cdi_dia,
    taxa_cdi_mes,
    taxa_cdi_12_meses
FROM [silver].[fact_cdi_historico]
ORDER BY data_ref DESC;

-- Comparar com dados da bronze
SELECT 
    'Bronze' as origem,
    COUNT(*) as total
FROM [bronze].[bc_cdi_historico]
UNION ALL
SELECT 
    'Silver' as origem,
    COUNT(*) as total
FROM [silver].[fact_cdi_historico];
*/

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | [Nome]         | Criação inicial da procedure

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Procedure realiza TRUNCATE/INSERT (full load), não incremental
- Depende da view vw_fact_cdi_historico para cálculos
- View realiza cálculos complexos com LAG e window functions
- Performance pode variar dependendo do volume de dados históricos
- Transação garante que tabela nunca fica parcialmente carregada

Arquitetura:
1. bronze.bc_cdi_historico: Dados brutos do Banco Central
2. silver.vw_fact_cdi_historico: Cálculos de taxas acumuladas
3. silver.fact_cdi_historico: Dados materializados para performance
4. Esta procedure: Materializa os cálculos da view

Possíveis melhorias:
1. Implementar carga incremental para grandes volumes
2. Adicionar log de execução em tabela de controle
3. Implementar notificações em caso de divergências
4. Adicionar validações de qualidade dos dados
5. Considerar particionamento da tabela destino

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
