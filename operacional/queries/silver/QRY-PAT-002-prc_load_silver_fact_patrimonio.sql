-- ==============================================================================
-- QRY-PAT-002-PRC_LOAD_SILVER_FACT_PATRIMONIO
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [etl, bronze_to_silver, procedure, patrimonio, share_of_wallet]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por carregar dados de patrimônio dos clientes
           na tabela fact_patrimonio. Consolida informações de múltiplas fontes:
           - Patrimônio XP (bronze.xp_positivador)
           - Patrimônio declarado (bronze.xp_rpa_clientes)
           - Open Investment (bronze.xp_open_investment_extrato)
           Calcula automaticamente o share of wallet.

Casos de uso:
- Carga mensal de dados patrimoniais (snapshot)
- Atualização completa da tabela fact_patrimonio
- Cálculo de share of wallet para análises gerenciais
- Consolidação de patrimônio interno e externo

Frequência de execução: Mensal (D+1 após fechamento)
Tempo médio de execução: 5-10 minutos
Volume esperado de linhas: ~1M registros por mês
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros: Nenhum (full load baseado nos dados mais recentes)

Obs: A procedure realiza carga completa (TRUNCATE/INSERT).
     Futura melhoria: implementar parâmetro @data_ref para carga incremental.
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela de destino: [silver].[fact_patrimonio]

Mapeamento de campos:
- data_ref: Vem de bronze.xp_positivador.data_ref
- conta_xp_cliente: bronze.xp_positivador.cod_xp (convertido para INT)
- patrimonio_xp: bronze.xp_positivador.net_em_M
- patrimonio_declarado: bronze.xp_rpa_clientes.patrimonio (mais recente)
- share_of_wallet: Calculado como (patrimonio_xp / patrimonio_declarado)
- patrimonio_open_investment: Soma de bronze.xp_open_investment_extrato.valor_bruto

Transformações aplicadas:
- Conversão de cod_xp para INT com TRY_CAST
- Cálculo de share_of_wallet com tratamento de divisão por zero
- ROW_NUMBER() para pegar patrimônio declarado mais recente
- SUM() para totalizar Open Investment por conta
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas de origem (INPUT):
- [bronze].[xp_positivador]: Dados de patrimônio na XP
  * Campos: cod_xp, data_ref, net_em_M
  * Granularidade: Um registro por cliente/data
  * Atualização: Diária
  
- [bronze].[xp_rpa_clientes]: Dados cadastrais com patrimônio declarado
  * Campos: cod_xp, patrimonio, data_carga
  * Versionado por data_carga
  * Atualização: Sob demanda
  
- [bronze].[xp_open_investment_extrato]: Dados de Open Investment
  * Campos: cod_conta, valor_bruto
  * Granularidade: Múltiplos registros por conta
  * Atualização: Mensal

Tabela de destino (OUTPUT):
- [silver].[fact_patrimonio]: Tabela fato de patrimônio
  * Operação: TRUNCATE seguido de INSERT
  * Modo: Full load (carga completa)

Pré-requisitos:
- Tabelas bronze devem estar atualizadas
- Tabela fact_patrimonio deve existir
- Usuário deve ter permissões: TRUNCATE, INSERT, SELECT
- Database M7Medallion deve estar acessível
*/

-- ==============================================================================
-- 5. SCRIPT DA PROCEDURE
-- ==============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [silver].[prc_load_silver_fact_patrimonio]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- ==============================================================================
        -- 5.1. INÍCIO DA TRANSAÇÃO
        -- ==============================================================================
        BEGIN TRANSACTION;
        
        -- ==============================================================================
        -- 5.2. TRUNCATE DA TABELA DE DESTINO
        -- ==============================================================================
        -- Limpa a tabela antes da carga (full load)
        TRUNCATE TABLE [M7Medallion].[silver].[fact_patrimonio];
        
        -- ==============================================================================
        -- 5.3. CARGA DOS DADOS CONSOLIDADOS
        -- ==============================================================================
        INSERT INTO [M7Medallion].[silver].[fact_patrimonio] (
            data_ref,
            conta_xp_cliente,
            patrimonio_xp,
            patrimonio_declarado,
            share_of_wallet,
            patrimonio_open_investment
        )
        SELECT 
            -- Data de referência do snapshot
            pos.data_ref,
            
            -- Conta do cliente (conversão segura para INT)
            TRY_CAST(pos.cod_xp AS INT) AS conta_xp_cliente,
            
            -- Patrimônio na XP
            pos.net_em_M AS patrimonio_xp,
            
            -- Patrimônio declarado (tratamento de NULL e zero)
            CASE 
                WHEN rpa.patrimonio IS NULL OR rpa.patrimonio = 0 THEN NULL
                ELSE rpa.patrimonio
            END AS patrimonio_declarado,
            
            -- Share of wallet: percentual do patrimônio na XP
            -- Cálculo: (patrimonio_xp / patrimonio_declarado) * 100
            CASE 
                WHEN rpa.patrimonio IS NULL OR rpa.patrimonio = 0 OR pos.net_em_M IS NULL THEN NULL
                ELSE ROUND((pos.net_em_M / rpa.patrimonio) * 100, 2)
            END AS share_of_wallet,
            
            -- Patrimônio em outras instituições (Open Investment)
            oin.patrimonio_open_investment
            
        FROM 
            -- Fonte principal: Positivador com patrimônio XP
            [M7Medallion].[bronze].[xp_positivador] pos
            
        -- Join com RPA para patrimônio declarado (versão mais recente)
        LEFT JOIN (
            SELECT 
                cod_xp,
                patrimonio,
                ROW_NUMBER() OVER (
                    PARTITION BY cod_xp 
                    ORDER BY data_carga DESC
                ) as rn
            FROM [M7Medallion].[bronze].[xp_rpa_clientes]
            WHERE cod_xp IS NOT NULL
        ) rpa ON TRY_CAST(pos.cod_xp AS INT) = rpa.cod_xp AND rpa.rn = 1
        
        -- Join com Open Investment (soma por conta)
        LEFT JOIN (
            SELECT 
                cod_conta,
                SUM(valor_bruto) AS patrimonio_open_investment
            FROM [M7Medallion].[bronze].[xp_open_investment_extrato]
            WHERE cod_conta IS NOT NULL
            GROUP BY cod_conta
        ) oin ON TRY_CAST(pos.cod_xp AS INT) = oin.cod_conta
        
        WHERE 
            -- Filtros de qualidade
            pos.cod_xp IS NOT NULL
            AND pos.data_ref IS NOT NULL
            AND TRY_CAST(pos.cod_xp AS INT) IS NOT NULL;
        
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
            
        -- Captura informações do erro
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        -- Relança o erro
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- ==============================================================================
-- 6. DOCUMENTAÇÃO DA PROCEDURE
-- ==============================================================================

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Procedure para carga completa (truncate/insert) da tabela fact_patrimonio. Consolida dados de patrimônio XP, patrimônio declarado e Open Investment, calculando share of wallet automaticamente.' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'PROCEDURE',@level1name=N'prc_load_silver_fact_patrimonio'
GO

-- ==============================================================================
-- 7. SCRIPTS DE VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar quantidade de registros carregados
SELECT 
    COUNT(*) as total_clientes,
    COUNT(DISTINCT data_ref) as total_periodos,
    MIN(data_ref) as data_inicial,
    MAX(data_ref) as data_final,
    COUNT(CASE WHEN patrimonio_declarado IS NOT NULL THEN 1 END) as com_patrimonio_declarado,
    COUNT(CASE WHEN patrimonio_open_investment IS NOT NULL THEN 1 END) as com_open_investment
FROM silver.fact_patrimonio;

-- Análise de share of wallet
SELECT 
    CASE 
        WHEN share_of_wallet IS NULL THEN 'Sem dados'
        WHEN share_of_wallet = 0 THEN '0%'
        WHEN share_of_wallet <= 25 THEN '1-25%'
        WHEN share_of_wallet <= 50 THEN '26-50%'
        WHEN share_of_wallet <= 75 THEN '51-75%'
        WHEN share_of_wallet <= 100 THEN '76-100%'
        ELSE 'Acima de 100%'
    END as faixa_share_wallet,
    COUNT(*) as quantidade_clientes,
    ROUND(AVG(patrimonio_xp), 2) as patrimonio_xp_medio,
    ROUND(AVG(patrimonio_declarado), 2) as patrimonio_declarado_medio
FROM silver.fact_patrimonio
WHERE data_ref = (SELECT MAX(data_ref) FROM silver.fact_patrimonio)
GROUP BY 
    CASE 
        WHEN share_of_wallet IS NULL THEN 'Sem dados'
        WHEN share_of_wallet = 0 THEN '0%'
        WHEN share_of_wallet <= 25 THEN '1-25%'
        WHEN share_of_wallet <= 50 THEN '26-50%'
        WHEN share_of_wallet <= 75 THEN '51-75%'
        WHEN share_of_wallet <= 100 THEN '76-100%'
        ELSE 'Acima de 100%'
    END
ORDER BY 1;

-- Verificar casos de share of wallet > 100%
SELECT TOP 100
    conta_xp_cliente,
    patrimonio_xp,
    patrimonio_declarado,
    share_of_wallet,
    patrimonio_open_investment
FROM silver.fact_patrimonio
WHERE share_of_wallet > 100
ORDER BY share_of_wallet DESC;
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
- Share of wallet é calculado como percentual (0-100)
- Usa TRY_CAST para conversão segura de cod_xp para INT
- ROW_NUMBER() garante que pegamos o patrimônio declarado mais recente
- LEFT JOINs permitem clientes sem patrimônio declarado ou Open Investment
- Transação garante que tabela nunca fica parcialmente carregada

Regras de negócio aplicadas:
1. Patrimônio declarado zero é tratado como NULL
2. Share of wallet só é calculado quando temos ambos valores
3. Open Investment é a soma de todos os valores_bruto por conta
4. Apenas registros com cod_xp válido (conversível para INT) são processados

Pontos de atenção:
- Positivador deve ter dados atualizados para o período
- RPA pode ter dados desatualizados (usa o mais recente disponível)
- Open Investment pode não ter dados para todos os clientes
- Share of wallet pode ser > 100% se cliente tem mais na XP do que declarou

Possíveis melhorias:
1. Implementar carga incremental por data_ref
2. Adicionar parâmetro para reprocessar período específico
3. Incluir log de execução em tabela de controle
4. Adicionar validações de qualidade dos dados
5. Implementar cálculo alternativo de patrimônio em outras instituições
6. Adicionar tratamento para casos onde Open Investment > declarado

Performance:
- Índices nas tabelas bronze são críticos
- Consider criar estatísticas nas colunas de join
- Monitorar tempo de execução para volumes crescentes

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
