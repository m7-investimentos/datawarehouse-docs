-- ==============================================================================
-- QRY-CLI-005-PRC_LOAD_FACT_CLIENTE_PERFIL_HISTORICO
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [etl, bronze_to_silver, procedure, clientes, perfil, historico]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por realizar a carga de dados da view 
           vw_fact_cliente_perfil_historico para a tabela fact_cliente_perfil_historico.
           A view realiza todos os cálculos complexos de perfil, patrimônio,
           share of wallet e categorização dos clientes.

Casos de uso:
- Carga mensal de dados de perfil de clientes
- Atualização completa da tabela fact_cliente_perfil_historico
- Parte do processo ETL bronze -> silver
- Materialização de cálculos complexos para performance

Frequência de execução: Mensal (após fechamento do mês)
Tempo médio de execução: 5-10 minutos
Volume esperado de linhas: ~2.4M registros/ano (200k clientes x 12 meses)
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros: Nenhum (full load)

Obs: A procedure realiza carga completa (TRUNCATE/INSERT), 
     não possui parâmetros de data para carga incremental.
     Futura melhoria: implementar carga incremental por data_ref.
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela de destino: [silver].[fact_cliente_perfil_historico]

Colunas carregadas:
- conta_xp_cliente: Código único do cliente
- data_ref: Data de referência (último dia do mês)
- patrimonio_declarado: Patrimônio declarado em outras instituições
- patrimonio_xp: Patrimônio na XP
- patrimonio_open_investment: Patrimônio no Open Investment
- share_of_wallet: Percentual do patrimônio na XP
- modelo_remuneracao: Commission Based ou Fee Based
- suitability: Perfil de risco do cliente
- tipo_investidor: Classificação CVM
- segmento_cliente: Segmento (apenas PJ)
- status_cliente: ATIVO ou INATIVO
- faixa_etaria: Classificação etária
- cod_assessor: Código do assessor responsável
- meses_cliente_m7: Tempo como cliente M7
- safra_cliente_m7: Coorte de entrada na M7

Granularidade: Um registro por cliente por mês
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views de origem (INPUT):
- [silver].[vw_fact_cliente_perfil_historico]: View que realiza todos os cálculos
  * Origem: Processa dados de bronze.xp_positivador, bronze.xp_rpa_clientes, 
            bronze.xp_open_investment_extrato
  * Cálculos: Share of wallet, faixa etária, tempo de cliente, etc.
  * Volume: Todos os registros históricos mensais

Tabela de destino (OUTPUT):
- [silver].[fact_cliente_perfil_historico]: Tabela fato de perfil histórico
  * Operação: TRUNCATE seguido de INSERT
  * Modo: Full load (carga completa)
  * Chave: (conta_xp_cliente, data_ref)

Pré-requisitos:
- View vw_fact_cliente_perfil_historico deve existir e estar funcional
- Tabela fact_cliente_perfil_historico deve existir
- Tabelas bronze devem estar atualizadas com dados do mês
- Usuário deve ter permissões: TRUNCATE, INSERT, SELECT
- Espaço em disco suficiente para ~2M registros
*/

-- ==============================================================================
-- 5. SCRIPT DA PROCEDURE
-- ==============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [silver].[prc_load_fact_cliente_perfil_historico]
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
        -- Limpa a tabela para carga completa
        TRUNCATE TABLE [silver].[fact_cliente_perfil_historico];
        
        -- ==============================================================================
        -- 5.3. CARGA DOS DADOS DA VIEW
        -- ==============================================================================
        -- Insere todos os dados processados pela view
        INSERT INTO [silver].[fact_cliente_perfil_historico] (
            conta_xp_cliente,
            data_ref,
            patrimonio_declarado,
            patrimonio_xp,
            patrimonio_open_investment,
            share_of_wallet,
            modelo_remuneracao,
            suitability,
            tipo_investidor,
            segmento_cliente,
            status_cliente,
            faixa_etaria,
            cod_assessor,
            meses_cliente_m7,
            safra_cliente_m7
        )
        SELECT 
            conta_xp_cliente,
            data_ref,
            patrimonio_declarado,
            patrimonio_xp,
            patrimonio_open_investment,
            share_of_wallet,
            modelo_remuneracao,
            suitability,
            tipo_investidor,
            segmento_cliente,
            status_cliente,
            faixa_etaria,
            cod_assessor,
            meses_cliente_m7,
            safra_cliente_m7
        FROM [silver].[vw_fact_cliente_perfil_historico];
        
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
- Captura e relança erro com detalhes originais

Erros comuns:
1. View não existe: Verificar se vw_fact_cliente_perfil_historico foi criada
2. Timeout: View realiza cálculos complexos, aumentar timeout se necessário
3. Espaço em disco: Verificar espaço disponível para ~2M registros
4. Dados faltantes: Verificar se bronze está atualizada
*/

-- ==============================================================================
-- 7. SCRIPTS DE VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar quantidade de registros carregados
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT conta_xp_cliente) as total_clientes,
    COUNT(DISTINCT data_ref) as total_meses,
    MIN(data_ref) as data_inicial,
    MAX(data_ref) as data_final
FROM [silver].[fact_cliente_perfil_historico];

-- Verificar distribuição por status
SELECT 
    status_cliente,
    data_ref,
    COUNT(*) as quantidade,
    SUM(patrimonio_xp) as patrimonio_total
FROM [silver].[fact_cliente_perfil_historico]
GROUP BY status_cliente, data_ref
ORDER BY data_ref DESC, status_cliente;

-- Verificar share of wallet médio por segmento
SELECT 
    segmento_cliente,
    AVG(share_of_wallet) * 100 as share_wallet_medio,
    COUNT(DISTINCT conta_xp_cliente) as qtd_clientes
FROM [silver].[fact_cliente_perfil_historico]
WHERE data_ref = (SELECT MAX(data_ref) FROM [silver].[fact_cliente_perfil_historico])
    AND segmento_cliente IS NOT NULL
GROUP BY segmento_cliente
ORDER BY share_wallet_medio DESC;
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
- Depende da view vw_fact_cliente_perfil_historico para cálculos
- View realiza cálculos complexos incluindo share of wallet e categorizações
- Performance pode variar dependendo do volume de dados históricos
- Transação garante que tabela nunca fica parcialmente carregada
- data_carga é preenchida automaticamente via DEFAULT da tabela

Arquitetura:
1. bronze.xp_positivador: Dados diários de patrimônio e perfil
2. bronze.xp_rpa_clientes: Dados de suitability e segmentação
3. bronze.xp_open_investment_extrato: Patrimônio Open Investment
4. silver.vw_fact_cliente_perfil_historico: Cálculos e transformações
5. silver.fact_cliente_perfil_historico: Dados materializados
6. Esta procedure: Materializa os cálculos da view

Possíveis melhorias:
1. Implementar carga incremental por data_ref
2. Adicionar parâmetros para reprocessar período específico
3. Implementar log de execução em tabela de controle
4. Adicionar validações de qualidade dos dados
5. Implementar notificações em caso de anomalias
6. Adicionar estatísticas de performance da carga

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
