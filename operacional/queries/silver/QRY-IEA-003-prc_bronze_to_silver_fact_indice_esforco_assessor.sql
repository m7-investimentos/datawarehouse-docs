-- ==============================================================================
-- QRY-IEA-003-PRC_BRONZE_TO_SILVER_FACT_INDICE_ESFORCO_ASSESSOR
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [etl, bronze_to_silver, procedure, indice_esforco, assessor]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por realizar a carga de dados da view 
           vw_fact_indice_esforco_assessor para a tabela fact_indice_esforco_assessor.
           A view realiza os cálculos de médias móveis e transformações necessárias
           dos dados da bronze.

Casos de uso:
- Carga mensal de métricas de performance dos assessores
- Atualização completa da tabela fact_indice_esforco_assessor
- Parte do processo ETL bronze -> silver
- Materialização de cálculos de médias móveis

Frequência de execução: Mensal (após fechamento e cálculo do IEA)
Tempo médio de execução: 1-2 minutos
Volume esperado de linhas: ~3.000 registros/mês
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros: Nenhum (full load)

Obs: A procedure realiza carga completa (TRUNCATE/INSERT), 
     não possui parâmetros de data para carga incremental.
     Futura melhoria: implementar carga incremental por ano_mes.
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela de destino: [silver].[fact_indice_esforco_assessor]

Colunas carregadas:
- ano_mes: Período de referência (YYYYMM)
- cod_assessor: Código do assessor
- indice_esforco_assessor: IEA principal
- indice_esforco_assessor_acum_3_meses: Média 3 meses
- indice_esforco_assessor_acum_6_meses: Média 6 meses
- indice_esforco_assessor_acum_12_meses: Média 12 meses
- esforco_prospeccao: Índice de prospecção
- esforco_relacionamento: Índice de relacionamento
- [métricas de prospecção]: 5 indicadores
- [métricas de relacionamento]: 4 indicadores

Retorno: Quantidade de registros inseridos
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views de origem (INPUT):
- [silver].[vw_fact_indice_esforco_assessor]: View que processa dados do IEA
  * Origem: bronze.xp_iea
  * Cálculos: Médias móveis com window functions
  * Transformações: CAST para tipos apropriados

Tabela de destino (OUTPUT):
- [silver].[fact_indice_esforco_assessor]: Tabela fato de índice de esforço
  * Operação: TRUNCATE seguido de INSERT
  * Modo: Full load (carga completa)

Pré-requisitos:
- View vw_fact_indice_esforco_assessor deve existir e estar funcional
- Tabela bronze.xp_iea deve estar atualizada com dados do mês
- Tabela fact_indice_esforco_assessor deve existir
- Usuário deve ter permissões: TRUNCATE, INSERT, SELECT
*/

-- ==============================================================================
-- 5. SCRIPT DA PROCEDURE
-- ==============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_indice_esforco_assessor]
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
        -- Limpa toda a tabela para carga completa
        TRUNCATE TABLE silver.fact_indice_esforco_assessor;
        
        -- ==============================================================================
        -- 5.3. CARGA DOS DADOS DA VIEW
        -- ==============================================================================
        -- Insere todos os dados processados pela view
        INSERT INTO silver.fact_indice_esforco_assessor (
        ano_mes,
        cod_assessor,
        indice_esforco_assessor,
        indice_esforco_assessor_acum_3_meses,
        indice_esforco_assessor_acum_6_meses,
        indice_esforco_assessor_acum_12_meses,
        esforco_prospeccao,
        esforco_relacionamento,
        prospeccao_captacao_de_novos_clientes_por_aai,
        prospeccao_atingimento_lead_starts,
        prospeccao_atingimento_habilitacoes,
        prospeccao_atingimento_conversao,
        prospeccao_atingimento_carteiras_simuladas_novos,
        relacionamento_captacao_da_base,
        relacionamento_atingimento_contas_aportarem,
        relacionamento_atingimento_ordens_enviadas,
        relacionamento_atingimento_contas_acessadas_hub
    )
    SELECT 
        ano_mes,
        cod_assessor,
        indice_esforco_assessor,
        indice_esforco_assessor_acum_3_meses,
        indice_esforco_assessor_acum_6_meses,
        indice_esforco_assessor_acum_12_meses,
        esforco_prospeccao,
        esforco_relacionamento,
        prospeccao_captacao_de_novos_clientes_por_aai,
        prospeccao_atingimento_lead_starts,
        prospeccao_atingimento_habilitacoes,
        prospeccao_atingimento_conversao,
        prospeccao_atingimento_carteiras_simuladas_novos,
        relacionamento_captacao_da_base,
        relacionamento_atingimento_contas_aportarem,
        relacionamento_atingimento_ordens_enviadas,
        relacionamento_atingimento_contas_acessadas_hub
    FROM silver.vw_fact_indice_esforco_assessor;
        
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
-- 6. TRATAMENTO DE ERROS
-- ==============================================================================
/*
Notas sobre tratamento de erros:
- Procedure possui tratamento de erros com TRY/CATCH
- Usa transação explícita para garantir atomicidade
- Em caso de erro, realiza ROLLBACK automático
- Captura e relança erro com detalhes originais

Erros comuns:
1. View não existe: Verificar se vw_fact_indice_esforco_assessor foi criada
2. Tabela bronze.xp_iea vazia: Verificar se dados do mês foram carregados
3. Tipos incompatíveis: View faz CAST correto dos tipos
*/

-- ==============================================================================
-- 7. SCRIPTS DE VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar quantidade de registros carregados
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT cod_assessor) as total_assessores,
    COUNT(DISTINCT ano_mes) as total_meses,
    MIN(ano_mes) as primeiro_mes,
    MAX(ano_mes) as ultimo_mes
FROM silver.fact_indice_esforco_assessor;

-- Verificar média do IEA por mês
SELECT 
    ano_mes,
    COUNT(DISTINCT cod_assessor) as qtd_assessores,
    AVG(indice_esforco_assessor) as iea_medio,
    MIN(indice_esforco_assessor) as iea_minimo,
    MAX(indice_esforco_assessor) as iea_maximo
FROM silver.fact_indice_esforco_assessor
GROUP BY ano_mes
ORDER BY ano_mes DESC;

-- Verificar assessores com melhor performance
SELECT TOP 10
    cod_assessor,
    AVG(indice_esforco_assessor) as iea_medio,
    AVG(esforco_prospeccao) as prospeccao_media,
    AVG(esforco_relacionamento) as relacionamento_medio
FROM silver.fact_indice_esforco_assessor
GROUP BY cod_assessor
ORDER BY iea_medio DESC;
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
- Depende da view vw_fact_indice_esforco_assessor para cálculos
- View calcula médias móveis usando window functions
- Performance depende do volume histórico na bronze.xp_iea
- Transação garante que tabela nunca fica parcialmente carregada
- data_carga é preenchida automaticamente via DEFAULT da tabela

Arquitetura:
1. bronze.xp_iea: Dados brutos do índice de esforço
2. silver.vw_fact_indice_esforco_assessor: Cálculos e transformações
3. silver.fact_indice_esforco_assessor: Dados materializados
4. Esta procedure: Materializa os cálculos da view

Possíveis melhorias:
1. Implementar carga incremental por ano_mes
2. Adicionar parâmetro para reprocessar período específico
3. Implementar log de execução em tabela de controle
4. Adicionar validações de qualidade (IEA entre 0 e N)
5. Enviar notificações após carga bem-sucedida
6. Criar snapshot histórico antes do truncate

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
