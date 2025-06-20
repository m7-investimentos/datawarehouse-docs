-- ==============================================================================
-- QRY-NPS-004-PRC_BRONZE_TO_SILVER_FACT_NPS_RESPOSTAS_ENVIOS_ANIVERSARIO
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [etl, bronze_to_silver, procedure, nps, pesquisa]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por realizar a carga de dados da view 
           vw_nps_respostas_envios_aniversario para a tabela fact_nps_respostas_envios_aniversario.
           A view consolida dados de envios e respostas de pesquisas NPS,
           calculando classificações e unindo informações.

Casos de uso:
- Carga periódica de dados de pesquisas NPS
- Atualização completa da tabela fact_nps_respostas_envios_aniversario
- Parte do processo ETL bronze -> silver
- Consolidação de envios e respostas em uma única tabela

Frequência de execução: Mensal ou conforme chegada de novos dados
Tempo médio de execução: 1-2 minutos
Volume esperado de linhas: ~4.000 registros/mês
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros: Nenhum (full load)

Obs: A procedure realiza carga completa (TRUNCATE/INSERT), 
     não possui parâmetros de data para carga incremental.
     Futura melhoria: implementar carga incremental por survey_id ou data.
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela de destino: [silver].[fact_nps_respostas_envios_aniversario]

Mapeamento de campos (view -> tabela):
- survey_id -> survey_id
- customer_id -> conta_xp_cliente
- cod_assessor -> cod_assessor
- data_entrega -> data_entrega
- data_resposta -> data_resposta
- survey_start_date -> data_inicio_survey
- survey_status -> survey_status
- invitation_opened -> convite_aberto
- xp_aniversario_nps_assessor -> nps_assessor
- xp_aniversario_nps_xp -> nps_xp
- xp_aniversario_recomendaria_assessor -> recomendaria_assessor
- classificacao_nps_assessor -> classificacao_nps_assessor
- classificacao_nps_xp -> classificacao_nps_xp
- xp_aniversario_comentario_assessor -> comentario_assessor
- xp_aniversario_comentario_xp -> comentario_xp
- xp_razao_nps -> razao_nps
- xp_aniversario_razao_nps_assessor -> razao_nps_assessor
- xp_aniversario_razao_nps_xp -> razao_nps_xp
- topics_tagged_original -> topicos_relevantes
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views de origem (INPUT):
- [silver].[vw_nps_respostas_envios_aniversario]: View que consolida dados NPS
  * Une bronze.xp_nps_respostas com bronze.xp_nps_envios
  * Calcula classificações (Promotor/Neutro/Detrator)
  * Pega sempre a versão mais recente de cada survey_id
  * Volume: Todos registros únicos de pesquisas

Tabela de destino (OUTPUT):
- [silver].[fact_nps_respostas_envios_aniversario]: Tabela fato de NPS
  * Operação: TRUNCATE seguido de INSERT
  * Modo: Full load (carga completa)
  * Chave: survey_id

Pré-requisitos:
- View vw_nps_respostas_envios_aniversario deve existir e estar funcional
- Tabelas bronze.xp_nps_respostas e bronze.xp_nps_envios devem estar atualizadas
- Tabela fact_nps_respostas_envios_aniversario deve existir
- Usuário deve ter permissões: TRUNCATE, INSERT, SELECT
*/

-- ==============================================================================
-- 5. SCRIPT DA PROCEDURE
-- ==============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_nps_respostas_envios_aniversario]
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
        -- Limpa a tabela antes da carga
        TRUNCATE TABLE [silver].[fact_nps_respostas_envios_aniversario];
        
        -- ==============================================================================
        -- 5.3. CARGA DOS DADOS DA VIEW
        -- ==============================================================================
        -- Insere dados da view para a tabela fato com mapeamento de campos
    INSERT INTO [silver].[fact_nps_respostas_envios_aniversario]
    (
        [survey_id],
        [conta_xp_cliente],
        [cod_assessor],
        [data_entrega],
        [data_resposta],
        [data_inicio_survey],
        [survey_status],
        [convite_aberto],
        [nps_assessor],
        [nps_xp],
        [recomendaria_assessor],
        [classificacao_nps_assessor],
        [classificacao_nps_xp],
        [comentario_assessor],
        [comentario_xp],
        [razao_nps],
        [razao_nps_assessor],
        [razao_nps_xp],
        [topicos_relevantes]
    )
    SELECT 
        [survey_id],
        [customer_id] AS [conta_xp_cliente],
        [cod_assessor],
        [data_entrega],
        [data_resposta],
        [survey_start_date] AS [data_inicio_survey],
        [survey_status],
        [invitation_opened] AS [convite_aberto],
        [xp_aniversario_nps_assessor] AS [nps_assessor],
        [xp_aniversario_nps_xp] AS [nps_xp],
        [xp_aniversario_recomendaria_assessor] AS [recomendaria_assessor],
        [classificacao_nps_assessor],
        [classificacao_nps_xp],
        [xp_aniversario_comentario_assessor] AS [comentario_assessor],
        [xp_aniversario_comentario_xp] AS [comentario_xp],
        [xp_razao_nps] AS [razao_nps],
        [xp_aniversario_razao_nps_assessor] AS [razao_nps_assessor],
        [xp_aniversario_razao_nps_xp] AS [razao_nps_xp],
        [topics_tagged_original] AS [topicos_relevantes]
    FROM [silver].[vw_nps_respostas_envios_aniversario];
        
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
    @value=N'Procedure para carga completa (truncate/insert) da tabela fact_nps_respostas_envios_aniversario a partir da view vw_nps_respostas_envios_aniversario. Consolida dados de envios e respostas de pesquisas NPS.' , 
    @level0type=N'SCHEMA',@level0name=N'silver', 
    @level1type=N'PROCEDURE',@level1name=N'prc_bronze_to_silver_fact_nps_respostas_envios_aniversario'
GO

-- ==============================================================================
-- 7. SCRIPTS DE VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar quantidade de registros carregados
SELECT 
    COUNT(*) as total_pesquisas,
    COUNT(DISTINCT cod_assessor) as total_assessores,
    COUNT(CASE WHEN survey_status = 'completed' THEN 1 END) as pesquisas_respondidas,
    ROUND(100.0 * COUNT(CASE WHEN survey_status = 'completed' THEN 1 END) / COUNT(*), 2) as taxa_resposta
FROM silver.fact_nps_respostas_envios_aniversario;

-- Verificar NPS por classificação
SELECT 
    classificacao_nps_assessor,
    COUNT(*) as quantidade,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentual
FROM silver.fact_nps_respostas_envios_aniversario
WHERE classificacao_nps_assessor IS NOT NULL
GROUP BY classificacao_nps_assessor;

-- Calcular NPS score dos assessores
WITH nps_calc AS (
    SELECT 
        cod_assessor,
        COUNT(*) as total_respostas,
        COUNT(CASE WHEN classificacao_nps_assessor = 'Promotor' THEN 1 END) as promotores,
        COUNT(CASE WHEN classificacao_nps_assessor = 'Detrator' THEN 1 END) as detratores
    FROM silver.fact_nps_respostas_envios_aniversario
    WHERE classificacao_nps_assessor IS NOT NULL
    GROUP BY cod_assessor
)
SELECT 
    cod_assessor,
    total_respostas,
    ROUND((100.0 * promotores / total_respostas) - (100.0 * detratores / total_respostas), 2) as nps_score
FROM nps_calc
WHERE total_respostas >= 10 -- Mínimo de respostas para cálculo confiável
ORDER BY nps_score DESC;
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
- Depende da view vw_nps_respostas_envios_aniversario para consolidação
- View une dados de bronze.xp_nps_respostas e bronze.xp_nps_envios
- View sempre pega a versão mais recente de cada survey_id
- Transação garante que tabela nunca fica parcialmente carregada
- data_carga é preenchida automaticamente via DEFAULT da tabela

Mapeamento de campos:
- A view usa nomes originais das tabelas bronze
- A procedure faz o DE-PARA para nomes mais amigáveis na silver
- customer_id vira conta_xp_cliente
- invitation_opened vira convite_aberto
- Prefixos xp_aniversario_ são removidos

Arquitetura:
1. bronze.xp_nps_respostas: Respostas das pesquisas
2. bronze.xp_nps_envios: Dados de envio das pesquisas
3. silver.vw_nps_respostas_envios_aniversario: Consolidação e cálculos
4. silver.fact_nps_respostas_envios_aniversario: Dados materializados
5. Esta procedure: Materializa os dados da view

Possíveis melhorias:
1. Implementar carga incremental por survey_id
2. Adicionar parâmetro para reprocessar período específico
3. Implementar log de execução em tabela de controle
4. Adicionar validações de qualidade (notas entre 0-10)
5. Enviar alerta quando taxa de resposta cair muito
6. Implementar análise de sentimento nos comentários

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
