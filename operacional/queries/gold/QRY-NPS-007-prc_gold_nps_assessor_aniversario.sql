-- ==============================================================================
-- QRY-NPS-007-prc_gold_nps_assessor_aniversario
-- ==============================================================================
-- Tipo: STORED PROCEDURE
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [nps, assessor, etl, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por atualizar a tabela gold.nps_assessor_aniversario
com os dados mais recentes da view gold.vw_nps_assessor_aniversario.
Realiza truncate/insert para garantir consistência dos dados de NPS.

Casos de uso:
- Execução diária via job do SQL Agent (recomendado: madrugada)
- Recarga manual após correções nos dados de pesquisas
- Atualização emergencial para dashboards de satisfação

Frequência de execução: Diária
Tempo médio de execução: 10-20 segundos
Volume processado: ~24.000 registros (12 meses x 2.000 assessores)
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não possui parâmetros de entrada.
Processa sempre todos os dados disponíveis na view.
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Saída via PRINT:
- Mensagem de sucesso com total de registros inseridos
- Mensagem de erro em caso de falha (via RAISERROR)

Tabela atualizada:
- gold.nps_assessor_aniversario (truncate/insert completo)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- gold.vw_nps_assessor_aniversario: View fonte com cálculos de NPS
- gold.nps_assessor_aniversario: Tabela destino que será atualizada

Pré-requisitos:
- View gold.vw_nps_assessor_aniversario deve estar atualizada
- Tabelas silver.fact_nps_respostas_envios_aniversario devem estar processadas
- Tabela gold.nps_assessor_aniversario deve existir
- Usuário deve ter permissões TRUNCATE e INSERT na tabela destino
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA PROCEDURE
-- ==============================================================================
CREATE   PROCEDURE [gold].[prc_gold_nps_assessor_aniversario]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Truncate da tabela de destino
        TRUNCATE TABLE [gold].[nps_assessor_aniversario];
        
        -- Insert dos dados da view
        INSERT INTO [gold].[nps_assessor_aniversario] (
            ano_mes,
            ano,
            mes,
            nome_mes,
            trimestre,
            semestre,
            cod_assessor,
            crm_id_assessor,
            nome_assessor,
            nivel_assessor,
            estrutura_id,
            estrutura_nome,
            qtd_pesquisas_enviadas,
            qtd_pesquisas_respondidas,
            qtd_convites_abertos,
            taxa_resposta,
            taxa_abertura,
            nps_score_assessor_mes,
            nps_score_assessor_trimestre,
            nps_score_assessor_semestre,
            nps_score_assessor_ano,
            nps_score_assessor_3_meses,
            nps_score_assessor_6_meses,
            nps_score_assessor_12_meses,
            qtd_promotores,
            qtd_neutros,
            qtd_detratores,
            qtd_recomendaria_sim,
            perc_recomendaria,
            razao_principal,
            data_carga
        )
        SELECT 
            ano_mes,
            ano,
            mes,
            nome_mes,
            trimestre,
            semestre,
            cod_assessor,
            crm_id_assessor,
            nome_assessor,
            nivel_assessor,
            estrutura_id,
            estrutura_nome,
            qtd_pesquisas_enviadas,
            qtd_pesquisas_respondidas,
            qtd_convites_abertos,
            taxa_resposta,
            taxa_abertura,
            nps_score_assessor_mes,
            nps_score_assessor_trimestre,
            nps_score_assessor_semestre,
            nps_score_assessor_ano,
            nps_score_assessor_3_meses,
            nps_score_assessor_6_meses,
            nps_score_assessor_12_meses,
            qtd_promotores,
            qtd_neutros,
            qtd_detratores,
            qtd_recomendaria_sim,
            perc_recomendaria,
            razao_principal,
            CAST(GETDATE() AS DATE) AS data_carga -- Adiciona a data de carga como hoje
        FROM [gold].[vw_nps_assessor_aniversario];
        
        -- Log de sucesso
        PRINT 'Procedure executada com sucesso!';
        PRINT 'Registros inseridos: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
        
    END TRY
    BEGIN CATCH
        -- Tratamento de erro
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- ==============================================================================
-- 7. TRATAMENTO DE ERROS
-- ==============================================================================
-- A procedure implementa tratamento básico de erros com:
-- - Bloco TRY/CATCH
-- - Captura e relançamento de mensagens de erro
-- - Log de quantidade de registros processados
-- Nota: Não há transação explícita - considerar adicionar para atomicidade

-- ==============================================================================
-- 8. EXEMPLOS DE USO
-- ==============================================================================
/*
-- Execução manual simples
EXEC gold.prc_gold_nps_assessor_aniversario;

-- Execução com verificação de resultado
DECLARE @result INT;
EXEC @result = gold.prc_gold_nps_assessor_aniversario;
IF @result = 0
    PRINT 'Execução concluída com sucesso';
ELSE
    PRINT 'Erro na execução';

-- Verificação de dados após execução
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT cod_assessor) as total_assessores,
    MIN(ano_mes) as periodo_inicial,
    MAX(ano_mes) as periodo_final,
    AVG(CAST(nps_score_assessor_mes AS FLOAT)) as nps_medio,
    MAX(data_carga) as ultima_carga
FROM gold.nps_assessor_aniversario;

-- Top 10 assessores por NPS no último mês
SELECT TOP 10
    ano_mes,
    cod_assessor,
    nome_assessor,
    qtd_pesquisas_respondidas,
    nps_score_assessor_mes,
    qtd_promotores,
    qtd_detratores,
    perc_recomendaria
FROM gold.nps_assessor_aniversario
WHERE ano_mes = (SELECT MAX(ano_mes) FROM gold.nps_assessor_aniversario)
    AND qtd_pesquisas_respondidas > 0
ORDER BY nps_score_assessor_mes DESC;
*/

-- ==============================================================================
-- 9. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2025-01-20 | equipe.dados   | Criação inicial da procedure

*/

-- ==============================================================================
-- 10. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Procedure usa TRUNCATE TABLE, removendo TODOS os dados antes da recarga
- Não há processamento incremental - sempre recarga completa
- Não há transação explícita - em caso de erro, tabela pode ficar vazia
- SET NOCOUNT ON melhora performance evitando mensagens desnecessárias
- data_carga é explicitamente definida como GETDATE() no INSERT

Melhorias sugeridas:
1. Adicionar transação explícita com BEGIN/COMMIT/ROLLBACK
2. Implementar log de execução em tabela de auditoria
3. Adicionar validação de dados antes do INSERT
4. Considerar processamento incremental para grandes volumes
5. Adicionar parâmetro opcional para processar período específico

Agenda recomendada:
- Execução diária às 04:00 AM via SQL Agent Job
- Deve executar APÓS atualização da silver.fact_nps_respostas_envios_aniversario

Contato para dúvidas: equipe-dados@m7investimentos.com.br
*/
