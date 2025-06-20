-- ==============================================================================
-- QRY-AHE-005-prc_gold_habilitacoes_ativacoes
-- ==============================================================================
-- Tipo: STORED PROCEDURE
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [habilitacoes, ativacoes, assessor, etl, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por atualizar a tabela gold.habilitacoes_ativacoes
com os dados mais recentes da view gold.vw_habilitacoes_ativacoes.
Realiza truncate/insert para garantir consistência dos dados.

Casos de uso:
- Execução diária via job do SQL Agent (recomendado: madrugada)
- Recarga manual após correções na view ou dados fonte
- Atualização sob demanda para relatórios urgentes

Frequência de execução: Diária 
Tempo médio de execução: 30-60 segundos
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
- gold.habilitacoes_ativacoes (truncate/insert completo)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- gold.vw_habilitacoes_ativacoes: View fonte com os dados consolidados
- gold.habilitacoes_ativacoes: Tabela destino que será atualizada

Pré-requisitos:
- View gold.vw_habilitacoes_ativacoes deve estar atualizada
- Tabela gold.habilitacoes_ativacoes deve existir
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
CREATE   PROCEDURE [gold].[prc_gold_habilitacoes_ativacoes]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Inicia transação
        BEGIN TRANSACTION;
        
        -- Trunca a tabela para limpar dados antigos
        TRUNCATE TABLE gold.habilitacoes_ativacoes;
        
        -- Insere dados atualizados da view
        INSERT INTO gold.habilitacoes_ativacoes (
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
            qtd_ativacoes_300k_mais,
            qtd_ativacoes_300k_menos,
            qtd_ativacoes_300k_mais_trimestre,
            qtd_ativacoes_300k_menos_trimestre,
            qtd_ativacoes_300k_mais_semestre,
            qtd_ativacoes_300k_menos_semestre,
            qtd_ativacoes_300k_mais_ano,
            qtd_ativacoes_300k_menos_ano,
            qtd_ativacoes_300k_mais_3_meses,
            qtd_ativacoes_300k_menos_3_meses,
            qtd_ativacoes_300k_mais_6_meses,
            qtd_ativacoes_300k_menos_6_meses,
            qtd_ativacoes_300k_mais_12_meses,
            qtd_ativacoes_300k_menos_12_meses,
            qtd_habilitacoes_300k_mais,
            qtd_habilitacoes_300k_menos,
            qtd_habilitacoes_300k_mais_trimestre,
            qtd_habilitacoes_300k_menos_trimestre,
            qtd_habilitacoes_300k_mais_semestre,
            qtd_habilitacoes_300k_menos_semestre,
            qtd_habilitacoes_300k_mais_ano,
            qtd_habilitacoes_300k_menos_ano,
            qtd_habilitacoes_300k_mais_3_meses,
            qtd_habilitacoes_300k_menos_3_meses,
            qtd_habilitacoes_300k_mais_6_meses,
            qtd_habilitacoes_300k_menos_6_meses,
            qtd_habilitacoes_300k_mais_12_meses,
            qtd_habilitacoes_300k_menos_12_meses
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
            qtd_ativacoes_300k_mais,
            qtd_ativacoes_300k_menos,
            qtd_ativacoes_300k_mais_trimestre,
            qtd_ativacoes_300k_menos_trimestre,
            qtd_ativacoes_300k_mais_semestre,
            qtd_ativacoes_300k_menos_semestre,
            qtd_ativacoes_300k_mais_ano,
            qtd_ativacoes_300k_menos_ano,
            qtd_ativacoes_300k_mais_3_meses,
            qtd_ativacoes_300k_menos_3_meses,
            qtd_ativacoes_300k_mais_6_meses,
            qtd_ativacoes_300k_menos_6_meses,
            qtd_ativacoes_300k_mais_12_meses,
            qtd_ativacoes_300k_menos_12_meses,
            qtd_habilitacoes_300k_mais,
            qtd_habilitacoes_300k_menos,
            qtd_habilitacoes_300k_mais_trimestre,
            qtd_habilitacoes_300k_menos_trimestre,
            qtd_habilitacoes_300k_mais_semestre,
            qtd_habilitacoes_300k_menos_semestre,
            qtd_habilitacoes_300k_mais_ano,
            qtd_habilitacoes_300k_menos_ano,
            qtd_habilitacoes_300k_mais_3_meses,
            qtd_habilitacoes_300k_menos_3_meses,
            qtd_habilitacoes_300k_mais_6_meses,
            qtd_habilitacoes_300k_menos_6_meses,
            qtd_habilitacoes_300k_mais_12_meses,
            qtd_habilitacoes_300k_menos_12_meses
        FROM gold.vw_habilitacoes_ativacoes;
        
        -- Conta registros inseridos
        DECLARE @RowCount INT = @@ROWCOUNT;
        
        -- Confirma transação
        COMMIT TRANSACTION;
        
        -- Retorna mensagem de sucesso
        PRINT 'Procedure executada com sucesso!';
        PRINT 'Total de registros inseridos: ' + CAST(@RowCount AS VARCHAR(10));
        
    END TRY
    BEGIN CATCH
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
-- 7. TRATAMENTO DE ERROS
-- ==============================================================================
-- A procedure implementa tratamento completo de erros com:
-- - Transação para garantir atomicidade
-- - Rollback automático em caso de erro
-- - Captura e relançamento de mensagens de erro detalhadas
-- - Log de quantidade de registros processados

-- ==============================================================================
-- 8. EXEMPLOS DE USO
-- ==============================================================================
/*
-- Execução manual simples
EXEC gold.prc_gold_habilitacoes_ativacoes;

-- Execução com verificação de resultado
DECLARE @result INT;
EXEC @result = gold.prc_gold_habilitacoes_ativacoes;
IF @result = 0
    PRINT 'Execução concluída com sucesso';
ELSE
    PRINT 'Erro na execução';

-- Verificação de dados após execução
SELECT 
    COUNT(*) as total_registros,
    MIN(ano_mes) as periodo_inicial,
    MAX(ano_mes) as periodo_final,
    MAX(data_carga) as ultima_carga
FROM gold.habilitacoes_ativacoes;
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
- Transação garante que ou todos os dados são atualizados ou nenhum
- SET NOCOUNT ON melhora performance evitando mensagens desnecessárias
- data_carga é preenchida automaticamente pelo DEFAULT da tabela

Troubleshooting comum:
1. Timeout: Aumentar timeout da conexão ou executar em horário de menor uso
2. Erro de permissão: Verificar se usuário tem TRUNCATE e INSERT na tabela
3. Dados não atualizados: Verificar se view fonte está processando corretamente

Agenda recomendada:
- Execução diária às 02:00 AM via SQL Agent Job
- Deve executar APÓS atualização das tabelas silver

Contato para dúvidas: equipe-dados@m7investimentos.com.br
*/
