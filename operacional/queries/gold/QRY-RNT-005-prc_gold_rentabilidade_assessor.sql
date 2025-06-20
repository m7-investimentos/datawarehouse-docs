-- ==============================================================================
-- QRY-RNT-005-prc_gold_rentabilidade_assessor
-- ==============================================================================
-- Tipo: STORED PROCEDURE
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [rentabilidade, assessor, etl, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por atualizar a tabela gold.rentabilidade_assessor
com os dados mais recentes da view gold.vw_rentabilidade_assessor.
Realiza truncate/insert para garantir consistência dos dados de rentabilidade.

Casos de uso:
- Execução diária via job do SQL Agent (recomendado: madrugada)
- Recarga manual após correções nos dados de rentabilidade ou CDI
- Atualização emergencial para dashboards executivos

Frequência de execução: Diária
Tempo médio de execução: 20-40 segundos
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
- gold.rentabilidade_assessor (truncate/insert completo)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- gold.vw_rentabilidade_assessor: View fonte com cálculos de rentabilidade
- gold.rentabilidade_assessor: Tabela destino que será atualizada

Pré-requisitos:
- View gold.vw_rentabilidade_assessor deve estar atualizada
- Tabelas silver devem estar processadas (rentabilidade, CDI, patrimônio)
- Tabela gold.rentabilidade_assessor deve existir
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
CREATE   PROCEDURE [gold].[prc_gold_rentabilidade_assessor]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Inicia transação
        BEGIN TRANSACTION;
        
        -- Trunca a tabela para limpar dados antigos
        TRUNCATE TABLE gold.rentabilidade_assessor;
        
        -- Insere dados atualizados da view
        INSERT INTO gold.rentabilidade_assessor (
            	[ano_mes],
	[ano],
	[mes],
	[nome_mes],
	[trimestre],
	[semestre],
	[cod_assessor],
	[codigo_crm_assessor],
	[nome_assessor],
	[nivel_assessor],
	[estrutura_id],
	[estrutura_nome],
	[qtd_clientes_300k_mais],
	[qtd_clientes_acima_cdi],
	[qtd_clientes_faixa_80_cdi],
	[qtd_clientes_faixa_50_cdi],
	[qtd_clientes_rentabilidade_positiva],
	[perc_clientes_acima_cdi]

        )
        SELECT 
                            	[ano_mes],
	[ano],
	[mes],
	[nome_mes],
	[trimestre],
	[semestre],
	[cod_assessor],
	[codigo_crm_assessor],
	[nome_assessor],
	[nivel_assessor],
	[estrutura_id],
	[estrutura_nome],
	[qtd_clientes_300k_mais],
	[qtd_clientes_acima_cdi],
	[qtd_clientes_faixa_80_cdi],
	[qtd_clientes_faixa_50_cdi],
	[qtd_clientes_rentabilidade_positiva],
	[perc_clientes_acima_cdi]

        FROM gold.vw_rentabilidade_assessor;

        
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
EXEC gold.prc_gold_rentabilidade_assessor;

-- Execução com verificação de resultado
DECLARE @result INT;
EXEC @result = gold.prc_gold_rentabilidade_assessor;
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
    AVG(CAST(perc_clientes_acima_cdi AS FLOAT)) as perc_medio_acima_cdi,
    MAX(data_carga) as ultima_carga
FROM gold.rentabilidade_assessor;

-- Top 10 assessores por % clientes acima do CDI
SELECT TOP 10
    ano_mes,
    cod_assessor,
    nome_assessor,
    qtd_clientes_300k_mais,
    qtd_clientes_acima_cdi,
    perc_clientes_acima_cdi,
    estrutura_nome
FROM gold.rentabilidade_assessor
WHERE ano_mes = (SELECT MAX(ano_mes) FROM gold.rentabilidade_assessor)
    AND qtd_clientes_300k_mais >= 10 -- Mínimo de 10 clientes para evitar distorções
ORDER BY perc_clientes_acima_cdi DESC;
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

Filtros aplicados:
- Apenas clientes PF (CPF não nulo)
- Apenas clientes com patrimônio >= R$ 300.000
- Dados a partir de 2024 (definido na view)

Troubleshooting comum:
1. Timeout: Aumentar timeout ou verificar índices nas tabelas silver
2. Erro de permissão: Verificar se usuário tem TRUNCATE e INSERT
3. Dados inconsistentes: Verificar processamento do CDI e rentabilidade

Agenda recomendada:
- Execução diária às 05:00 AM via SQL Agent Job
- Deve executar APÓS atualização das tabelas silver de rentabilidade e CDI

Contato para dúvidas: equipe-dados@m7investimentos.com.br
*/
