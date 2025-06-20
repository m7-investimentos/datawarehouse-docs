-- ==============================================================================
-- QRY-IEA-006-prc_gold_indice_esforco_assessor
-- ==============================================================================
-- Tipo: STORED PROCEDURE
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [equipe.dados@m7investimentos.com.br]
-- Revisor: [revisor@m7investimentos.com.br]
-- Tags: [indice_esforco, assessor, etl, gold]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: gold
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por atualizar a tabela gold.indice_esforco_assessor
com os dados mais recentes da view gold.vw_indice_esforco_assessor.
Realiza truncate/insert para garantir consistência dos dados do IEA.

Casos de uso:
- Execução diária via job do SQL Agent (recomendado: madrugada)
- Recarga manual após correções nos dados fonte da silver
- Atualização emergencial para dashboards executivos

Frequência de execução: Diária
Tempo médio de execução: 15-30 segundos
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
Saída:
- Não retorna resultset
- Não possui mensagens de saída (considerar adicionar)

Tabela atualizada:
- gold.indice_esforco_assessor (truncate/insert completo)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- gold.vw_indice_esforco_assessor: View fonte com cálculos do IEA
- gold.indice_esforco_assessor: Tabela destino que será atualizada

Pré-requisitos:
- View gold.vw_indice_esforco_assessor deve estar atualizada
- Tabelas silver.fact_indice_esforco_assessor devem estar processadas
- Tabela gold.indice_esforco_assessor deve existir
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
CREATE   PROCEDURE [gold].[prc_gold_indice_esforco_assessor]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Trunca a tabela gold
    TRUNCATE TABLE gold.indice_esforco_assessor;
    
    -- Insere os dados da view com todas as colunas explícitas
    INSERT INTO gold.indice_esforco_assessor (
        ano,
        ano_mes,
        mes,
        nome_mes,
        semestre,
        trimestre,
        cod_assessor,
        crm_id_assessor,
        nome_assessor,
        nivel_assessor,
        estrutura_id,
        estrutura_nome,
        esforco_prospeccao,
        esforco_relacionamento,
        indice_esforco_assessor,
        indice_esforco_assessor_3_meses,
        indice_esforco_assessor_6_meses,
        indice_esforco_assessor_12_meses,
        indice_esforco_assessor_ano,
        indice_esforco_assessor_semestre,
        indice_esforco_assessor_trimestre,
        prospeccao_atingimento_carteiras_simuladas_novos,
        prospeccao_atingimento_conversao,
        prospeccao_atingimento_habilitacoes,
        prospeccao_atingimento_lead_starts,
        prospeccao_captacao_de_novos_clientes_por_aai,
        relacionamento_atingimento_contas_acessadas_hub,
        relacionamento_atingimento_contas_aportarem,
        relacionamento_atingimento_ordens_enviadas,
        relacionamento_captacao_da_base
    )
    SELECT 
        ano,
        ano_mes,
        mes,
        nome_mes,
        semestre,
        trimestre,
        cod_assessor,
        crm_id_assessor,
        nome_assessor,
        nivel_assessor,
        estrutura_id,
        estrutura_nome,
        esforco_prospeccao,
        esforco_relacionamento,
        indice_esforco_assessor,
        indice_esforco_assessor_3_meses,
        indice_esforco_assessor_6_meses,
        indice_esforco_assessor_12_meses,
        indice_esforco_assessor_ano,
        indice_esforco_assessor_semestre,
        indice_esforco_assessor_trimestre,
        prospeccao_atingimento_carteiras_simuladas_novos,
        prospeccao_atingimento_conversao,
        prospeccao_atingimento_habilitacoes,
        prospeccao_atingimento_lead_starts,
        prospeccao_captacao_de_novos_clientes_por_aai,
        relacionamento_atingimento_contas_acessadas_hub,
        relacionamento_atingimento_contas_aportarem,
        relacionamento_atingimento_ordens_enviadas,
        relacionamento_captacao_da_base
    FROM gold.vw_indice_esforco_assessor;
    
END
GO

-- ==============================================================================
-- 7. TRATAMENTO DE ERROS
-- ==============================================================================
-- ATENÇÃO: Esta procedure NÃO possui tratamento de erros implementado!
-- Recomenda-se adicionar:
-- - Bloco TRY/CATCH
-- - Transação com ROLLBACK em caso de erro
-- - Log de execução e mensagens de status
-- - Validação de dados antes do INSERT

-- ==============================================================================
-- 8. EXEMPLOS DE USO
-- ==============================================================================
/*
-- Execução manual simples
EXEC gold.prc_gold_indice_esforco_assessor;

-- Verificação de dados após execução
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT cod_assessor) as total_assessores,
    MIN(ano_mes) as periodo_inicial,
    MAX(ano_mes) as periodo_final,
    MAX(data_carga) as ultima_carga
FROM gold.indice_esforco_assessor;

-- Verificar top 10 assessores por IEA no último mês
SELECT TOP 10
    ano_mes,
    cod_assessor,
    nome_assessor,
    indice_esforco_assessor,
    esforco_prospeccao,
    esforco_relacionamento
FROM gold.indice_esforco_assessor
WHERE ano_mes = (SELECT MAX(ano_mes) FROM gold.indice_esforco_assessor)
ORDER BY indice_esforco_assessor DESC;
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
- Não há tratamento de erros - em caso de falha, tabela pode ficar vazia
- SET NOCOUNT ON melhora performance evitando mensagens desnecessárias
- Ordem das colunas no INSERT deve corresponder exatamente à ordem no SELECT
- data_carga é preenchida automaticamente pelo DEFAULT da tabela

Melhorias sugeridas:
1. Adicionar tratamento de erros com TRY/CATCH
2. Implementar log de execução em tabela de auditoria
3. Adicionar validação de dados antes do INSERT
4. Considerar processamento incremental para grandes volumes
5. Adicionar parâmetro opcional para processar período específico

Agenda recomendada:
- Execução diária às 03:00 AM via SQL Agent Job
- Deve executar APÓS atualização da silver.fact_indice_esforco_assessor

Contato para dúvidas: equipe-dados@m7investimentos.com.br
*/
