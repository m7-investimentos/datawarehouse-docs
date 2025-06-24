-- ==============================================================================
-- QRY-IND-003-prc_bronze_to_silver_dim_indicators
-- ==============================================================================
-- Tipo: Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-27
-- Autor: bruno.chiaramonti@multisete.com
-- Revisor: bruno.chiaramonti@multisete.com
-- Tags: [etl, bronze, silver, dimensão, indicadores, performance]
-- Status: desenvolvimento
-- Banco de Dados: SQL Server 2019
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure ETL para carregar dados de indicadores de performance da 
camada Bronze para Silver, realizando transformações e validações necessárias.

Casos de uso:
- Carga inicial de indicadores na implantação do sistema
- Atualização diária de novos indicadores ou alterações
- Sincronização após mudanças no Google Sheets

Frequência de execução: Diária ou sob demanda
Tempo médio de execução: < 5 segundos
Volume esperado de linhas: 50-100 registros
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Esta procedure não possui parâmetros de entrada.
Processa todos os registros com is_processed = 0 na bronze.
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
A procedure não retorna resultset, apenas mensagem de status via PRINT.

Tabela atualizada: silver.dim_indicators
- INSERT: Novos indicadores são inseridos
- UPDATE: Indicadores existentes são atualizados se houver mudanças
- Campos is_processed na bronze são marcados como processados
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- bronze.performance_indicators: Fonte dos dados de indicadores (Google Sheets)
- silver.dim_indicators: Destino - dimensão de indicadores

Pré-requisitos:
- Tabela silver.dim_indicators deve existir
- Dados devem estar carregados na bronze via ETL Python
- Usuário deve ter permissões de SELECT na bronze e INSERT/UPDATE na silver
*/

-- ==============================================================================
-- 5. PROCEDURE
-- ==============================================================================

CREATE OR ALTER PROCEDURE silver.sp_load_dim_indicators
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Usar MERGE para inserir novos e atualizar existentes
        MERGE silver.dim_indicators AS target
        USING (
            SELECT 
                -- Mapeamento direto das colunas conforme documentação MOD-IND-002
                indicator_code AS indicator_id,
                indicator_name AS nome,
                
                -- Tipo: CARD, GATILHO, KPI ou PPI (conforme documentação)
                COALESCE(
                    CASE 
                        WHEN notes LIKE '%CARD%' THEN 'CARD'
                        WHEN notes LIKE '%GATILHO%' THEN 'GATILHO'
                        WHEN notes LIKE '%PPI%' THEN 'PPI'
                        WHEN notes LIKE '%KPI%' THEN 'KPI'
                        ELSE NULL
                    END,
                    indicator_type,
                    'KPI'  -- Default se não identificado
                ) AS tipo,
                
                -- Grupo vem da category
                ISNULL(category, 'OUTROS') AS grupo,
                
                -- Unidade de medida vem de unit
                unit AS unidade_medida,
                
                -- Fórmula SQL vem direto
                formula AS formula_calculo,
                
                -- Método de agregação com default 'SUM'
                ISNULL(aggregation, 'SUM') AS aggregation_method,
                
                -- Tabela origem: extrair da fórmula
                CASE 
                    WHEN formula LIKE '%captacao_liquida_assessor%' THEN 'gold.captacao_liquida_assessor'
                    WHEN formula LIKE '%nps_assessor_aniversario%' THEN 'gold.nps_assessor_aniversario'
                    WHEN formula LIKE '%habilitacoes_ativacoes%' THEN 'gold.habilitacoes_ativacoes'
                    WHEN formula LIKE '%indice_esforco_assessor%' THEN 'gold.indice_esforco_assessor'
                    WHEN formula LIKE '%rentabilidade_assessor%' THEN 'gold.rentabilidade_assessor'
                    ELSE 'gold.custom'
                END AS tabela_origem,
                
                -- Flags booleanos - is_active vem direto do Google Sheets
                CAST(ISNULL(is_inverted, '0') AS BIT) AS is_inverted,
                CAST(ISNULL(is_active, '1') AS BIT) AS is_active
                
            FROM bronze.performance_indicators
            WHERE is_processed = 0  -- Apenas não processados
               OR NOT EXISTS (SELECT 1 FROM silver.dim_indicators)  -- Ou se silver estiver vazia
        ) AS source
        ON target.indicator_id = source.indicator_id
        
        -- Quando encontrar correspondência, atualizar se houver mudanças
        WHEN MATCHED AND (
            target.nome <> source.nome OR
            target.tipo <> source.tipo OR
            target.grupo <> source.grupo OR
            ISNULL(target.unidade_medida, '') <> ISNULL(source.unidade_medida, '') OR
            target.formula_calculo <> source.formula_calculo OR
            target.aggregation_method <> source.aggregation_method OR
            target.tabela_origem <> source.tabela_origem OR
            target.is_inverted <> source.is_inverted OR
            target.is_active <> source.is_active
        )
        THEN UPDATE SET
            nome = source.nome,
            tipo = source.tipo,
            grupo = source.grupo,
            unidade_medida = source.unidade_medida,
            formula_calculo = source.formula_calculo,
            aggregation_method = source.aggregation_method,
            tabela_origem = source.tabela_origem,
            is_inverted = source.is_inverted,
            is_active = source.is_active,
            modified_date = GETDATE()
        
        -- Quando não encontrar, inserir novo
        WHEN NOT MATCHED THEN
        INSERT (
            indicator_id,
            nome,
            tipo,
            grupo,
            unidade_medida,
            formula_calculo,
            aggregation_method,
            tabela_origem,
            is_inverted,
            is_active,
            created_date,
            modified_date
        )
        VALUES (
            source.indicator_id,
            source.nome,
            source.tipo,
            source.grupo,
            source.unidade_medida,
            source.formula_calculo,
            source.aggregation_method,
            source.tabela_origem,
            source.is_inverted,
            source.is_active,
            GETDATE(),
            GETDATE()
        );

        -- Marcar registros como processados na bronze
        UPDATE bronze.performance_indicators
        SET is_processed = 1,
            processing_date = GETDATE(),
            processing_status = 'SUCCESS'
        WHERE indicator_code IN (
            SELECT indicator_id FROM silver.dim_indicators
        );

        PRINT 'Carga concluída. Total de registros afetados: ' + CAST(@@ROWCOUNT AS VARCHAR);

    END TRY
    BEGIN CATCH
        -- Em caso de erro, marcar registros com falha
        UPDATE bronze.performance_indicators
        SET processing_status = 'ERROR',
            processing_notes = LEFT(ERROR_MESSAGE(), 500),
            processing_date = GETDATE()
        WHERE is_processed = 0;
        
        -- Re-lançar o erro
        THROW;
    END CATCH
END;
GO

-- ==============================================================================
-- 6. DOCUMENTAÇÃO DA PROCEDURE
-- ==============================================================================

EXEC sys.sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Procedure ETL para migrar indicadores de Bronze para Silver conforme documentação MOD-IND-002',
    @level0type = N'SCHEMA', @level0name = N'silver',
    @level1type = N'PROCEDURE', @level1name = N'sp_load_dim_indicators';
GO

-- ==============================================================================
-- 7. EXEMPLOS DE USO
-- ==============================================================================
/*
-- Executar a carga
EXEC silver.sp_load_dim_indicators;

-- Verificar indicadores carregados
SELECT 
    indicator_id,
    nome,
    tipo,
    is_active,
    created_date,
    modified_date
FROM silver.dim_indicators
ORDER BY modified_date DESC;

-- Verificar registros pendentes de processamento
SELECT COUNT(*) as pendentes
FROM bronze.performance_indicators
WHERE is_processed = 0;
*/

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                      | Descrição
--------|------------|----------------------------|--------------------------------------------
1.0.0   | 2025-01-27 | bruno.chiaramonti         | Criação inicial da procedure

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- O campo is_active é definido no Google Sheets pela equipe de gestão
- Tipo do indicador é derivado do campo notes ou indicator_type
- Tabela origem é extraída automaticamente da fórmula SQL
- Procedure usa MERGE para suportar inserções e atualizações

Troubleshooting comum:
1. Erro de tipo inválido: Verificar se notes contém CARD/GATILHO/KPI/PPI
2. Registros não processados: Verificar flag is_processed na bronze
3. Fórmulas inválidas: Não impede a carga, mas pode gerar erro na execução

Contato para dúvidas: analytics@m7investimentos.com.br
*/