-- ==============================================================================
-- QRY-AHE-003-PRC_BRONZE_TO_SILVER_FACT_ATIVACOES_HABILITACOES_EVASOES
-- ==============================================================================
-- Tipo: Stored Procedure
-- Versão: 1.0.0
-- Última atualização: 2025-01-20
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [etl, bronze_to_silver, procedure, ativacoes, habilitacoes, evasoes]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Procedure responsável por realizar a carga de dados da camada bronze 
           para a silver, processando registros de ativações, habilitações e 
           evasões de clientes.

Casos de uso:
- Carga diária de dados de movimentações de clientes
- Atualização completa da tabela fact_ativacoes_habilitacoes_evasoes
- Parte do processo ETL bronze -> silver

Frequência de execução: Diária
Tempo médio de execução: 2-5 minutos
Volume esperado de linhas: ~2.000 registros/dia
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
Tabela de destino: [silver].[fact_ativacoes_habilitacoes_evasoes]

Colunas carregadas:
- data_ref: Data de referência do registro
- cod_xp: Código do cliente no sistema XP
- crm_id: Identificador do assessor no CRM
- id_estrutura: Identificador da estrutura
- faixa_pl: Faixa de patrimônio líquido
- tipo_movimentacao: Tipo de movimentação (ativação/habilitação/evasão)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas de origem (INPUT):
- [bronze].[xp_ativacoes_habilitacoes_evasoes]: Dados brutos de movimentações
  * Campos utilizados: data_ref, cod_xp, cod_aai, faixa_pl, tipo_movimentacao
  * Volume esperado: ~2.000 registros/dia
  
- [silver].[dim_pessoas]: Dimensão de pessoas para obter crm_id
  * Campos utilizados: cod_aai (join), crm_id (output)
  * Tipo de join: LEFT JOIN
  * Condição: s.cod_aai = d.cod_aai
  
- [silver].[fact_estrutura_pessoas]: Estrutura organizacional para obter id_estrutura
  * Campos utilizados: crm_id (join), id_estrutura (output), data_entrada, data_saida
  * Tipo de join: LEFT JOIN temporal
  * Condição: d.crm_id = f.crm_id AND vigência temporal

Tabela de destino (OUTPUT):
- [silver].[fact_ativacoes_habilitacoes_evasoes]: Tabela fato de movimentações
  * Operação: TRUNCATE seguido de INSERT
  * Modo: Full load (carga completa)

Pré-requisitos:
- Tabela bronze deve estar atualizada com dados do dia
- dim_pessoas deve estar carregada e atualizada
- fact_estrutura_pessoas deve estar carregada com histórico de estruturas
- Usuário deve ter permissões: TRUNCATE, INSERT, SELECT
- Registros sem crm_id (assessor não encontrado) são descartados
*/

-- ==============================================================================
-- 5. SCRIPT DA PROCEDURE
-- ==============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [silver].[prc_bronze_to_silver_fact_ativacoes_habilitacoes_evasoes]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- ==============================================================================
    -- 5.1. TRUNCATE DA TABELA DE DESTINO
    -- ==============================================================================
    -- Elimina todos os registros anteriores para garantir consistência
    TRUNCATE TABLE [silver].[fact_ativacoes_habilitacoes_evasoes];
    
    -- ==============================================================================
    -- 5.2. CARGA DOS DADOS
    -- ==============================================================================
    -- Insere os dados atualizados diretamente da stage
    INSERT INTO [silver].[fact_ativacoes_habilitacoes_evasoes] (
        data_ref,
        cod_xp,
        crm_id,
        id_estrutura,
        faixa_pl,
        tipo_movimentacao
    )
    SELECT 
        s.data_ref,
        s.cod_xp,
        d.crm_id,
        f.id_estrutura,
        s.faixa_pl,
        s.tipo_movimentacao
    FROM [bronze].[xp_ativacoes_habilitacoes_evasoes] s
    LEFT JOIN [silver].[dim_pessoas] d
        ON s.cod_aai = d.cod_aai
    LEFT JOIN [silver].[fact_estrutura_pessoas] f
        ON d.crm_id = f.crm_id
        AND s.data_ref >= f.data_entrada
        AND (f.data_saida IS NULL OR s.data_ref <= f.data_saida)
    WHERE d.crm_id IS NOT NULL;  -- Filtro para garantir que apenas registros com assessor sejam carregados
    
END;
GO

-- ==============================================================================
-- 6. TRATAMENTO DE ERROS
-- ==============================================================================
/*
Notas sobre tratamento de erros:
- A procedure não possui tratamento explícito de erros (TRY/CATCH)
- Em caso de erro, a transação será revertida automaticamente
- Logs de erro devem ser verificados no SQL Server Agent

Recomendação: Implementar bloco TRY/CATCH para melhor controle de erros
*/

-- ==============================================================================
-- 7. SCRIPTS DE VALIDAÇÃO
-- ==============================================================================
/*
-- Verificar quantidade de registros carregados
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT cod_xp) as clientes_unicos,
    COUNT(DISTINCT crm_id) as assessores_unicos,
    MIN(data_ref) as data_inicial,
    MAX(data_ref) as data_final
FROM [silver].[fact_ativacoes_habilitacoes_evasoes];

-- Verificar distribuição por tipo de movimentação
SELECT 
    tipo_movimentacao,
    COUNT(*) as quantidade
FROM [silver].[fact_ativacoes_habilitacoes_evasoes]
GROUP BY tipo_movimentacao
ORDER BY quantidade DESC;

-- Verificar registros sem correspondência (perdidos no JOIN)
SELECT COUNT(*) as registros_sem_assessor
FROM [bronze].[xp_ativacoes_habilitacoes_evasoes] s
LEFT JOIN [silver].[dim_pessoas] d ON s.cod_aai = d.cod_aai
WHERE d.crm_id IS NULL;
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
- Registros sem crm_id (assessor) são descartados
- Join temporal com fact_estrutura_pessoas considera período de vigência
- Performance pode ser impactada pelo volume de dados em fact_estrutura_pessoas

Possíveis melhorias:
1. Implementar carga incremental baseada em data_ref
2. Adicionar tratamento de erros com TRY/CATCH
3. Incluir log de execução em tabela de controle
4. Adicionar validações de qualidade de dados
5. Implementar notificações em caso de anomalias

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
