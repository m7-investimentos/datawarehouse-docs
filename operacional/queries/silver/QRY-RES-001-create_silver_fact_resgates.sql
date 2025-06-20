-- ==============================================================================
-- QRY-RES-001-create_silver_fact_resgates
-- ==============================================================================
-- Tipo: DDL - CREATE TABLE
-- Versão: 1.0.0
-- Última atualização: 2024-11-28
-- Autor: [nome.sobrenome@m7investimentos.com.br]
-- Revisor: [nome.sobrenome@m7investimentos.com.br]
-- Tags: [fato, resgates, transferências, silver]
-- Status: produção
-- Banco de Dados: SQL Server 2016+
-- Schema: silver
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Criação da tabela fato de resgates e transferências de saída no Data Warehouse.
Esta tabela contém informações consolidadas sobre todos os tipos de saídas de recursos
dos clientes, incluindo resgates diretos e transferências para outras instituições.

Casos de uso:
- Análise de evasão de recursos por período
- Identificação de padrões de resgate por assessor
- Acompanhamento de transferências de saída
- Cálculo de net captação (junto com fact_captacao_bruta)
- Análise de origem dos resgates (TED, Previdência, OTA, etc.)

Frequência de execução: Única (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: ~100.000 registros/ano
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Não aplicável - Script DDL de criação de tabela
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Colunas criadas:

| Coluna                      | Tipo         | Descrição                                                    | Exemplo           |
|-----------------------------|--------------|--------------------------------------------------------------|-------------------|
| data_ref                    | DATE         | Data de referência do registro                               | 2024-03-15        |
| conta_xp_cliente            | INT          | Código da conta do cliente no sistema XP                     | 123456            |
| cod_assessor                | VARCHAR(50)  | Código do assessor no sistema da XP                          | 'A1234'           |
| origem_resgate              | VARCHAR(100) | Origem do resgate (TED, Prev, OTA...)                       | 'TED'             |
| resgate_bruto_xp            | DECIMAL(18,2)| Valor dos resgates brutos XP (negativo)                      | -50000.00         |
| tipo_transferencia          | VARCHAR(100) | Tipo de transferência de saída                               | 'saída'           |
| resgate_bruto_transferencia | DECIMAL(18,2)| Valor da transferência de saída (negativo)                   | -100000.00        |
| resgate_bruto_total         | DECIMAL(18,2)| Soma total dos resgates (XP + transferência)                 | -150000.00        |

Chave primária: Não definida (considerar criar índice clustered em data_ref, conta_xp_cliente)
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
- Nenhuma (criação inicial)

Tabelas de origem (para carga via procedure):
- bronze.xp_captacao: Dados de captação/resgate da XP
- bronze.xp_transferencia_clientes: Dados de transferências entre instituições
- bronze.xp_positivador: Dados de patrimônio para calcular valor de transferência

Funções/Procedures chamadas:
- sys.sp_addextendedproperty: Adição de metadados descritivos

Pré-requisitos:
- Schema silver deve existir
- Permissões CREATE TABLE no schema silver
- Permissões para adicionar extended properties
- Executar procedure prc_bronze_to_silver_fact_resgates após criação
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ==============================================================================
-- 6. CRIAÇÃO DA TABELA
-- ==============================================================================
CREATE TABLE [silver].[fact_resgates](
	[data_ref] [date] NOT NULL,
	[conta_xp_cliente] [int] NOT NULL,
	[cod_assessor] [varchar](50) NOT NULL,
	[origem_resgate] [varchar](100) NOT NULL,
	[resgate_bruto_xp] [decimal](18, 2) NOT NULL,
	[tipo_transferencia] [varchar](100) NOT NULL,
	[resgate_bruto_transferencia] [decimal](18, 2) NOT NULL,
	[resgate_bruto_total] [decimal](18, 2) NOT NULL
) ON [PRIMARY]
GO

-- ==============================================================================
-- 7. DOCUMENTAÇÃO DOS CAMPOS (EXTENDED PROPERTIES)
-- ==============================================================================
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'data de referência do registro' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'data_ref'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código da conta do cliente no sistema xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'conta_xp_cliente'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'código do assessor no sistema da xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'cod_assessor'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'origem do resgate (ted, prev, ota....)' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'origem_resgate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor dos resgates brutos xp' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'resgate_bruto_xp'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tipo de transferência de saida, nesse caso, todas sao saida' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'tipo_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'valor da transferência de saida. quanto de patrimonio o cliente tinha quando foi transferido para a M7' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'resgate_bruto_transferencia'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'resgate bruto total = valor dos resgates brutos xp + valor da transferência de saida' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates', @level2type=N'COLUMN',@level2name=N'resgate_bruto_total'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'tabela silver que contém os dados de resgates e transferencias de saida. apenas os valores de saida são considerados nessa tabela' , @level0type=N'SCHEMA',@level0name=N'silver', @level1type=N'TABLE',@level1name=N'fact_resgates'
GO

-- ==============================================================================
-- 8. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor           | Descrição
--------|------------|-----------------|--------------------------------------------
1.0.0   | 2024-11-28 | [Nome]         | Criação inicial da tabela

*/

-- ==============================================================================
-- 9. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Esta tabela armazena apenas valores de SAÍDA (resgates e transferências)
- Todos os valores são armazenados como NEGATIVOS para facilitar cálculos
- A procedure prc_bronze_to_silver_fact_resgates realiza TRUNCATE antes da carga
- Transferências de saída consideram o último patrimônio antes da transferência
- Campo tipo_transferencia terá 'N/A' para resgates normais e 'saída' para transferências

Troubleshooting comum:
1. Valores positivos na tabela: Verificar lógica de sinal na procedure de carga
2. Transferências duplicadas: Validar filtro por status 'CONCLUIDO'
3. Diferenças em resgate_bruto_total: Verificar soma dos componentes

Contato para dúvidas: [equipe-dados@m7investimentos.com.br]
*/
