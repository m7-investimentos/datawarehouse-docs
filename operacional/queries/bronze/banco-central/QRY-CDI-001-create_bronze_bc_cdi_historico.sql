-- ==============================================================================
-- QRY-CDI-001-create_bronze_bc_cdi_historico
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, banco_central, cdi, historico, taxas]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.bc_cdi_historico para armazenar o histórico 
diário bruto das taxas CDI (Certificado de Depósito Interbancário) conforme 
divulgado pelo Banco Central do Brasil.

Casos de uso:
- Armazenamento de taxas CDI diárias
- Base para cálculos de rentabilidade
- Referência para produtos atrelados ao CDI
- Análises históricas de taxas de juros

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: ~250 registros/ano (dias úteis)
*/

-- ==============================================================================
-- 2. PARÂMETROS DE ENTRADA
-- ==============================================================================
/*
Parâmetros necessários para execução:
N/A - Script DDL sem parâmetros

Exemplo de uso:
USE M7Medallion;
GO
-- Executar script completo
*/

-- ==============================================================================
-- 3. ESTRUTURA DE SAÍDA
-- ==============================================================================
/*
Tabela criada: bronze.bc_cdi_historico

Colunas principais:
- data_ref: Data de referência da taxa
- taxa_cdi: Taxa CDI do dia (percentual)
- data_carga: Timestamp de carga
*/

-- ==============================================================================
-- 4. DEPENDÊNCIAS
-- ==============================================================================
/*
Tabelas/Views utilizadas:
N/A

Pré-requisitos:
- Schema bronze deve existir
- Permissões CREATE TABLE no schema bronze
*/

-- ==============================================================================
-- 5. CONFIGURAÇÕES E OTIMIZAÇÕES
-- ==============================================================================
USE M7Medallion;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- ==============================================================================
-- 6. VERIFICAÇÃO E LIMPEZA
-- ==============================================================================

-- Verificar se a tabela já existe
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[bc_cdi_historico]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.bc_cdi_historico já existe. Dropando...';
    DROP TABLE [bronze].[bc_cdi_historico];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[bc_cdi_historico](
	[data_ref] [date] NOT NULL,
	[taxa_cdi] [decimal](18, 8) NOT NULL,
	[data_carga] [datetime] NOT NULL
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[bc_cdi_historico] ADD DEFAULT (getdate()) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice clustered na data de referência (chave natural)
CREATE UNIQUE CLUSTERED INDEX [PK_bronze_bc_cdi_historico]
ON [bronze].[bc_cdi_historico] ([data_ref] ASC)
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, 
      IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, 
      ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
GO

-- Índice para busca por período
CREATE NONCLUSTERED INDEX [IX_bronze_cdi_periodo]
ON [bronze].[bc_cdi_historico] ([data_ref] DESC)
INCLUDE ([taxa_cdi]);
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Tabela bronze que armazena o histórico diário bruto das taxas CDI (Certificado de Depósito Interbancário) conforme divulgado pelo Banco Central do Brasil. Contém apenas dias úteis e representa a taxa de juros praticada em operações entre bancos. Fonte de dados provavelmente oriunda de API do BACEN ou sistema interno de captura', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'bc_cdi_historico';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data de referência da taxa CDI. Representa o dia útil específico da cotação, não incluindo finais de semana ou feriados bancários. Chave primária natural da tabela', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'bc_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'data_ref';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Taxa CDI do dia em formato percentual. Valor representa percentual ao dia (ex: 0.05426600 = 0,054266% a.d.). Precisão de 8 casas decimais. Range histórico observado: 0.03927000 a 0.05426600. Média histórica aproximada: 0.04484157', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'bc_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'taxa_cdi';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Timestamp de carga dos dados no data warehouse. Momento exato em que o registro foi inserido na tabela bronze. Usado para auditoria e controle de processo ETL. Formato datetime com precisão de milissegundos', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'bc_cdi_historico', 
    @level2type=N'COLUMN',@level2name=N'data_carga';
GO

-- ==============================================================================
-- 10. HISTÓRICO DE MUDANÇAS
-- ==============================================================================
/*
Versão  | Data       | Autor                    | Descrição
--------|------------|--------------------------|--------------------------------------------
1.0.0   | 2025-01-17 | arquitetura.dados       | Criação inicial da tabela

*/

-- ==============================================================================
-- 11. NOTAS E OBSERVAÇÕES
-- ==============================================================================
/*
Notas importantes:
- Taxa CDI em formato percentual ao dia (a.d.)
- Contém apenas dias úteis bancários
- Fonte primária: Banco Central do Brasil
- Usado como referência para cálculos de rentabilidade
- Precisão de 8 casas decimais para máxima acurácia

Troubleshooting comum:
1. Gaps nas datas: Normal, representa finais de semana e feriados
2. Conversão para taxa mensal: (1 + taxa_cdi)^21 - 1 (considerando 21 dias úteis)
3. Conversão para taxa anual: (1 + taxa_cdi)^252 - 1 (considerando 252 dias úteis)

Queries úteis:
-- Taxa CDI mais recente
SELECT TOP 1 data_ref, taxa_cdi 
FROM bronze.bc_cdi_historico 
ORDER BY data_ref DESC;

-- Média mensal
SELECT 
    YEAR(data_ref) as ano,
    MONTH(data_ref) as mes,
    AVG(taxa_cdi) as taxa_media,
    COUNT(*) as dias_uteis
FROM bronze.bc_cdi_historico
GROUP BY YEAR(data_ref), MONTH(data_ref)
ORDER BY ano DESC, mes DESC;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.bc_cdi_historico criada com sucesso!';
GO
