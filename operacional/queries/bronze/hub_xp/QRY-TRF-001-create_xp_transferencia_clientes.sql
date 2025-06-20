-- ==============================================================================
-- QRY-TRF-001-create_xp_transferencia_clientes
-- ==============================================================================
-- Tipo: Query DDL
-- Versão: 1.0.0
-- Última atualização: 2025-01-17
-- Autor: dados@m7investimentos.com.br
-- Revisor: dados@m7investimentos.com.br
-- Tags: [ddl, bronze, xp, transferencia, clientes, assessores]
-- Status: produção
-- Banco de Dados: SQL Server
-- Schema: bronze
-- ==============================================================================

-- ==============================================================================
-- 1. OBJETIVO
-- ==============================================================================
/*
Descrição: Cria a tabela bronze.xp_transferencia_clientes para armazenar dados 
brutos de transferências de clientes entre assessores. Registra quando um cliente 
foi transferido de um AAI para outro dentro da XP, incluindo datas e status.

Casos de uso:
- Tracking de movimentações de carteira entre assessores
- Análise de retenção e perda de clientes
- Auditoria de transferências aprovadas/rejeitadas
- Base para políticas de transferência

Frequência de execução: Uma vez (criação inicial)
Tempo médio de execução: < 1 segundo
Volume esperado de linhas: 1k-10k transferências/ano
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
Tabela criada: bronze.xp_transferencia_clientes

Colunas principais:
- cod_xp: Cliente sendo transferido
- cod_aai_origem: Assessor que perde o cliente
- cod_aai_destino: Assessor que recebe o cliente
- data_solicitacao/data_transferencia: Controle temporal
- status: Situação da transferência
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
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[xp_transferencia_clientes]') AND type in (N'U'))
BEGIN
    PRINT 'Tabela bronze.xp_transferencia_clientes já existe. Dropando...';
    DROP TABLE [bronze].[xp_transferencia_clientes];
END
GO

-- ==============================================================================
-- 7. QUERY PRINCIPAL - CRIAÇÃO DA TABELA
-- ==============================================================================

CREATE TABLE [bronze].[xp_transferencia_clientes](
	[cod_xp] [int] NULL,
	[cod_aai_origem] [varchar](50) NULL,
	[cod_aai_destino] [varchar](50) NULL,
	[data_solicitacao] [date] NULL,
	[data_transferencia] [date] NULL,
	[status] [varchar](50) NULL,
	[data_carga] [date] NOT NULL
) ON [PRIMARY];
GO

ALTER TABLE [bronze].[xp_transferencia_clientes] ADD DEFAULT (CONVERT([date],getdate())) FOR [data_carga];
GO

-- ==============================================================================
-- 8. ÍNDICES E OTIMIZAÇÕES
-- ==============================================================================

-- Índice para busca por cliente
CREATE NONCLUSTERED INDEX [IX_bronze_transf_cliente]
ON [bronze].[xp_transferencia_clientes] ([cod_xp])
INCLUDE ([data_solicitacao], [status]);
GO

-- Índice para análise por assessor origem
CREATE NONCLUSTERED INDEX [IX_bronze_transf_origem]
ON [bronze].[xp_transferencia_clientes] ([cod_aai_origem], [data_solicitacao])
INCLUDE ([cod_xp], [status]);
GO

-- Índice para análise por assessor destino
CREATE NONCLUSTERED INDEX [IX_bronze_transf_destino]
ON [bronze].[xp_transferencia_clientes] ([cod_aai_destino], [data_solicitacao])
INCLUDE ([cod_xp], [status]);
GO

-- Índice para análise por status
CREATE NONCLUSTERED INDEX [IX_bronze_transf_status]
ON [bronze].[xp_transferencia_clientes] ([status], [data_solicitacao])
WHERE [status] IS NOT NULL;
GO

-- ==============================================================================
-- 9. DOCUMENTAÇÃO DE COLUNAS
-- ==============================================================================

-- Descrição da tabela
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Dados brutos de transferências de clientes entre assessores. Registra quando um cliente foi transferido de um AAI para outro dentro da XP', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes';
GO

-- Documentação das colunas
EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do cliente sendo transferido', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', 
    @level2type=N'COLUMN',@level2name=N'cod_xp';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor de origem que está transferindo o cliente', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', 
    @level2type=N'COLUMN',@level2name=N'cod_aai_origem';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Código do assessor de destino que receberá o cliente', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', 
    @level2type=N'COLUMN',@level2name=N'cod_aai_destino';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data em que a transferência foi solicitada', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', 
    @level2type=N'COLUMN',@level2name=N'data_solicitacao';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data em que a transferência foi efetivada', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', 
    @level2type=N'COLUMN',@level2name=N'data_transferencia';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Status atual da transferência (Aprovada, Pendente, Rejeitada)', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', 
    @level2type=N'COLUMN',@level2name=N'status';
GO

EXEC sys.sp_addextendedproperty 
    @name=N'MS_Description', 
    @value=N'Data em que os dados foram carregados no sistema', 
    @level0type=N'SCHEMA',@level0name=N'bronze', 
    @level1type=N'TABLE',@level1name=N'xp_transferencia_clientes', 
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
- Cliente pode ter múltiplas transferências ao longo do tempo
- Status "Aprovada" indica transferência efetivada
- Status "Pendente" requer acompanhamento
- Status "Rejeitada" pode indicar problemas
- Data_transferencia NULL para status Pendente/Rejeitada

Troubleshooting comum:
1. Transferências pendentes antigas: Verificar processos parados
2. Cliente sem assessor após transferência: Validar integridade
3. Transferências em loop: Cliente voltando para origem

Queries úteis:
-- Resumo de transferências por assessor
SELECT 
    cod_aai_origem,
    COUNT(CASE WHEN status = 'Aprovada' THEN 1 END) as perdas_aprovadas,
    COUNT(CASE WHEN status = 'Pendente' THEN 1 END) as perdas_pendentes,
    COUNT(CASE WHEN status = 'Rejeitada' THEN 1 END) as perdas_rejeitadas
FROM bronze.xp_transferencia_clientes
WHERE data_solicitacao >= DATEADD(MONTH, -6, GETDATE())
GROUP BY cod_aai_origem
UNION ALL
SELECT 
    cod_aai_destino,
    COUNT(CASE WHEN status = 'Aprovada' THEN 1 END) * -1 as ganhos_aprovados,
    COUNT(CASE WHEN status = 'Pendente' THEN 1 END) * -1 as ganhos_pendentes,
    COUNT(CASE WHEN status = 'Rejeitada' THEN 1 END) * -1 as ganhos_rejeitados
FROM bronze.xp_transferencia_clientes
WHERE data_solicitacao >= DATEADD(MONTH, -6, GETDATE())
GROUP BY cod_aai_destino
ORDER BY 1;

-- Tempo médio de aprovação
SELECT 
    YEAR(data_solicitacao) as ano,
    MONTH(data_solicitacao) as mes,
    COUNT(*) as qtd_transferencias,
    AVG(DATEDIFF(DAY, data_solicitacao, data_transferencia)) as dias_medio_aprovacao
FROM bronze.xp_transferencia_clientes
WHERE status = 'Aprovada'
    AND data_transferencia IS NOT NULL
GROUP BY YEAR(data_solicitacao), MONTH(data_solicitacao)
ORDER BY ano DESC, mes DESC;

-- Clientes com múltiplas transferências
SELECT 
    cod_xp,
    COUNT(*) as qtd_transferencias,
    MIN(data_solicitacao) as primeira_transf,
    MAX(data_solicitacao) as ultima_transf,
    STRING_AGG(CONCAT(cod_aai_origem, '->', cod_aai_destino), ', ') 
        WITHIN GROUP (ORDER BY data_solicitacao) as historico
FROM bronze.xp_transferencia_clientes
WHERE status = 'Aprovada'
GROUP BY cod_xp
HAVING COUNT(*) > 1
ORDER BY qtd_transferencias DESC;

-- Taxa de aprovação por período
SELECT 
    YEAR(data_solicitacao) as ano,
    MONTH(data_solicitacao) as mes,
    COUNT(*) as total_solicitacoes,
    COUNT(CASE WHEN status = 'Aprovada' THEN 1 END) as aprovadas,
    COUNT(CASE WHEN status = 'Rejeitada' THEN 1 END) as rejeitadas,
    COUNT(CASE WHEN status = 'Pendente' THEN 1 END) as pendentes,
    CAST(COUNT(CASE WHEN status = 'Aprovada' THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as taxa_aprovacao
FROM bronze.xp_transferencia_clientes
GROUP BY YEAR(data_solicitacao), MONTH(data_solicitacao)
ORDER BY ano DESC, mes DESC;

Contato para dúvidas: arquitetura.dados@m7investimentos.com.br
*/

-- Confirmar criação
PRINT 'Tabela bronze.xp_transferencia_clientes criada com sucesso!';
GO
