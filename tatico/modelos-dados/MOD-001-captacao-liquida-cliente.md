# MOD-001-captacao-liquida-cliente

---
título: Modelo de Dados - Captação Líquida por Cliente
tipo: MOD - Modelo de Dados
versão: 1.0.0
última_atualização: 2025-01-16
autor: bruno.chiaramonti@multisete.com
aprovador: bruno.chiaramonti@multisete.com
tags: [modelo, dados, gold, captacao, cliente, dimensional]
status: aprovado
dependências:
  - tipo: view
    ref: QRY-CAP-004-create_gold_view_captacao_liquida_cliente
    repo: datawarehouse-docs
  - tipo: tabela
    ref: QRY-CAP-005-create_gold_captacao_liquida_cliente
    repo: datawarehouse-docs
  - tipo: procedure
    ref: QRY-CAP-006-prc_gold_to_table_captacao_liquida_cliente
    repo: datawarehouse-docs
---

## 1. Objetivo

Este modelo de dados documenta a estrutura da tabela `gold.captacao_liquida_cliente`, que consolida métricas de captação líquida (captação bruta - resgates) por cliente em base mensal. O modelo suporta análises de comportamento individual de clientes, identificação de padrões de investimento, segmentação avançada e modelos preditivos de churn.

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: Comercial/Financeiro - Gestão de Investimentos
- **Processos suportados**: 
  - Análise de performance de carteira por assessor
  - Identificação de clientes em risco (churn)
  - Segmentação de clientes por comportamento
  - Relatórios executivos de captação líquida
  - Modelos preditivos de resgate
- **Stakeholders**: 
  - Assessores de investimento
  - Gestores comerciais
  - Equipe de analytics
  - Diretoria executiva

### 2.2 Contexto Técnico
- **Tipo de modelo**: Dimensional (Fato agregado mensal)
- **Plataforma**: SQL Server
- **Database**: M7Medallion
- **Schema**: gold
- **Layer**: Gold (Camada de consumo/análise)

## 3. Visão Geral do Modelo

### 3.1 Diagrama Entidade-Relacionamento
```
┌─────────────────────────┐        ┌──────────────────────┐
│  dim_clientes (silver)  │        │  dim_pessoas (silver)│
├─────────────────────────┤        ├──────────────────────┤
│ PK: cod_xp              │        │ PK: crm_id           │
│ nome_cliente            │        │ cod_aai              │
│ cpf/cnpj               │        │ nome_pessoa          │
│ grupo_cliente          │        │ assessor_nivel       │
│ data_cadastro          │        └──────────────────────┘
└───────────┬─────────────┘                   
            │                                  
            │     ┌─────────────────────────────────┐     
            │     │ fact_cliente_perfil_historico   │     
            │     │           (silver)              │     
            │     ├─────────────────────────────────┤     
            │     │ PK: conta_xp_cliente, data_ref  │     
            └────►│ FK: conta_xp_cliente            │     
                  │ patrimonio_total                │     
                  │ perfil_api                      │     
                  │ assessor_xp                     │     
                  └─────────────────────────────────┘     
                                                          
┌─────────────────────────────────────────────────────────┐
│            gold.captacao_liquida_cliente                │
├─────────────────────────────────────────────────────────┤
│ PK: data_ref, conta_xp_cliente, cod_assessor           │
│ captacao_bruta_total                                    │
│ resgate_bruto_total                                     │
│ captacao_liquida_total                                  │
│ qtd_operacoes_aporte/resgate                           │
│ ticket_medio_aporte/resgate                            │
│ meses_como_cliente                                      │
└─────────────────────────────────────────────────────────┘
            ▲                                  ▲
            │                                  │
┌───────────┴─────────────┐        ┌──────────┴───────────┐
│ fact_captacao_bruta     │        │  fact_resgates       │
│      (silver)           │        │     (silver)         │
├─────────────────────────┤        ├──────────────────────┤
│ data_ref                │        │ data_ref             │
│ conta_xp_cliente        │        │ conta_xp_cliente     │
│ cod_assessor            │        │ cod_assessor         │
│ captacao_bruta_total    │        │ resgate_bruto_total  │
└─────────────────────────┘        └──────────────────────┘
```

### 3.2 Principais Entidades
| Entidade | Tipo | Descrição | Volume Estimado |
|----------|------|-----------|-----------------|
| captacao_liquida_cliente | Fato Agregado | Métricas mensais de captação por cliente/assessor | 50K-100K registros/mês |
| dim_clientes | Dimensão | Cadastro de clientes | 500K registros |
| dim_pessoas | Dimensão | Cadastro de assessores | 5K registros |
| fact_captacao_bruta | Fato | Operações diárias de captação | 200K registros/mês |
| fact_resgates | Fato | Operações diárias de resgate | 150K registros/mês |
| fact_cliente_perfil_historico | Fato Histórico | Histórico mensal de perfil e patrimônio do cliente | 200K registros/mês |

## 4. Dicionário de Dados Detalhado

### 4.1 Tabela: gold.captacao_liquida_cliente

**Descrição**: Tabela materializada que armazena dados consolidados de captação líquida por cliente e assessor em base mensal, com métricas de comportamento e tendências.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| data_ref | DATE | PK, NOT NULL | Data de referência (último dia do mês) | 2024-12-31 | Sempre último dia disponível do mês |
| conta_xp_cliente | INT | PK, NOT NULL | Código da conta do cliente | 12345 | Referência: silver.dim_clientes.cod_xp |
| cod_assessor | VARCHAR(50) | PK, NOT NULL | Código do assessor responsável | 'AAI123' | Referência: silver.dim_pessoas.cod_aai |
| ano | INT | NOT NULL | Ano de referência | 2024 | Extraído de data_ref |
| mes | INT | NOT NULL | Mês de referência | 12 | Extraído de data_ref |
| nome_mes | VARCHAR(20) | NULL | Nome do mês por extenso | 'Dezembro' | Português BR |
| trimestre | CHAR(2) | NULL | Trimestre do ano | 'Q4' | Formato: Q1-Q4 |
| nome_cliente | VARCHAR(200) | NULL | Nome do cliente | 'João Silva' | Pode ser NULL se não cadastrado |
| tipo_cliente | VARCHAR(12) | NULL | Tipo de cliente (PF/PJ) | 'PF' | Derivado de CPF/CNPJ |
| grupo_cliente | VARCHAR(100) | NULL | Grupo econômico do cliente | 'Grupo ABC' | NULL para clientes sem grupo |
| segmento_cliente | VARCHAR(50) | NULL | Segmentação do cliente | 'Private' | Campo reservado (não implementado) |
| status_cliente | VARCHAR(50) | NULL | Status do cliente | 'Ativo' | Campo reservado (não implementado) |
| faixa_etaria | VARCHAR(50) | NULL | Faixa etária do cliente | '40-50 anos' | Campo reservado (não implementado) |
| codigo_cliente_crm | VARCHAR(100) | NULL | Código CRM do cliente | 'CLI123' | Integração com CRM |
| nome_assessor | VARCHAR(200) | NULL | Nome do assessor | 'Maria Santos' | Referência: silver.dim_pessoas |
| assessor_nivel | VARCHAR(50) | NULL | Nível do assessor | 'Senior' | Classificação interna |
| assessor_status | VARCHAR(50) | NULL | Status do assessor | 'Ativo' | Baseado em data_fim_vigencia |
| codigo_assessor_crm | VARCHAR(20) | NULL | Código CRM do assessor | 'CRM123' | Integração com CRM |
| nome_estrutura | VARCHAR(100) | NULL | Estrutura do assessor | 'Equipe SP' | Referência: silver.dim_estruturas |
| captacao_bruta_xp | DECIMAL(18,2) | NULL | Captação bruta via XP | 50000.00 | Valores positivos |
| captacao_bruta_transferencia | DECIMAL(18,2) | NULL | Captação bruta via transferência | 0.00 | Valores positivos |
| captacao_bruta_total | DECIMAL(18,2) | NULL | Total de captação bruta | 50000.00 | Soma de XP + transferência |
| resgate_bruto_xp | DECIMAL(18,2) | NULL | Resgates via XP | -10000.00 | Valores negativos |
| resgate_bruto_transferencia | DECIMAL(18,2) | NULL | Resgates via transferência | 0.00 | Valores negativos |
| resgate_bruto_total | DECIMAL(18,2) | NULL | Total de resgates | -10000.00 | Soma de XP + transferência |
| captacao_liquida_xp | DECIMAL(18,2) | NULL | Captação líquida XP | 40000.00 | captacao_bruta_xp + resgate_bruto_xp |
| captacao_liquida_transferencia | DECIMAL(18,2) | NULL | Captação líquida transferência | 0.00 | captacao_bruta_transf + resgate_bruto_transf |
| captacao_liquida_total | DECIMAL(18,2) | NULL | Captação líquida total | 40000.00 | captacao_bruta_total + resgate_bruto_total |
| qtd_operacoes_aporte | INT | NULL | Número de operações de aporte no mês | 3 | Contador de transações |
| qtd_operacoes_resgate | INT | NULL | Número de operações de resgate no mês | 1 | Contador de transações |
| ticket_medio_aporte | DECIMAL(18,2) | NULL | Valor médio por operação de aporte | 16666.67 | captacao_bruta_total / qtd_operacoes |
| ticket_medio_resgate | DECIMAL(18,2) | NULL | Valor médio por operação de resgate | -10000.00 | resgate_bruto_total / qtd_operacoes |
| meses_como_cliente | INT | NULL | Tempo de relacionamento em meses | 24 | DATEDIFF(MONTH, data_cadastro, data_ref) |
| primeira_captacao | DATE | NULL | Data da primeira captação | 2023-01-15 | MIN(data_ref) com captacao |
| ultima_captacao | DATE | NULL | Data da última captação | 2024-12-20 | MAX(data_ref) com captacao |
| ultimo_resgate | DATE | NULL | Data do último resgate | 2024-11-15 | MAX(data_ref) com resgate |
| data_carga | DATETIME | NOT NULL, DEFAULT | Data e hora da última carga | 2025-01-16 10:30:00 | GETDATE() na carga |
| hash_registro | VARBINARY(32) | NULL | Hash para controle de mudanças | 0x1A2B3C... | SHA2_256 de campos chave |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_captacao_liquida_cliente | CLUSTERED | data_ref, conta_xp_cliente, cod_assessor | Chave primária |
| IX_captacao_liquida_cliente_assessor_periodo | NONCLUSTERED | cod_assessor, ano, mes | Consultas por assessor |
| IX_captacao_liquida_cliente_comportamento | NONCLUSTERED | captacao_liquida_total DESC, ano, mes | Análise de comportamento |
| IX_captacao_liquida_cliente_segmentacao | NONCLUSTERED | tipo_cliente, grupo_cliente, ano, mes | Segmentação |
| IX_captacao_liquida_cliente_churn | NONCLUSTERED FILTERED | ultimo_resgate DESC, ultima_captacao DESC | Identificar clientes em risco |

**Particionamento**: Não implementado (considerar por data_ref se volume > 10M registros)

**Compressão**: PAGE - Ativada para otimizar armazenamento

## 5. Relacionamentos e Integridade

### 5.1 Chaves Estrangeiras
| Tabela Origem | Campo(s) | Tabela Destino | Campo(s) | Tipo | On Delete | On Update |
|---------------|----------|----------------|----------|------|-----------|-----------|
| captacao_liquida_cliente | conta_xp_cliente | silver.dim_clientes | cod_xp | N:1 | N/A* | N/A* |
| captacao_liquida_cliente | cod_assessor | silver.dim_pessoas | cod_aai | N:1 | N/A* | N/A* |

*Nota: FKs não implementadas fisicamente por estar em schema diferente (gold vs silver)

### 5.2 Cardinalidade dos Relacionamentos
- **Cliente → Captação Líquida**: 1:N (Um cliente pode ter múltiplos registros mensais)
- **Assessor → Captação Líquida**: 1:N (Um assessor atende múltiplos clientes)
- **Cliente+Assessor+Mês → Captação Líquida**: 1:1 (Combinação única)

## 6. Regras de Negócio e Validações

### 6.1 Regras de Integridade
| Regra | Implementação | Descrição |
|-------|---------------|-----------|
| RN001 | Chave primária composta | Combinação data_ref + conta_xp_cliente + cod_assessor deve ser única |
| RN002 | Cálculo captação líquida | captacao_liquida_total = captacao_bruta_total + resgate_bruto_total |
| RN003 | Valores de resgate | Resgates sempre armazenados como valores negativos |
| RN004 | Data referência | Sempre último dia disponível do mês |
| RN005 | Ticket médio | Calculado apenas quando qtd_operacoes > 0 |

### 6.2 Implementação das Regras
```sql
-- RN002: Validação do cálculo de captação líquida (implementado na view)
COALESCE(mcc.captacao_bruta_total, 0) + COALESCE(mrc.resgate_bruto_total, 0) AS captacao_liquida_total

-- RN004: Último dia do mês (implementado na CTE ultimo_dia_mes)
SELECT 
    YEAR(data_ref) AS ano,
    MONTH(data_ref) AS mes,
    MAX(data_ref) AS ultimo_dia_disponivel
FROM [silver].[fact_captacao_bruta]
GROUP BY YEAR(data_ref), MONTH(data_ref)
```

## 7. Histórico e Auditoria

### 7.1 Estratégia de Historização
- **Tipo**: Snapshot mensal (não é SCD)
- **Campos de controle**:
  - `data_carga`: Timestamp da última atualização
  - `hash_registro`: Hash SHA2_256 para detectar mudanças

### 7.2 Controle de Mudanças
- Procedure `prc_gold_to_table_captacao_liquida_cliente` implementa MERGE
- Atualiza apenas registros que mudaram (baseado no hash)
- Mantém histórico completo (sem deleção de dados antigos)

## 8. Performance e Otimização

### 8.1 Estratégias de Indexação
| Padrão de Query | Índice Utilizado | Justificativa |
|-----------------|------------------|---------------|
| Análise por assessor/período | IX_captacao_liquida_cliente_assessor_periodo | Cobre queries de performance por assessor |
| Ranking de captação | IX_captacao_liquida_cliente_comportamento | Ordenação por captacao_liquida_total |
| Segmentação PF/PJ | IX_captacao_liquida_cliente_segmentacao | Filtros por tipo_cliente |
| Identificação de churn | IX_captacao_liquida_cliente_churn | Filtered index para captacao < 0 |

### 8.2 Estatísticas e Manutenção
```sql
-- Atualização de estatísticas após carga (implementado na procedure)
UPDATE STATISTICS [gold].[captacao_liquida_cliente];

-- Monitoramento de fragmentação
SELECT 
    index_id,
    index_type_desc,
    avg_fragmentation_in_percent,
    page_count
FROM sys.dm_db_index_physical_stats(
    DB_ID('M7Medallion'),
    OBJECT_ID('gold.captacao_liquida_cliente'),
    NULL, NULL, 'LIMITED'
);
```

## 9. Segurança e Privacidade

### 9.1 Classificação de Dados
| Campo | Classificação | Tratamento |
|-------|---------------|------------|
| nome_cliente | PII - Confidencial | Acesso controlado por role |
| codigo_cliente_crm | Confidencial | Acesso controlado |
| valores financeiros | Confidencial | Acesso por role comercial |

### 9.2 Políticas de Acesso
```sql
-- Roles sugeridas (a implementar)
-- GRANT SELECT ON [gold].[captacao_liquida_cliente] TO [role_gold_read];
-- GRANT SELECT ON [gold].[captacao_liquida_cliente] TO [role_assessor] 
--     WHERE cod_assessor = USER_NAME(); -- Row Level Security
```

## 10. Integração e Dependências

### 10.1 Pipeline de Dados
```
1. Dados Bronze (arquivos Excel/CSV)
   ↓
2. ETL Bronze → Silver (procedures modelagem_b2s)
   ↓
3. Tabelas Silver (fact_captacao_bruta, fact_resgates)
   ↓
4. View Gold (view_captacao_liquida_cliente)
   ↓
5. Procedure ETL (prc_gold_to_table_captacao_liquida_cliente)
   ↓
6. Tabela Gold (captacao_liquida_cliente)
```

### 10.2 Sistemas Consumidores
| Sistema | Uso | Requisitos |
|---------|-----|------------|
| Power BI | Dashboards de captação | Atualização diária |
| Excel/Access | Relatórios ad-hoc | Acesso via views |
| Python/R | Modelos preditivos | Acesso read-only |

## 11. Evolução e Versionamento

### 11.1 Histórico de Versões
| Versão | Data | Autor | Mudanças |
|--------|------|-------|----------|
| 1.0.0 | 2025-01-16 | Bruno Chiaramonti | Criação inicial |
| 1.1.0 | 2025-01-16 | Bruno Chiaramonti | Alteração PK para incluir cod_assessor |

### 11.2 Mudanças Planejadas
- Implementar campos de segmentação (segmento_cliente, status_cliente)
- Adicionar métricas de produtos mais captados
- Considerar particionamento por ano para volumes > 10M

## 12. Qualidade e Governança

### 12.1 Regras de Qualidade
| Dimensão | Métrica | Target | Medição |
|----------|---------|--------|---------|
| Completude | % nome_cliente preenchido | > 98% | Mensal |
| Unicidade | % duplicatas na PK | 0% | A cada carga |
| Consistência | captacao_liquida = bruta + resgate | 100% | A cada carga |
| Atualidade | Lag da última carga | < 24h | Diário |

### 12.2 Validações Implementadas
```sql
-- Verificar consistência de cálculos
SELECT COUNT(*) as inconsistencias
FROM [gold].[captacao_liquida_cliente]
WHERE ABS(captacao_liquida_total - (captacao_bruta_total + resgate_bruto_total)) > 0.01;

-- Verificar completude de dados
SELECT 
    COUNT(*) as total_registros,
    COUNT(nome_cliente) as clientes_com_nome,
    COUNT(nome_assessor) as assessores_identificados,
    CAST(COUNT(nome_cliente) AS FLOAT) / COUNT(*) * 100 as pct_completude_cliente
FROM [gold].[captacao_liquida_cliente]
WHERE ano = YEAR(GETDATE()) AND mes = MONTH(GETDATE()) - 1;
```

## 13. Exemplos e Casos de Uso

### 13.1 Queries Comuns
```sql
-- Top 10 clientes por captação líquida no mês
SELECT TOP 10
    conta_xp_cliente,
    nome_cliente,
    nome_assessor,
    captacao_liquida_total,
    qtd_operacoes_aporte,
    ticket_medio_aporte
FROM [gold].[captacao_liquida_cliente]
WHERE ano = 2024 AND mes = 12
ORDER BY captacao_liquida_total DESC;

-- Clientes em risco (3 meses consecutivos de resgate líquido)
WITH resgates_consecutivos AS (
    SELECT 
        conta_xp_cliente,
        nome_cliente,
        captacao_liquida_total,
        LAG(captacao_liquida_total, 1) OVER (PARTITION BY conta_xp_cliente ORDER BY data_ref) AS mes_anterior_1,
        LAG(captacao_liquida_total, 2) OVER (PARTITION BY conta_xp_cliente ORDER BY data_ref) AS mes_anterior_2
    FROM [gold].[captacao_liquida_cliente]
)
SELECT DISTINCT
    conta_xp_cliente,
    nome_cliente
FROM resgates_consecutivos
WHERE captacao_liquida_total < 0 
  AND mes_anterior_1 < 0 
  AND mes_anterior_2 < 0;

-- Performance por assessor
SELECT 
    cod_assessor,
    nome_assessor,
    COUNT(DISTINCT conta_xp_cliente) as qtd_clientes,
    SUM(captacao_bruta_total) as captacao_bruta_total,
    SUM(resgate_bruto_total) as resgate_bruto_total,
    SUM(captacao_liquida_total) as captacao_liquida_total,
    AVG(ticket_medio_aporte) as ticket_medio_aporte
FROM [gold].[captacao_liquida_cliente]
WHERE ano = 2024
GROUP BY cod_assessor, nome_assessor
ORDER BY captacao_liquida_total DESC;
```

## 14. Troubleshooting

### 14.1 Problemas Comuns
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| Dados duplicados | Múltiplas linhas para cliente/mês | Verificar PK e view source | Revisar JOIN com cod_assessor |
| Performance lenta | Queries > 30s | Verificar execution plan | Atualizar estatísticas |
| Dados faltantes | Clientes sem nome | JOIN com dim_clientes | Verificar integridade silver |
| Valores incorretos | Captação líquida errada | Validar cálculos | Verificar sinais dos resgates |

### 14.2 Scripts de Diagnóstico
```sql
-- Verificar volume por período
SELECT 
    ano, 
    mes,
    COUNT(*) as registros,
    COUNT(DISTINCT conta_xp_cliente) as clientes_unicos,
    COUNT(DISTINCT cod_assessor) as assessores_ativos
FROM [gold].[captacao_liquida_cliente]
GROUP BY ano, mes
ORDER BY ano DESC, mes DESC;

-- Identificar assessores com mudanças frequentes
SELECT 
    conta_xp_cliente,
    COUNT(DISTINCT cod_assessor) as qtd_assessores
FROM [gold].[captacao_liquida_cliente]
WHERE ano = 2024
GROUP BY conta_xp_cliente
HAVING COUNT(DISTINCT cod_assessor) > 3
ORDER BY qtd_assessores DESC;
```

## 15. Referências e Anexos

### 15.1 Scripts DDL Completos
- [QRY-CAP-004-create_gold_view_captacao_liquida_cliente.sql](../operacional/queries/gold/QRY-CAP-004-create_gold_view_captacao_liquida_cliente.sql)
- [QRY-CAP-005-create_gold_captacao_liquida_cliente.sql](../operacional/queries/gold/QRY-CAP-005-create_gold_captacao_liquida_cliente.sql)
- [QRY-CAP-006-prc_gold_to_table_captacao_liquida_cliente.sql](../operacional/queries/gold/QRY-CAP-006-prc_gold_to_table_captacao_liquida_cliente.sql)

### 15.2 Documentação Relacionada
- [ETL-001-processo-carga-bronze.md](../tatico/processos-etl/README.md)
- [IND-001-metricas-captacao.md](../tatico/metricas/README.md)

### 15.3 Ferramentas e Recursos
- SQL Server Management Studio (SSMS)
- Visual Studio Code com extensão SQL
- Power BI para visualização

---

**Documento criado por**: Bruno Chiaramonti - Multisete Consultoria  
**Última revisão**: 2025-01-16