# MOD-002-captacao-liquida-assessor

---
título: Modelo de Dados - Captação Líquida por Assessor
tipo: MOD - Modelo de Dados
versão: 1.0.0
última_atualização: 2025-01-16
autor: bruno.chiaramonti@multisete.com
aprovador: bruno.chiaramonti@multisete.com
tags: [modelo, dados, gold, captacao, assessor, dimensional, performance]
status: aprovado
dependências:
  - tipo: view
    ref: QRY-CAP-001-create_gold_view_captacao_liquida_assessor
    repo: datawarehouse-docs
  - tipo: tabela
    ref: QRY-CAP-002-create_gold_captacao_liquida_assessor
    repo: datawarehouse-docs
  - tipo: procedure
    ref: QRY-CAP-003-prc_gold_to_table_captacao_liquida_assessor
    repo: datawarehouse-docs
---

## 1. Objetivo

Este modelo de dados documenta a estrutura da tabela `gold.captacao_liquida_assessor`, que consolida métricas de captação líquida (captação bruta - resgates) por assessor em base mensal. O modelo suporta análises de performance individual e comparativa de assessores, dashboards executivos, acompanhamento de metas e identificação de padrões de comportamento de clientes por assessor.

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: Comercial/Financeiro - Gestão de Performance
- **Processos suportados**: 
  - Análise de performance mensal de assessores
  - Dashboards executivos de captação líquida
  - Relatórios comparativos entre estruturas
  - Acompanhamento de metas individuais
  - Identificação de best practices
- **Stakeholders**: 
  - Assessores de investimento
  - Gestores de estruturas
  - Diretoria comercial
  - Equipe de RH (avaliação de performance)

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
│  dim_pessoas (silver)   │        │ dim_estruturas       │
├─────────────────────────┤        │    (silver)          │
│ PK: crm_id              │        ├──────────────────────┤
│ cod_aai (assessor)      │        │ PK: id_estrutura     │
│ nome_pessoa             │        │ nome_estrutura       │
│ assessor_nivel          │        │ tipo_estrutura       │
└───────────┬─────────────┘        └──────────┬───────────┘
            │                                  │
            │                                  │
            ▼                                  ▼
┌─────────────────────────────────────────────────────────┐
│            gold.captacao_liquida_assessor               │
├─────────────────────────────────────────────────────────┤
│ PK: data_ref, cod_assessor                              │
│ captacao_bruta_total                                    │
│ resgate_bruto_total                                     │
│ captacao_liquida_total                                  │
│ qtd_clientes_aportando/resgatando                      │
│ ticket_medio_aporte/resgate                            │
│ qtd_clientes_apenas_aportando                          │
│ qtd_clientes_apenas_resgatando                         │
│ qtd_clientes_aporte_e_resgate                          │
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
            │                                  │
            ▼                                  ▼
┌─────────────────────────────────────────────────────────┐
│                 fact_estrutura_pessoas                   │
│                       (silver)                           │
├─────────────────────────────────────────────────────────┤
│ crm_id, id_estrutura, data_entrada, data_saida         │
└─────────────────────────────────────────────────────────┘
```

### 3.2 Principais Entidades
| Entidade | Tipo | Descrição | Volume Estimado |
|----------|------|-----------|-----------------|
| captacao_liquida_assessor | Fato Agregado | Métricas mensais de captação por assessor | 500-1K registros/mês |
| dim_pessoas | Dimensão | Cadastro de assessores e funcionários | 5K registros |
| dim_estruturas | Dimensão | Estruturas organizacionais | 100 registros |
| fact_captacao_bruta | Fato | Operações diárias de captação | 200K registros/mês |
| fact_resgates | Fato | Operações diárias de resgate | 150K registros/mês |
| fact_estrutura_pessoas | Fato | Histórico de vinculação pessoa-estrutura | 10K registros |

## 4. Dicionário de Dados Detalhado

### 4.1 Tabela: gold.captacao_liquida_assessor

**Descrição**: Tabela materializada que armazena dados consolidados de captação líquida por assessor em base mensal, incluindo métricas de performance e análise comportamental dos clientes.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| data_ref | DATE | PK, NOT NULL | Data de referência (último dia do mês) | 2024-12-31 | Sempre último dia disponível do mês |
| cod_assessor | VARCHAR(50) | PK, NOT NULL | Código do assessor | 'AAI123' | Referência: silver.dim_pessoas.cod_aai |
| ano | INT | NOT NULL | Ano de referência | 2024 | Extraído de data_ref |
| mes | INT | NOT NULL | Mês de referência | 12 | Extraído de data_ref |
| nome_mes | VARCHAR(20) | NULL | Nome do mês por extenso | 'Dezembro' | Português BR |
| trimestre | CHAR(2) | NULL | Trimestre do ano | 'Q4' | Formato: Q1-Q4 |
| nome_assessor | VARCHAR(200) | NULL | Nome completo do assessor | 'João Silva' | Pode ser NULL se não cadastrado |
| assessor_nivel | VARCHAR(50) | NULL | Nível do assessor | 'Senior' | Classificação hierárquica |
| codigo_assessor_crm | VARCHAR(20) | NULL | Código CRM do assessor | 'CRM123' | Integração com sistema CRM |
| assessor_status | VARCHAR(50) | NULL | Status do assessor | 'Ativo' | Baseado em data_fim_vigencia |
| nome_estrutura | VARCHAR(100) | NULL | Nome da estrutura do assessor | 'Equipe SP' | Estrutura vigente no período |
| captacao_bruta_xp | DECIMAL(18,2) | NULL | Captação bruta via XP | 150000.00 | Valores sempre positivos |
| captacao_bruta_transferencia | DECIMAL(18,2) | NULL | Captação bruta via transferência | 50000.00 | Valores sempre positivos |
| captacao_bruta_total | DECIMAL(18,2) | NULL | Total de captação bruta | 200000.00 | Soma XP + transferência |
| resgate_bruto_xp | DECIMAL(18,2) | NULL | Resgates via XP | -30000.00 | Valores sempre negativos |
| resgate_bruto_transferencia | DECIMAL(18,2) | NULL | Resgates via transferência | -10000.00 | Valores sempre negativos |
| resgate_bruto_total | DECIMAL(18,2) | NULL | Total de resgates | -40000.00 | Soma XP + transferência |
| captacao_liquida_xp | DECIMAL(18,2) | NULL | Captação líquida XP | 120000.00 | captacao_bruta_xp + resgate_bruto_xp |
| captacao_liquida_transferencia | DECIMAL(18,2) | NULL | Captação líquida transferência | 40000.00 | captacao_bruta_transf + resgate_bruto_transf |
| captacao_liquida_total | DECIMAL(18,2) | NULL | Captação líquida total | 160000.00 | captacao_bruta_total + resgate_bruto_total |
| qtd_clientes_aportando | INT | NULL | Clientes únicos que fizeram aportes | 45 | COUNT(DISTINCT) com captacao > 0 |
| qtd_clientes_resgatando | INT | NULL | Clientes únicos que fizeram resgates | 12 | COUNT(DISTINCT) com resgate < 0 |
| ticket_medio_aporte | DECIMAL(18,2) | NULL | Valor médio por operação de aporte | 3500.00 | captacao_total / qtd_operacoes |
| ticket_medio_resgate | DECIMAL(18,2) | NULL | Valor médio por operação de resgate | -2500.00 | resgate_total / qtd_operacoes |
| qtd_clientes_apenas_aportando | INT | NULL | Clientes que só aportaram no mês | 35 | Sem resgates no período |
| qtd_clientes_apenas_resgatando | INT | NULL | Clientes que só resgataram no mês | 5 | Sem aportes no período |
| qtd_clientes_aporte_e_resgate | INT | NULL | Clientes com aportes E resgates | 10 | Ambas operações no período |
| data_carga | DATETIME | NOT NULL, DEFAULT | Data e hora da última carga | 2025-01-16 06:30:00 | GETDATE() na carga |
| hash_registro | VARBINARY(32) | NULL | Hash para controle de mudanças | 0x1A2B3C... | SHA2_256 de campos chave |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_captacao_liquida_assessor | CLUSTERED | data_ref, cod_assessor | Chave primária |
| IX_captacao_liquida_assessor_periodo | NONCLUSTERED | ano, mes | Consultas por período |
| IX_captacao_liquida_assessor_estrutura | NONCLUSTERED | nome_estrutura, ano, mes | Análise por estrutura |
| IX_captacao_liquida_assessor_performance | NONCLUSTERED | captacao_liquida_total DESC, ano, mes | Ranking de performance |

**Particionamento**: Não implementado (volume pequeno)

**Compressão**: PAGE - Ativada para otimizar armazenamento

## 5. Relacionamentos e Integridade

### 5.1 Chaves Estrangeiras
| Tabela Origem | Campo(s) | Tabela Destino | Campo(s) | Tipo | On Delete | On Update |
|---------------|----------|----------------|----------|------|-----------|-----------|
| captacao_liquida_assessor | cod_assessor | silver.dim_pessoas | cod_aai | N:1 | N/A* | N/A* |

*Nota: FKs não implementadas fisicamente por estar em schema diferente (gold vs silver)

### 5.2 Cardinalidade dos Relacionamentos
- **Assessor → Captação Líquida**: 1:N (Um assessor tem múltiplos registros mensais)
- **Estrutura → Assessor**: 1:N (Uma estrutura tem vários assessores)
- **Assessor+Mês → Captação Líquida**: 1:1 (Combinação única)

## 6. Regras de Negócio e Validações

### 6.1 Regras de Integridade
| Regra | Implementação | Descrição |
|-------|---------------|-----------|
| RN001 | Chave primária composta | Combinação data_ref + cod_assessor deve ser única |
| RN002 | Cálculo captação líquida | captacao_liquida_total = captacao_bruta_total + resgate_bruto_total |
| RN003 | Valores de resgate | Resgates sempre armazenados como valores negativos |
| RN004 | Data referência | Sempre último dia disponível do mês |
| RN005 | Comportamento clientes | qtd_apenas_aportando + qtd_apenas_resgatando + qtd_aporte_e_resgate = qtd_clientes_total |
| RN006 | Estrutura vigente | Usar estrutura do assessor vigente no último dia do mês |

### 6.2 Implementação das Regras
```sql
-- RN002: Validação do cálculo de captação líquida
captacao_liquida_total = captacao_bruta_total + resgate_bruto_total

-- RN005: Análise de comportamento (implementado na CTE analise_comportamento)
qtd_clientes_apenas_aportando = COUNT(CASE WHEN resgate_total IS NULL THEN 1 END)
qtd_clientes_apenas_resgatando = COUNT(CASE WHEN captacao_total IS NULL THEN 1 END)
qtd_clientes_aporte_e_resgate = COUNT(CASE WHEN captacao_total IS NOT NULL AND resgate_total IS NOT NULL THEN 1 END)

-- RN006: Estrutura vigente (CTE estrutura_assessor_periodo)
LEFT JOIN fact_estrutura_pessoas fep 
    ON p.crm_id = fep.crm_id
    AND udm.ultimo_dia_disponivel >= fep.data_entrada
    AND udm.ultimo_dia_disponivel <= COALESCE(fep.data_saida, '9999-12-31')
```

## 7. Histórico e Auditoria

### 7.1 Estratégia de Historização
- **Tipo**: Snapshot mensal (não é SCD)
- **Campos de controle**:
  - `data_carga`: Timestamp da última atualização
  - `hash_registro`: Hash SHA2_256 para detectar mudanças

### 7.2 Controle de Mudanças
- Procedure `prc_gold_to_table_captacao_liquida_assessor` implementa MERGE
- Atualiza apenas registros que mudaram (baseado no hash)
- Mantém histórico completo sem deleção

## 8. Performance e Otimização

### 8.1 Estratégias de Indexação
| Padrão de Query | Índice Utilizado | Justificativa |
|-----------------|------------------|---------------|
| Análise temporal | IX_captacao_liquida_assessor_periodo | Filtros por ano/mês |
| Comparação estruturas | IX_captacao_liquida_assessor_estrutura | Agregações por estrutura |
| Ranking assessores | IX_captacao_liquida_assessor_performance | Ordenação por captação líquida |

### 8.2 Estatísticas e Manutenção
```sql
-- Atualização de estatísticas após carga
UPDATE STATISTICS [gold].[captacao_liquida_assessor];

-- Volume esperado: ~10K registros/ano
-- Crescimento: ~1MB/mês
-- Tempo de carga: < 1 minuto
```

## 9. Segurança e Privacidade

### 9.1 Classificação de Dados
| Campo | Classificação | Tratamento |
|-------|---------------|------------|
| nome_assessor | Interno | Acesso controlado |
| valores financeiros | Confidencial | Acesso por role |
| métricas de performance | Restrito | Acesso gerencial |

### 9.2 Políticas de Acesso
```sql
-- Sugestão de implementação
-- Role para leitura geral
-- GRANT SELECT ON [gold].[captacao_liquida_assessor] TO [role_gold_read];

-- Role para assessores (ver apenas próprios dados)
-- CREATE POLICY assessor_proprio_dado ON [gold].[captacao_liquida_assessor]
--     FOR SELECT TO [role_assessor]
--     USING (cod_assessor = USER_NAME());

-- Role para gestores (ver toda estrutura)
-- CREATE POLICY gestor_estrutura ON [gold].[captacao_liquida_assessor]
--     FOR SELECT TO [role_gestor]
--     USING (nome_estrutura IN (SELECT estrutura FROM user_permissions WHERE user = USER_NAME()));
```

## 10. Integração e Dependências

### 10.1 Pipeline de Dados
```
1. Dados Bronze (Excel/CSV diários)
   ↓
2. ETL Bronze → Silver
   ↓
3. Tabelas Silver (captação/resgates)
   ↓
4. View Gold (view_captacao_liquida_assessor)
   ↓
5. Procedure ETL (prc_gold_to_table_captacao_liquida_assessor)
   ↓
6. Tabela Gold (captacao_liquida_assessor)
   ↓
7. Consumo (BI/Analytics)
```

### 10.2 Sistemas Consumidores
| Sistema | Uso | Requisitos |
|---------|-----|------------|
| Power BI | Dashboards executivos | Atualização diária até 7h |
| SharePoint | Relatórios automatizados | Export PDF/Excel |
| Python Analytics | Modelos preditivos | API read-only |
| SQL Agent Jobs | Alertas de performance | Queries scheduled |

## 11. Evolução e Versionamento

### 11.1 Histórico de Versões
| Versão | Data | Autor | Mudanças |
|--------|------|-------|----------|
| 1.0.0 | 2025-01-06 | Bruno Chiaramonti | Criação inicial |
| 1.1.0 | 2025-01-16 | Bruno Chiaramonti | Migração schema gold |

### 11.2 Mudanças Planejadas
- Adicionar métricas de produtos por assessor
- Incluir metas vs realizado
- Implementar score de qualidade de carteira
- Adicionar análise de concentração de clientes

## 12. Qualidade e Governança

### 12.1 Regras de Qualidade
| Dimensão | Métrica | Target | Medição |
|----------|---------|--------|---------|
| Completude | % assessores com nome | 100% | A cada carga |
| Unicidade | Duplicatas na PK | 0% | A cada carga |
| Consistência | Validação comportamento clientes | 100% | A cada carga |
| Atualidade | Lag da última carga | < 24h | Diário |
| Acurácia | Captação líquida = bruta + resgate | 100% | A cada carga |

### 12.2 Validações Implementadas
```sql
-- Verificar consistência de comportamento
SELECT 
    cod_assessor,
    qtd_clientes_aportando,
    qtd_clientes_resgatando,
    qtd_clientes_apenas_aportando,
    qtd_clientes_apenas_resgatando,
    qtd_clientes_aporte_e_resgate,
    CASE 
        WHEN qtd_clientes_aportando - qtd_clientes_aporte_e_resgate <> qtd_clientes_apenas_aportando THEN 'ERRO'
        WHEN qtd_clientes_resgatando - qtd_clientes_aporte_e_resgate <> qtd_clientes_apenas_resgatando THEN 'ERRO'
        ELSE 'OK'
    END as validacao
FROM [gold].[captacao_liquida_assessor]
WHERE ano = YEAR(GETDATE()) AND mes = MONTH(GETDATE()) - 1;

-- Verificar assessores sem estrutura
SELECT COUNT(*) as assessores_sem_estrutura
FROM [gold].[captacao_liquida_assessor]
WHERE nome_estrutura IS NULL
  AND ano = YEAR(GETDATE());
```

## 13. Exemplos e Casos de Uso

### 13.1 Queries Comuns
```sql
-- Ranking mensal de assessores
SELECT TOP 20
    nome_assessor,
    nome_estrutura,
    captacao_liquida_total,
    qtd_clientes_aportando,
    ticket_medio_aporte,
    RANK() OVER (ORDER BY captacao_liquida_total DESC) as ranking
FROM [gold].[captacao_liquida_assessor]
WHERE ano = 2024 AND mes = 12
ORDER BY captacao_liquida_total DESC;

-- Evolução histórica de um assessor
SELECT 
    ano,
    mes,
    captacao_bruta_total,
    resgate_bruto_total,
    captacao_liquida_total,
    qtd_clientes_aportando,
    qtd_clientes_apenas_aportando
FROM [gold].[captacao_liquida_assessor]
WHERE cod_assessor = 'AAI123'
ORDER BY ano, mes;

-- Comparativo entre estruturas
SELECT 
    nome_estrutura,
    COUNT(DISTINCT cod_assessor) as qtd_assessores,
    SUM(captacao_liquida_total) as captacao_liquida_estrutura,
    AVG(captacao_liquida_total) as media_por_assessor,
    SUM(qtd_clientes_aportando) as total_clientes
FROM [gold].[captacao_liquida_assessor]
WHERE ano = 2024
GROUP BY nome_estrutura
ORDER BY captacao_liquida_estrutura DESC;

-- Análise de retenção (clientes que só resgatam)
SELECT 
    cod_assessor,
    nome_assessor,
    SUM(qtd_clientes_apenas_resgatando) as clientes_perdendo,
    SUM(qtd_clientes_aportando) as total_clientes,
    CAST(SUM(qtd_clientes_apenas_resgatando) AS FLOAT) / 
    NULLIF(SUM(qtd_clientes_aportando), 0) * 100 as pct_risco
FROM [gold].[captacao_liquida_assessor]
WHERE ano = 2024
GROUP BY cod_assessor, nome_assessor
HAVING SUM(qtd_clientes_apenas_resgatando) > 5
ORDER BY pct_risco DESC;
```

### 13.2 Padrões de Uso
- **Dashboards**: Atualização diária, filtros por período/estrutura
- **Relatórios**: Comparativos mensais, rankings, tendências
- **Alertas**: Assessores com captação negativa, alta taxa de resgate
- **Analytics**: Previsão de captação, identificação de padrões

## 14. Troubleshooting

### 14.1 Problemas Comuns
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| Dados faltantes | Assessor sem dados | Verificar silver.dim_pessoas | Atualizar cadastro |
| Performance lenta | Query > 10s | Estatísticas desatualizadas | UPDATE STATISTICS |
| Valores incorretos | Captação negativa inesperada | Validar fact_resgates | Verificar sinais |
| Estrutura incorreta | Assessor em estrutura errada | fact_estrutura_pessoas | Corrigir vigência |

### 14.2 Scripts de Diagnóstico
```sql
-- Verificar volume mensal
SELECT 
    ano, 
    mes,
    COUNT(*) as qtd_assessores,
    SUM(CASE WHEN captacao_liquida_total > 0 THEN 1 ELSE 0 END) as assessores_positivos,
    SUM(CASE WHEN captacao_liquida_total < 0 THEN 1 ELSE 0 END) as assessores_negativos,
    SUM(captacao_liquida_total) as captacao_total_mes
FROM [gold].[captacao_liquida_assessor]
GROUP BY ano, mes
ORDER BY ano DESC, mes DESC;

-- Identificar assessores sem movimento
SELECT 
    p.cod_aai,
    p.nome_pessoa,
    MAX(c.data_ref) as ultimo_movimento
FROM silver.dim_pessoas p
LEFT JOIN [gold].[captacao_liquida_assessor] c ON p.cod_aai = c.cod_assessor
WHERE p.cod_aai IS NOT NULL
  AND p.data_fim_vigencia IS NULL
GROUP BY p.cod_aai, p.nome_pessoa
HAVING MAX(c.data_ref) < DATEADD(MONTH, -3, GETDATE())
    OR MAX(c.data_ref) IS NULL;
```

## 15. Referências e Anexos

### 15.1 Scripts DDL Completos
- [QRY-CAP-001-create_gold_view_captacao_liquida_assessor.sql](../operacional/queries/gold/QRY-CAP-001-create_gold_view_captacao_liquida_assessor.sql)
- [QRY-CAP-002-create_gold_captacao_liquida_assessor.sql](../operacional/queries/gold/QRY-CAP-002-create_gold_captacao_liquida_assessor.sql)
- [QRY-CAP-003-prc_gold_to_table_captacao_liquida_assessor.sql](../operacional/queries/gold/QRY-CAP-003-prc_gold_to_table_captacao_liquida_assessor.sql)

### 15.2 Documentação Relacionada
- [MOD-001-captacao-liquida-cliente.md](MOD-001-captacao-liquida-cliente.md)
- [IND-001-metricas-captacao.md](../tatico/metricas/README.md)
- [ARQ-001-visao-geral-datawarehouse.md](../estrategico/arquiteturas/ARQ-001-visao-geral-datawarehouse.md)

### 15.3 Ferramentas e Recursos
- SQL Server Management Studio (SSMS)
- Power BI Desktop
- Excel com Power Query

---

**Documento criado por**: Bruno Chiaramonti - Multisete Consultoria  
**Última revisão**: 2025-01-16