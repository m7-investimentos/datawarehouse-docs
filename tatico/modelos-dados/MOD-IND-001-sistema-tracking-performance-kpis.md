# MOD-001-performance-tracking-m7

---
título: Modelo de Dados - Sistema de Performance Tracking M7
tipo: MOD - Modelo de Dados
versão: 1.0.0
última_atualização: 2025-01-16
autor: arquitetura.dados@m7investimentos.com.br
aprovador: diretoria.ti@m7investimentos.com.br
tags: [modelo, performance, tracking, silver, platinum, medallion]
status: aprovado
dependências:
  - tipo: arquitetura
    ref: [ARQ-002]
    repo: datawarehouse-docs
  - tipo: etl
    ref: [ETL-001, ETL-002, ETL-003]
    repo: datawarehouse-docs
---

## 1. Objetivo

Definir o modelo de dados para o sistema de Performance Tracking da M7 Investimentos, contemplando as camadas de Metadados e Platinum da arquitetura Medallion. O sistema permite gestão dinâmica de indicadores de performance (KPIs) com pesos variáveis por assessor e período, suportando diferentes tipos de indicadores (CARD, GATILHO, KPI, PPI, METRICA).

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: Gestão de Performance / RH / Comercial
- **Processos suportados**: 
  - Definição e atribuição de KPIs
  - Estabelecimento de metas mensais
  - Cálculo de performance individual
  - Remuneração variável baseada em resultados
- **Stakeholders**: 
  - Gestão de Performance
  - RH
  - Diretoria Comercial
  - Assessores (~35 ativos)

### 2.2 Contexto Técnico
- **Tipo de modelo**: Híbrido (Relacional + EAV para flexibilidade)
- **Plataforma**: SQL Server 2019+
- **Database**: M7Medallion
- **Schemas**: 
  - `silver`: Configurações validadas
  - `platinum`: Dados processados para consumo
- **Layer**: Metadados (configuração) + Platinum (resultados)

## 3. Visão Geral do Modelo

### 3.1 Diagrama Entidade-Relacionamento

```
CAMADA SILVER (Configuração)
================================

┌─────────────────────────┐
│ performance_indicators  │
├─────────────────────────┤
│ PK: indicator_id       │
│ UK: indicator_code     │◄─────┐
│ indicator_name         │      │
│ category              │      │
│ unit                  │      │
│ aggregation_method    │      │
│ formula               │      │
│ is_inverted          │      │
│ is_active            │      │
└─────────────────────────┘      │
                                 │
┌─────────────────────────┐      │
│ performance_assignments │      │
├─────────────────────────┤      │
│ PK: assignment_id      │      │
│ FK: indicator_id       │──────┘
│ FK: cod_assessor       │
│ indicator_weight       │◄─── Soma = 100% para CARD
│ indicator_type         │
│ valid_from            │
│ valid_to              │
└─────────────────────────┘
         │
         │
┌─────────────────────────┐
│ performance_targets     │
├─────────────────────────┤
│ PK: target_id          │
│ FK: indicator_id       │
│ FK: cod_assessor       │
│ period_type            │
│ period_start           │
│ period_end             │
│ target_value           │
│ stretch_target         │
│ minimum_target         │
└─────────────────────────┘

CAMADA PLATINUM (Resultados)
=============================

┌─────────────────────────┐     ┌──────────────────────────┐
│ performance_tracking    │     │ performance_score_summary│
├─────────────────────────┤     ├──────────────────────────┤
│ PK: tracking_id        │     │ PK: summary_id           │
│ FK: cod_assessor       │◄────┤ FK: cod_assessor         │
│ FK: indicator_id       │     │ period_start             │
│ period_start           │     │ period_end               │
│ attribute_name         │     │ card_score               │
│ attribute_value        │     │ gatilhos_ok              │
│ attribute_type         │     │ performance_status       │
└─────────────────────────┘     └──────────────────────────┘
   ↑                                     ↑
   │ EAV Pattern                         │ Agregado mensal
   │                                     │
   └─────────────────────────────────────┘
```

### 3.2 Principais Entidades
| Entidade | Schema | Tipo | Descrição | Volume Estimado |
|----------|--------|------|-----------|-----------------|
| performance_indicators | silver | Configuração | Catálogo de KPIs | ~50 registros |
| performance_assignments | silver | Configuração | Atribuição assessor-KPI | ~500 registros |
| performance_targets | silver | Configuração | Metas mensais | ~6000 registros/ano |
| performance_tracking | platinum | Transacional | Resultados EAV | ~50K registros/mês |
| performance_score_summary | platinum | Agregado | Scores consolidados | ~420 registros/mês |

## 4. Dicionário de Dados Detalhado

### 4.1 Tabela: silver.performance_indicators

**Descrição**: Catálogo centralizado de todos os indicadores de performance disponíveis no sistema.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| indicator_id | INT | PK, IDENTITY | ID único do indicador | 1 | Auto-incremento |
| indicator_code | VARCHAR(50) | UK, NOT NULL | Código único do indicador | "CAPTACAO_LIQUIDA" | UPPER_CASE, sem espaços |
| indicator_name | VARCHAR(200) | NOT NULL | Nome amigável | "Captação Líquida Mensal" | Min 5 caracteres |
| category | VARCHAR(50) | NOT NULL | Categoria do indicador | "FINANCEIRO" | Valores permitidos |
| unit | VARCHAR(20) | NOT NULL | Unidade de medida | "R$" | R$, %, QTD, SCORE, etc |
| aggregation_method | VARCHAR(20) | NOT NULL | Método de agregação | "SUM" | SUM, AVG, COUNT, MAX, MIN, LAST, CUSTOM |
| formula | VARCHAR(MAX) | NULL | Fórmula SQL de cálculo | "SELECT SUM(valor)..." | Validada antes de salvar |
| is_inverted | BIT | NOT NULL, DEFAULT 0 | Se menor é melhor | 0 | Ex: 1 para churn |
| is_active | BIT | NOT NULL, DEFAULT 1 | Se está ativo | 1 | Soft delete |
| description | VARCHAR(MAX) | NULL | Descrição detalhada | "Soma de aplicações..." | Documentação |
| created_date | DATETIME | NOT NULL, DEFAULT | Data de criação | 2024-01-15 | GETDATE() |
| created_by | VARCHAR(100) | NOT NULL | Usuário criador | "admin@m7.com.br" | Do contexto |
| modified_date | DATETIME | NULL | Última modificação | NULL | Trigger update |
| modified_by | VARCHAR(100) | NULL | Último modificador | NULL | Do contexto |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_performance_indicators | CLUSTERED | indicator_id | Chave primária |
| UK_indicator_code | UNIQUE | indicator_code | Garantir unicidade |
| IX_indicators_active_cat | NONCLUSTERED | is_active, category | Queries por status |

**Constraints adicionais**:
```sql
-- Check constraint para categorias válidas
ALTER TABLE silver.performance_indicators 
ADD CONSTRAINT CHK_indicator_category 
CHECK (category IN ('FINANCEIRO', 'QUALIDADE', 'VOLUME', 'COMPORTAMENTAL', 'PROCESSO', 'GATILHO'));

-- Check constraint para units válidas
ALTER TABLE silver.performance_indicators 
ADD CONSTRAINT CHK_indicator_unit 
CHECK (unit IN ('R$', '%', 'QTD', 'SCORE', 'HORAS', 'DIAS', 'RATIO'));
```

### 4.2 Tabela: silver.performance_assignments

**Descrição**: Define quais indicadores estão atribuídos a cada assessor, com seus respectivos pesos e períodos de vigência.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| assignment_id | INT | PK, IDENTITY | ID único da atribuição | 1 | Auto-incremento |
| indicator_id | INT | FK, NOT NULL | Referência ao indicador | 1 | FK para indicators |
| cod_assessor | VARCHAR(20) | FK, NOT NULL | Código do assessor | "AAI001" | Padrão AAI + números |
| indicator_weight | DECIMAL(5,2) | NOT NULL | Peso do indicador | 25.00 | 0-100, soma=100 para CARD |
| indicator_type | VARCHAR(20) | NOT NULL | Tipo de indicador | "CARD" | CARD, GATILHO, KPI, PPI, METRICA |
| valid_from | DATE | NOT NULL | Início da vigência | 2024-01-01 | Não pode ser futuro |
| valid_to | DATE | NULL | Fim da vigência | NULL | NULL = vigente |
| created_date | DATETIME | NOT NULL, DEFAULT | Data de criação | 2024-01-01 | GETDATE() |
| created_by | VARCHAR(100) | NOT NULL | Usuário criador | "gestor@m7.com" | Do contexto |
| approved_by | VARCHAR(100) | NULL | Aprovador | "diretor@m7.com" | Para rastreabilidade |
| approved_date | DATETIME | NULL | Data aprovação | 2024-01-02 | Quando aprovado |
| comments | VARCHAR(MAX) | NULL | Observações | "Ajuste trimestral" | Histórico |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_assignments | CLUSTERED | assignment_id | Chave primária |
| IX_assignments_assessor | NONCLUSTERED | cod_assessor, valid_from | Busca por assessor |
| IX_assignments_current | FILTERED | cod_assessor, indicator_id | WHERE valid_to IS NULL |

**Constraints adicionais**:
```sql
-- Garantir unicidade de indicador por assessor no período
ALTER TABLE silver.performance_assignments
ADD CONSTRAINT UK_assignment_unique 
UNIQUE (cod_assessor, indicator_id, valid_from);

-- Validar tipo de indicador
ALTER TABLE silver.performance_assignments
ADD CONSTRAINT CHK_indicator_type
CHECK (indicator_type IN ('CARD', 'GATILHO', 'KPI', 'PPI', 'METRICA'));

-- Trigger para validar soma de pesos
CREATE TRIGGER trg_validate_card_weights
ON silver.performance_assignments
AFTER INSERT, UPDATE
AS
BEGIN
    -- Validar que soma dos pesos CARD = 100%
    IF EXISTS (
        SELECT cod_assessor, valid_from, SUM(indicator_weight) as total_weight
        FROM silver.performance_assignments
        WHERE indicator_type = 'CARD' 
          AND valid_to IS NULL
        GROUP BY cod_assessor, valid_from
        HAVING ABS(SUM(indicator_weight) - 100.0) > 0.01
    )
    BEGIN
        RAISERROR('Soma dos pesos CARD deve ser 100%', 16, 1);
        ROLLBACK TRANSACTION;
    END
END;
```

### 4.3 Tabela: silver.performance_targets

**Descrição**: Armazena as metas mensais estabelecidas para cada combinação assessor-indicador.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| target_id | INT | PK, IDENTITY | ID único da meta | 1 | Auto-incremento |
| indicator_id | INT | FK, NOT NULL | Referência ao indicador | 1 | FK para indicators |
| cod_assessor | VARCHAR(20) | FK, NOT NULL | Código do assessor | "AAI001" | Padrão AAI + números |
| period_type | VARCHAR(20) | NOT NULL | Tipo de período | "MENSAL" | Sempre MENSAL por ora |
| period_start | DATE | NOT NULL | Início do período | 2024-01-01 | Primeiro dia do mês |
| period_end | DATE | NOT NULL | Fim do período | 2024-01-31 | Último dia do mês |
| target_value | DECIMAL(18,4) | NOT NULL | Valor da meta | 1000000.00 | > 0 (exceto %) |
| stretch_target | DECIMAL(18,4) | NULL | Meta stretch | 1200000.00 | > target (exceto inverted) |
| minimum_target | DECIMAL(18,4) | NULL | Meta mínima | 800000.00 | < target (exceto inverted) |
| created_date | DATETIME | NOT NULL, DEFAULT | Data de criação | 2024-01-01 | GETDATE() |
| created_by | VARCHAR(100) | NOT NULL | Usuário criador | "gestor@m7.com" | Do contexto |
| modified_date | DATETIME | NULL | Última modificação | NULL | Trigger update |
| modified_by | VARCHAR(100) | NULL | Último modificador | NULL | Do contexto |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_targets | CLUSTERED | target_id | Chave primária |
| UK_target_unique | UNIQUE | cod_assessor, indicator_id, period_start | Evitar duplicatas |
| IX_targets_period | NONCLUSTERED | period_start, cod_assessor | Queries por período |

### 4.4 Tabela: platinum.performance_tracking

**Descrição**: Armazena os resultados de performance calculados em formato EAV (Entity-Attribute-Value) para máxima flexibilidade.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| tracking_id | BIGINT | PK, IDENTITY | ID único do registro | 1 | Auto-incremento |
| cod_assessor | VARCHAR(20) | FK, NOT NULL | Código do assessor | "AAI001" | Entity do EAV |
| indicator_id | INT | FK, NOT NULL | Referência ao indicador | 1 | Define o contexto |
| period_start | DATE | NOT NULL | Início do período | 2024-01-01 | Para agrupamento |
| period_end | DATE | NOT NULL | Fim do período | 2024-01-31 | Para agrupamento |
| attribute_name | VARCHAR(100) | NOT NULL | Nome do atributo | "valor_realizado" | Attribute do EAV |
| attribute_value | VARCHAR(MAX) | NULL | Valor do atributo | "1500000.00" | Value do EAV |
| attribute_type | VARCHAR(50) | NOT NULL | Tipo do valor | "DECIMAL" | Para casting |
| calculation_date | DATETIME | NOT NULL | Data do cálculo | 2024-02-01 | Quando foi calculado |
| data_source | VARCHAR(200) | NULL | Fonte dos dados | "gold.fact_captacao" | Rastreabilidade |
| is_current | BIT | NOT NULL, DEFAULT 1 | Se é o valor mais recente | 1 | Para performance |
| created_date | DATETIME | NOT NULL, DEFAULT | Data de criação | 2024-02-01 | GETDATE() |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_tracking | CLUSTERED | tracking_id | Chave primária |
| IX_tracking_current | FILTERED | cod_assessor, period_start, indicator_id | WHERE is_current = 1 |
| IX_tracking_lookup | NONCLUSTERED | cod_assessor, indicator_id, period_start, attribute_name | Busca específica |

**Atributos típicos no EAV**:
| attribute_name | Descrição | Tipo esperado |
|----------------|-----------|---------------|
| valor_realizado | Valor alcançado no período | DECIMAL |
| percentual_atingimento | % da meta atingida | DECIMAL |
| valor_meta | Meta do período | DECIMAL |
| valor_stretch | Meta stretch | DECIMAL |
| valor_minimum | Meta mínima | DECIMAL |
| score_final | Score calculado (0-100) | DECIMAL |
| ranking_equipe | Posição no ranking da equipe | INT |
| ranking_geral | Posição no ranking geral | INT |
| tendencia | Tendência vs mês anterior | VARCHAR |
| status_meta | ATINGIU, NAO_ATINGIU, SUPEROU | VARCHAR |

### 4.5 Tabela: platinum.performance_score_summary

**Descrição**: Visão consolidada mensal do score de performance de cada assessor.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| summary_id | INT | PK, IDENTITY | ID único do resumo | 1 | Auto-incremento |
| cod_assessor | VARCHAR(20) | FK, NOT NULL | Código do assessor | "AAI001" | Chave do resumo |
| period_start | DATE | NOT NULL | Início do período | 2024-01-01 | Primeiro dia do mês |
| period_end | DATE | NOT NULL | Fim do período | 2024-01-31 | Último dia do mês |
| card_score | DECIMAL(5,2) | NOT NULL | Score ponderado CARD | 85.50 | 0-100 |
| card_indicators_count | INT | NOT NULL | Qtd indicadores CARD | 5 | Para contexto |
| gatilhos_total | INT | NOT NULL | Total de gatilhos | 3 | Qtd gatilhos atribuídos |
| gatilhos_ok | INT | NOT NULL | Gatilhos atingidos | 2 | <= gatilhos_total |
| gatilhos_status | VARCHAR(20) | NOT NULL | Status dos gatilhos | "PENDENTE" | OK, PENDENTE, FALHOU |
| kpi_average | DECIMAL(5,2) | NULL | Média dos KPIs | 78.30 | Se houver KPIs |
| performance_status | VARCHAR(20) | NOT NULL | Status geral | "APROVADO" | APROVADO, REPROVADO, PENDENTE |
| final_score | DECIMAL(5,2) | NULL | Score final ajustado | 85.50 | Após gatilhos |
| ranking_equipe | INT | NULL | Ranking na equipe | 3 | Posição |
| ranking_geral | INT | NULL | Ranking geral | 15 | Entre todos |
| equipe | VARCHAR(100) | NULL | Nome da equipe | "Premium SP" | Desnormalizado para performance |
| calculation_date | DATETIME | NOT NULL | Data do cálculo | 2024-02-05 | Quando calculado |
| created_date | DATETIME | NOT NULL, DEFAULT | Data de criação | 2024-02-05 | GETDATE() |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_summary | CLUSTERED | summary_id | Chave primária |
| UK_summary_unique | UNIQUE | cod_assessor, period_start | Um por assessor/mês |
| IX_summary_period | NONCLUSTERED | period_start, performance_status | Análises mensais |
| IX_summary_ranking | NONCLUSTERED | period_start, final_score DESC | Para rankings |

**Constraints adicionais**:
```sql
-- Validar status de performance
ALTER TABLE platinum.performance_score_summary
ADD CONSTRAINT CHK_performance_status
CHECK (performance_status IN ('APROVADO', 'REPROVADO', 'PENDENTE'));

-- Validar status de gatilhos
ALTER TABLE platinum.performance_score_summary
ADD CONSTRAINT CHK_gatilhos_status
CHECK (gatilhos_status IN ('OK', 'PENDENTE', 'FALHOU'));

-- Validar scores entre 0 e 100
ALTER TABLE platinum.performance_score_summary
ADD CONSTRAINT CHK_scores_range
CHECK (card_score BETWEEN 0 AND 100 
   AND (kpi_average IS NULL OR kpi_average BETWEEN 0 AND 100)
   AND (final_score IS NULL OR final_score BETWEEN 0 AND 100));
```

## 5. Relacionamentos e Integridade

### 5.1 Chaves Estrangeiras
| Tabela Origem | Campo(s) | Tabela Destino | Campo(s) | Tipo | On Delete | On Update |
|---------------|----------|----------------|----------|------|-----------|-----------|
| performance_assignments | indicator_id | performance_indicators | indicator_id | N:1 | RESTRICT | CASCADE |
| performance_targets | indicator_id | performance_indicators | indicator_id | N:1 | RESTRICT | CASCADE |
| performance_tracking | indicator_id | performance_indicators | indicator_id | N:1 | RESTRICT | CASCADE |

### 5.2 Cardinalidade dos Relacionamentos
- **Indicator → Assignment**: 1:N (Um indicador pode estar em várias atribuições)
- **Indicator → Target**: 1:N (Um indicador tem várias metas mensais)
- **Indicator → Tracking**: 1:N (Um indicador gera vários resultados)
- **Assessor → Assignment**: N:N através de Assignment (via períodos)
- **Assessor → Summary**: 1:N (Um resumo por mês)

## 6. Regras de Negócio e Validações

### 6.1 Regras de Integridade
| Regra | Implementação | Descrição |
|-------|---------------|-----------|
| RN001 | Trigger | Soma dos pesos CARD deve ser 100% por assessor/período |
| RN002 | Check constraint | Indicadores não-CARD devem ter peso 0 |
| RN003 | Trigger | Stretch > Target > Minimum (exceto inverted) |
| RN004 | Unique constraint | Um indicador por assessor por período |
| RN005 | Procedure | Todos os gatilhos OK para score válido |
| RN006 | Check constraint | Period_end deve ser último dia do mês |

### 6.2 Implementação das Regras
```sql
-- RN003: Validação de metas lógicas
CREATE TRIGGER trg_validate_target_logic
ON silver.performance_targets
AFTER INSERT, UPDATE
AS
BEGIN
    IF EXISTS (
        SELECT 1
        FROM inserted i
        JOIN silver.performance_indicators ind 
            ON i.indicator_id = ind.indicator_id
        WHERE 
            -- Para indicadores normais
            (ind.is_inverted = 0 AND (
                (i.stretch_target IS NOT NULL AND i.stretch_target <= i.target_value) OR
                (i.minimum_target IS NOT NULL AND i.minimum_target >= i.target_value)
            ))
            OR
            -- Para indicadores invertidos
            (ind.is_inverted = 1 AND (
                (i.stretch_target IS NOT NULL AND i.stretch_target >= i.target_value) OR
                (i.minimum_target IS NOT NULL AND i.minimum_target <= i.target_value)
            ))
    )
    BEGIN
        RAISERROR('Lógica de metas inválida: verificar stretch/target/minimum', 16, 1);
        ROLLBACK TRANSACTION;
    END
END;
```

## 7. Histórico e Auditoria

### 7.1 Estratégia de Historização
- **Tipo**: 
  - Metadados: SCD Type 2 via valid_from/valid_to
  - Platinum: Append-only com is_current flag
- **Campos de controle**:
  - `created_date`, `created_by`: Registro inicial
  - `modified_date`, `modified_by`: Alterações
  - `calculation_date`: Quando foi calculado
  - `is_current`: Registro mais recente

### 7.2 Views de Histórico
```sql
-- View para histórico de mudanças de peso
CREATE VIEW silver.vw_assignment_history AS
SELECT 
    a.cod_assessor,
    a.indicator_id,
    i.indicator_code,
    a.indicator_weight,
    a.valid_from,
    a.valid_to,
    DATEDIFF(DAY, a.valid_from, ISNULL(a.valid_to, GETDATE())) as days_active,
    a.created_by,
    a.created_date
FROM silver.performance_assignments a
JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
ORDER BY a.cod_assessor, a.indicator_id, a.valid_from;
```

## 8. Performance e Otimização

### 8.1 Estratégias de Indexação
| Padrão de Query | Índice Recomendado | Justificativa |
|-----------------|-------------------|---------------|
| Score mensal por assessor | IX_tracking_current (filtered) | Apenas registros atuais |
| Ranking por período | IX_summary_ranking | Ordenado por score |
| Histórico de indicador | IX_tracking_lookup | Cobertura completa |
| Metas do mês | IX_targets_period | Busca por período |

### 8.2 Particionamento
```sql
-- Particionamento mensal para performance_tracking (volume alto)
CREATE PARTITION FUNCTION pf_tracking_monthly (DATE)
AS RANGE RIGHT FOR VALUES (
    '2024-01-01', '2024-02-01', '2024-03-01', -- etc
);

CREATE PARTITION SCHEME ps_tracking_monthly
AS PARTITION pf_tracking_monthly
ALL TO ([PRIMARY]);

-- Aplicar à tabela
CREATE CLUSTERED INDEX IX_tracking_partitioned
ON platinum.performance_tracking (period_start, tracking_id)
ON ps_tracking_monthly(period_start);
```

## 9. Segurança e Privacidade

### 9.1 Classificação de Dados
| Campo | Classificação | Tratamento |
|-------|---------------|------------|
| cod_assessor | PII - Confidencial | Acesso controlado |
| valores de meta | Confidencial | Não expor publicamente |
| scores/rankings | Restrito | Apenas gestão e próprio |
| fórmulas | Interno | Proteger lógica de negócio |

### 9.2 Políticas de Acesso (RLS)
```sql
-- Row Level Security para assessores verem apenas seus dados
CREATE SCHEMA security;
GO

CREATE FUNCTION security.fn_performance_filter(@cod_assessor VARCHAR(20))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS access_granted
WHERE @cod_assessor = USER_NAME() -- Mapear com AD
   OR IS_ROLEMEMBER('PerformanceAdmin') = 1
   OR IS_ROLEMEMBER('RH_Manager') = 1;
GO

-- Aplicar às tabelas
CREATE SECURITY POLICY performance_policy
ADD FILTER PREDICATE security.fn_performance_filter(cod_assessor)
ON platinum.performance_tracking,
ADD FILTER PREDICATE security.fn_performance_filter(cod_assessor)
ON platinum.performance_score_summary
WITH (STATE = ON);
```

## 10. Integração e Dependências

### 10.1 Sistemas Fonte
| Sistema | Tabelas/APIs | Frequência | Método |
|---------|--------------|-----------|---------|
| Google Sheets | 3 planilhas | Sob demanda | ETL Python |
| ERP M7 | Dados financeiros | Diário | CDC/Query |
| CRM | Dados comerciais | Diário | API REST |
| RH System | Dados funcionários | Mensal | Arquivo |

### 10.2 Sistemas Consumidores
| Sistema | Uso | Requisitos |
|---------|-----|------------|
| Power BI | Dashboards | Views otimizadas |
| Portal RH | Consulta individual | API real-time |
| Sistema Remuneração | Cálculo variável | Score mensal |
| Mobile App | Acompanhamento | API com cache |

## 11. Evolução e Versionamento

### 11.1 Estratégia de Versionamento
- **Schema versioning**: Prefixo v1_, v2_ em objetos
- **Compatibilidade**: Manter views de compatibilidade
- **Deprecação**: Aviso 60 dias antes de remover

### 11.2 Roadmap de Evolução
1. **v1.1**: Suporte a metas trimestrais
2. **v1.2**: Indicadores compostos (calculados de outros)
3. **v2.0**: Machine Learning para projeções
4. **v2.1**: Integração com OKRs corporativos

## 12. Qualidade e Governança

### 12.1 Regras de Qualidade
| Dimensão | Métrica | Target | Medição |
|----------|---------|--------|---------|
| Completude | % assessores com metas | 100% | Mensal |
| Integridade | % pesos válidos (=100) | 100% | Real-time |
| Atualidade | Lag cálculo score | < 1 dia | Diário |
| Consistência | Scores recalculados = original | 100% | Semanal |

### 12.2 Processos de Governança
- **Data Steward**: Gestão de Performance
- **Aprovações**: Mudanças em indicators requerem 2 aprovadores
- **Auditoria**: Log completo de mudanças
- **Documentação**: Atualizada a cada sprint

## 13. Exemplos e Casos de Uso

### 13.1 Query: Score Mensal de um Assessor
```sql
-- Score completo com detalhamento
WITH score_detail AS (
    SELECT 
        pt.cod_assessor,
        pi.indicator_code,
        pi.indicator_name,
        pa.indicator_type,
        pa.indicator_weight,
        MAX(CASE WHEN pt.attribute_name = 'valor_realizado' 
                 THEN CAST(pt.attribute_value AS DECIMAL(18,2)) END) as realizado,
        MAX(CASE WHEN pt.attribute_name = 'valor_meta' 
                 THEN CAST(pt.attribute_value AS DECIMAL(18,2)) END) as meta,
        MAX(CASE WHEN pt.attribute_name = 'percentual_atingimento' 
                 THEN CAST(pt.attribute_value AS DECIMAL(5,2)) END) as percentual,
        MAX(CASE WHEN pt.attribute_name = 'score_final' 
                 THEN CAST(pt.attribute_value AS DECIMAL(5,2)) END) as score
    FROM platinum.performance_tracking pt
    JOIN silver.performance_indicators pi ON pt.indicator_id = pi.indicator_id
    JOIN silver.performance_assignments pa ON pt.indicator_id = pa.indicator_id 
        AND pt.cod_assessor = pa.cod_assessor
        AND pa.valid_to IS NULL
    WHERE pt.cod_assessor = 'AAI001'
      AND pt.period_start = '2024-01-01'
      AND pt.is_current = 1
    GROUP BY pt.cod_assessor, pi.indicator_code, pi.indicator_name, 
             pa.indicator_type, pa.indicator_weight
)
SELECT 
    cod_assessor,
    indicator_type,
    COUNT(*) as qtd_indicadores,
    SUM(CASE WHEN indicator_type = 'CARD' 
             THEN score * indicator_weight / 100.0 
             ELSE 0 END) as score_ponderado,
    STRING_AGG(
        indicator_name + ': ' + 
        FORMAT(percentual, 'N1') + '% (' +
        CASE WHEN score >= 100 THEN 'SUPEROU'
             WHEN score >= 80 THEN 'ATINGIU'
             ELSE 'NÃO ATINGIU' END + ')',
        '; '
    ) as detalhamento
FROM score_detail
GROUP BY cod_assessor, indicator_type
ORDER BY indicator_type;
```

### 13.2 Procedure: Cálculo Mensal de Performance
```sql
CREATE PROCEDURE platinum.prc_calculate_monthly_performance
    @year INT,
    @month INT,
    @cod_assessor VARCHAR(20) = NULL -- NULL = todos
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @period_start DATE = DATEFROMPARTS(@year, @month, 1);
    DECLARE @period_end DATE = EOMONTH(@period_start);
    
    -- 1. Marcar registros anteriores como não-correntes
    UPDATE platinum.performance_tracking
    SET is_current = 0
    WHERE period_start = @period_start
      AND (@cod_assessor IS NULL OR cod_assessor = @cod_assessor);
    
    -- 2. Calcular cada indicador
    INSERT INTO platinum.performance_tracking (
        cod_assessor, indicator_id, period_start, period_end,
        attribute_name, attribute_value, attribute_type,
        calculation_date, data_source
    )
    SELECT 
        a.cod_assessor,
        a.indicator_id,
        @period_start,
        @period_end,
        attribute_name,
        attribute_value,
        attribute_type,
        GETDATE(),
        'prc_calculate_monthly_performance'
    FROM silver.performance_assignments a
    CROSS APPLY platinum.fn_calculate_indicator(
        a.cod_assessor, 
        a.indicator_id, 
        @period_start, 
        @period_end
    ) calc
    WHERE a.valid_from <= @period_start
      AND (a.valid_to IS NULL OR a.valid_to >= @period_end)
      AND (@cod_assessor IS NULL OR a.cod_assessor = @cod_assessor);
    
    -- 3. Consolidar scores
    EXEC platinum.prc_consolidate_monthly_scores 
        @period_start = @period_start,
        @cod_assessor = @cod_assessor;
    
    -- 4. Calcular rankings
    EXEC platinum.prc_calculate_rankings
        @period_start = @period_start;
END;
```

## 14. Troubleshooting

### 14.1 Problemas Comuns
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| Score incorreto | Valor diferente do esperado | Verificar pesos e fórmulas | Recalcular período |
| Gatilho não validado | Score = NULL com gatilho pendente | Query gatilhos não atingidos | Verificar regras gatilho |
| Performance lenta | Queries > 5s | Missing indexes | Criar índices filtered |
| Dados duplicados | Múltiplos scores mesmo mês | Validar is_current | Limpar duplicatas |

### 14.2 Queries de Diagnóstico
```sql
-- Verificar integridade de pesos
SELECT 
    cod_assessor,
    valid_from,
    SUM(indicator_weight) as total_weight,
    COUNT(*) as qtd_indicators,
    STRING_AGG(indicator_code, ', ') as indicators
FROM silver.performance_assignments a
JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
WHERE a.indicator_type = 'CARD'
  AND a.valid_to IS NULL
GROUP BY cod_assessor, valid_from
HAVING ABS(SUM(indicator_weight) - 100.0) > 0.01;

-- Identificar cálculos pendentes
SELECT 
    a.cod_assessor,
    COUNT(DISTINCT i.indicator_id) as indicators_assigned,
    COUNT(DISTINCT t.indicator_id) as indicators_calculated,
    COUNT(DISTINCT i.indicator_id) - COUNT(DISTINCT t.indicator_id) as pending
FROM silver.performance_assignments a
JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
LEFT JOIN platinum.performance_tracking t 
    ON a.cod_assessor = t.cod_assessor 
    AND a.indicator_id = t.indicator_id
    AND t.period_start = '2024-01-01'
    AND t.is_current = 1
WHERE a.valid_to IS NULL
GROUP BY a.cod_assessor
HAVING COUNT(DISTINCT i.indicator_id) > COUNT(DISTINCT t.indicator_id);
```

## 15. Referências e Anexos

### 15.1 Scripts DDL
[Disponíveis no repositório: /sql/silver/performance/]

### 15.2 Documentação Relacionada
- [ARQ-002 - Arquitetura Medallion M7]
- [ETL-001 - Extração Performance Indicators]
- [ETL-002 - Extração Performance Assignments]
- [ETL-003 - Extração Performance Targets]
- [PRO-001 - Processo de Gestão de Performance]

### 15.3 Glossário de Termos
- **CARD**: Indicador principal que compõe o score
- **GATILHO**: Pré-requisito obrigatório
- **KPI**: Key Performance Indicator para monitoramento
- **PPI**: Process Performance Indicator
- **METRICA**: Indicador auxiliar de acompanhamento
- **EAV**: Entity-Attribute-Value (padrão de modelagem)

---

**Documento criado por**: Arquitetura de Dados M7
**Última revisão**: 2025-01-16
**Próxima revisão**: 2025-04-16