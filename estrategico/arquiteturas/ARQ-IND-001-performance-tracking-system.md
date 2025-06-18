# ARQ-IND-001-performance-tracking-system

---
**título**: Arquitetura do Sistema de Performance Tracking
**versão**: 1.0.0
**última_atualização**: 2025-01-18
**tags**: [arquitetura, performance, kpi, indicadores, medallion, BI]
**responsável**: bruno.chiaramonti@multisete.com
**status**: aprovado
**tipo_documento**: ARQ
**nível_hierárquico**: estratégico
**repositório**: [business-documentation/datawarehouse-docs/ai-agents-contexts]

**dependências**:
  - tipo: política
    ref: POL-GOV-001
    repo: business-documentation/estrategico/politicas
  - tipo: manual
    ref: MAN-ANA-001
    repo: business-documentation/estrategico/manuais/ANA
  - tipo: arquitetura
    ref: ARQ-DWH-001
    repo: datawarehouse-docs

**aprovações**:
  - nome: Diretoria Comercial
    cargo: Diretor Comercial
    data: 2025-01-18
---

## 1. Visão Geral

### 1.1 Objetivo
O Sistema de Performance Tracking é uma solução de gestão de indicadores (KPIs) individualizados que permite atribuir diferentes métricas para cada assessor de investimentos, com mudanças trimestrais e cálculo de performance ponderada. O sistema resolve o problema de cada pessoa ter indicadores diferentes e pesos variáveis, mantendo flexibilidade total através de configuração via metadados.

### 1.2 Escopo
**Incluído:**
- Gestão de indicadores de performance personalizados por assessor
- Atribuição dinâmica de pesos por indicador
- Definição e acompanhamento de metas mensais
- Cálculo de performance individual e rankings
- Suporte a indicadores normais e invertidos
- Integração com PowerBI, LLMs e Machine Learning

**Excluído:**
- Cálculo de remuneração variável (consome dados do sistema)
- Gestão de recursos humanos
- Sistemas de vendas/CRM (fontes de dados)

### 1.3 Stakeholders

| Stakeholder | Papel | Interesse |
|-------------|-------|-----------|
| Diretoria Comercial | Sponsor/Aprovador | Visão estratégica de performance |
| Gestão de Performance | Owner do Sistema | Configuração e manutenção de indicadores |
| Assessores de Investimento | Usuários Finais | Acompanhamento de metas individuais |
| Controladoria | Consumidor | Dados para remuneração variável |
| TI/Analytics | Mantenedor | Operação e evolução do sistema |

## 2. Contexto de Negócio

### 2.1 Drivers de Negócio
- **Driver 1**: Necessidade de flexibilidade na atribuição de indicadores individualizados
- **Driver 2**: Mudanças trimestrais de pesos e indicadores por assessor
- **Driver 3**: Transparência no cálculo de performance e remuneração
- **Driver 4**: Agilidade na inclusão de novos indicadores sem alteração de código

### 2.2 Capacidades de Negócio

| Capacidade | Descrição | Prioridade |
|------------|-----------|------------|
| Gestão de Indicadores | Cadastrar e configurar KPIs dinamicamente | Alta |
| Atribuição Personalizada | Definir indicadores e pesos por assessor | Alta |
| Acompanhamento de Metas | Definir e monitorar metas mensais | Alta |
| Cálculo de Performance | Processar resultados e calcular atingimento | Alta |
| Rankings e Comparações | Gerar rankings e análises comparativas | Média |
| Integração ML/BI | Disponibilizar dados para consumo | Alta |

## 3. Visão Arquitetural

### 3.1 Princípios Arquiteturais

| Princípio | Descrição | Rationale |
|-----------|-----------|-----------|
| Flexibilidade Total | Configuração via metadados, sem código | Permite mudanças rápidas de negócio |
| Simplicidade | Evitar complexidade desnecessária | Facilita manutenção e entendimento |
| Rastreabilidade | Histórico completo preservado | Auditoria e compliance |
| Escalabilidade | Suporta N indicadores × N pessoas | Crescimento futuro |
| Transparência | Cálculos claros e auditáveis | Confiança no sistema |

### 3.2 Decisões Arquiteturais Chave (ADRs)

| ADR# | Decisão | Alternativas Consideradas | Justificativa |
|------|---------|-------------------------|---------------|
| ADR-001 | Arquitetura EAV para Gold | Modelo relacional tradicional | Flexibilidade para N indicadores |
| ADR-002 | Google Sheets como fonte | Interface web customizada | Simplicidade e familiaridade |
| ADR-003 | Medallion com Platinum | Apenas Bronze-Silver-Gold | Suporte a ML e APIs |
| ADR-004 | Full reload em Bronze | Incremental/CDC | Volume pequeno, simplicidade |
| ADR-005 | Procedures para Silver | ETL Python end-to-end | Melhor performance SQL Server |

## 4. Arquitetura de Componentes

### 4.1 Diagrama de Contexto (C4 - Nível 1)

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│  Gestão Performance │────▶│ Performance Tracking │────▶│      Power BI       │
│  (Google Sheets)    │     │      System         │     │   (Dashboards)      │
└─────────────────────┘     └──────────┬──────────┘     └─────────────────────┘
                                       │                           │
                            ┌──────────┴──────────┐               │
                            │                     │               │
                    ┌───────▼────────┐   ┌───────▼────────┐      │
                    │ Controladoria  │   │  ML Platform   │      │
                    │ (Remuneração)  │   │  (Previsões)   │◀─────┘
                    └────────────────┘   └────────────────┘
```

### 4.2 Componentes Principais

| Componente | Responsabilidade | Tecnologia | Interfaces |
|------------|------------------|------------|------------|
| Google Sheets | Interface de configuração | Google Workspace | API v4 |
| ETL Python | Extração de dados | Python 3.8+ | Google API → SQL |
| Bronze Layer | Staging de dados brutos | SQL Server | Tables |
| Silver Procedures | Transformação e validação | T-SQL | Stored Procedures |
| Silver Layer | Dados limpos e validados | SQL Server | Tables + Views |
| Gold Layer | Modelo EAV para consumo | SQL Server | Views |
| Platinum Layer | Features ML e cache | SQL Server | Tables |

### 4.3 Diagrama de Componentes (C4 - Nível 2)

```
┌────────────────────────────────────────────────────────────────────┐
│                      Performance Tracking System                    │
├────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
│  │ ETL-IND-001     │  │ ETL-IND-002     │  │ ETL-IND-003     │   │
│  │ (Indicators)    │  │ (Assignments)   │  │ (Targets)       │   │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘   │
│           │                    │                    │              │
│           ▼                    ▼                    ▼              │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                      BRONZE LAYER                            │  │
│  │  performance_indicators | performance_assignments | targets  │  │
│  └─────────────────────────────┬───────────────────────────────┘  │
│                                │                                   │
│           ┌────────────────────┴────────────────────┐             │
│           │        SQL Server Procedures            │             │
│           │  prc_bronze_to_silver_indicators        │             │
│           │  prc_bronze_to_silver_assignments       │             │
│           │  prc_bronze_to_silver_targets          │             │
│           └────────────────────┬────────────────────┘             │
│                                │                                   │
│  ┌─────────────────────────────▼───────────────────────────────┐  │
│  │                      SILVER LAYER                            │  │
│  │  performance_indicators | performance_assignments | targets  │  │
│  └─────────────────────────────┬───────────────────────────────┘  │
│                                │                                   │
│  ┌─────────────────────────────▼───────────────────────────────┐  │
│  │                       GOLD LAYER                             │  │
│  │              card_metas (EAV Model)                          │  │
│  │    vw_card_metas_pivot | vw_weighted_score | vw_rankings    │  │
│  └─────────────────────────────┬───────────────────────────────┘  │
│                                │                                   │
│  ┌─────────────────────────────▼───────────────────────────────┐  │
│  │                    PLATINUM LAYER                            │  │
│  │          ml_features | api_cache | forecasts                │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                     │
└────────────────────────────────────────────────────────────────────┘
```

## 5. Arquitetura de Dados

### 5.1 Modelo Conceitual de Dados - Camadas Silver

```
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│   Indicators    │         │   Assignments   │         │    Targets      │
├─────────────────┤         ├─────────────────┤         ├─────────────────┤
│ • Code          │◀────────│ • CRM ID        │────────▶│ • CRM ID        │
│ • Name          │         │ • Indicator ID  │         │ • Indicator ID  │
│ • Category      │         │ • Weight (%)    │         │ • Period        │
│ • Formula SQL   │         │ • Valid From/To │         │ • Target Value  │
│ • Aggregation   │         │ • Type (CARD,   │         │ • Stretch/Min   │
│ • Is Inverted   │         │   GATILHO, KPI) │         │                 │
└─────────────────┘         └─────────────────┘         └─────────────────┘
```

### 5.2 Estratégia da Camada Gold - Processamento Dinâmico

#### 5.2.1 Processo de Cálculo Gold
```
┌──────────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER - Processing Flow                      │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  1. Para cada Assessor + Período:                                    │
│     └─> Buscar indicadores ativos (assignments)                      │
│                                                                       │
│  2. Para cada Indicador:                                             │
│     ├─> Executar formula SQL do indicador                           │
│     ├─> Aplicar aggregation_method (SUM, AVG, CUSTOM)               │
│     └─> Calcular realized_value                                      │
│                                                                       │
│  3. Calcular Achievement:                                             │
│     ├─> Normal: (realized / target) × 100                           │
│     └─> Invertido: (2 - realized/target) × 100                      │
│                                                                       │
│  4. Aplicar Pesos (apenas CARD):                                     │
│     └─> weighted_achievement = achievement × weight                   │
│                                                                       │
│  5. Gerar registro EAV em gold.card_metas                           │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

#### 5.2.2 Estrutura Gold - Modelo EAV
```
gold.card_metas (Entity-Attribute-Value)
├─ meta_id (PK)
├─ period_start
├─ period_end
├─ entity_type ('ASSESSOR')
├─ entity_id (crm_id)
├─ attribute_type ('INDICATOR')
├─ attribute_code (indicator_code)
├─ attribute_name (indicator_name)
├─ indicator_type (CARD, GATILHO, KPI)
├─ indicator_category
├─ target_value
├─ stretch_value
├─ minimum_value
├─ realized_value (calculado via formula SQL)
├─ achievement_percentage
├─ indicator_weight
├─ weighted_achievement
├─ achievement_status (SUPERADO, ATINGIDO, PARCIAL, NAO_ATINGIDO)
├─ is_inverted
├─ calculation_formula (para auditoria)
├─ created_date
└─ processing_notes
```

#### 5.2.3 Views de Consumo Gold
```
1. vw_card_metas_pivot
   └─> Transforma EAV em colunar (1 coluna por indicador)
   
2. vw_card_metas_weighted_score
   └─> Score consolidado por assessor (soma ponderada CARD)
   
3. vw_card_metas_ranking
   └─> Rankings por indicador e geral
   
4. vw_card_metas_monthly_comparison
   └─> Comparação MoM, YoY
   
5. vw_card_metas_dashboard
   └─> Visão executiva para Power BI
```

### 5.3 Exemplo de Processamento Gold

#### Assessor: Clever Mota (20471)
#### Período: Janeiro 2025

**Indicadores Atribuídos:**
1. **CAPT_LIQ** (CARD - 40%)
   - Formula: `captacao_liquida_total FROM gold.captacao_liquida_assessor`
   - Target: 500.000
   - Realized: 450.000 (via SQL execution)
   - Achievement: 90%
   - Weighted: 36% (90% × 40%)

2. **CLIENT_300K_CDI** (CARD - 10%)
   - Formula: `COUNT(*) FROM gold.clientes WHERE valor > 300000`
   - Target: 0.5
   - Realized: 0.45
   - Achievement: 90%
   - Weighted: 9% (90% × 10%)

3. **ABERT_300K** (CARD - 20%)
   - Formula: `COUNT(*) FROM gold.captacao_liquida_assessor WHERE tipo='ABERTURA'`
   - Target: 2
   - Realized: 3
   - Achievement: 150%
   - Weighted: 30% (150% × 20%)

4. **IEA** (CARD - 30%)
   - Formula: Custom calculation
   - Is_inverted: FALSE
   - Achievement: 85%
   - Weighted: 25.5% (85% × 30%)

**Score Final**: 36% + 9% + 30% + 25.5% = **100.5%**

### 5.4 Fluxo de Dados Completo

| Fonte | Dado | Transformação | Destino | Frequência |
|-------|------|---------------|---------|------------|
| Google Sheets | Indicadores + Fórmulas | ETL-001 → Bronze → Silver | performance_indicators | Sob demanda |
| Google Sheets | Atribuições + Pesos | ETL-002 → Bronze → Silver | performance_assignments | Trimestral |
| Google Sheets | Metas Mensais | ETL-003 → Bronze → Silver | performance_targets | Mensal |
| Sistemas Origem | Dados Transacionais | ETLs diversos | Tabelas fact/dim | Diário |
| Silver + Facts | Execução Fórmulas SQL | Procedures dinâmicas | gold.card_metas | Mensal (dia 5) |
| Gold EAV | Agregações e Rankings | Views materializadas | Consumo BI/ML | Real-time |

### 5.6 Diagrama de Processamento Gold End-to-End

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      EXEMPLO: Assessor Clever Mota - Jan/2025           │
└─────────────────────────────────────────────────────────────────────────┘

SILVER TABLES                           GOLD PROCESSING
┌──────────────────┐
│ Indicators       │     ┌─────────────────────────────────────────────┐
│ ┌──────────────┐ │     │ 1. LOOP: Para cada assessor + período       │
│ │ CAPT_LIQ     │ │────▶│    Clever Mota (20471) + Jan/2025          │
│ │ Formula: ... │ │     └─────────────────────────┬───────────────────┘
│ │ Method: SUM  │ │                               │
│ └──────────────┘ │     ┌─────────────────────────▼───────────────────┐
└──────────────────┘     │ 2. BUSCAR: Indicadores atribuídos           │
                         │    - CAPT_LIQ (CARD, 40%)                   │
┌──────────────────┐     │    - CLIENT_300K_CDI (CARD, 10%)           │
│ Assignments     │────▶│    - ABERT_300K (CARD, 20%)                │
│ ┌──────────────┐ │     │    - IEA (CARD, 30%)                       │
│ │ 20471        │ │     │    - NPS_NOTA (GATILHO, 0%)               │
│ │ Weights...   │ │     └─────────────────────────┬───────────────────┘
│ └──────────────┘ │                               │
└──────────────────┘     ┌─────────────────────────▼───────────────────┐
                         │ 3. EXECUTAR: Para cada indicador            │
┌──────────────────┐     │                                             │
│ Targets         │     │ CAPT_LIQ:                                   │
│ ┌──────────────┐ │────▶│   SQL: SELECT captacao_liquida_total       │
│ │ 20471        │ │     │        FROM gold.captacao_liquida_assessor │
│ │ Jan: 500K    │ │     │        WHERE crm_id = '20471'              │
│ └──────────────┘ │     │        AND data BETWEEN '2025-01-01'       │
└──────────────────┘     │                     AND '2025-01-31'       │
                         │   Result: 450.000                           │
                         │   Target: 500.000                           │
                         │   Achievement: 90%                          │
                         │   Weighted: 36% (90% × 40%)                 │
                         └─────────────────────────┬───────────────────┘
                                                   │
FACT TABLES              ┌─────────────────────────▼───────────────────┐
┌──────────────────┐     │ 4. GRAVAR: Em formato EAV                   │
│ Captação        │     │                                             │
│ Clientes        │────▶│ gold.card_metas:                            │
│ NPS             │     │ ┌─────────────────────────────────────┐     │
│ Etc...          │     │ │ entity_id: 20471                    │     │
└──────────────────┘     │ │ attribute_code: CAPT_LIQ           │     │
                         │ │ period: 2025-01                     │     │
                         │ │ target_value: 500000                │     │
                         │ │ realized_value: 450000              │     │
                         │ │ achievement_pct: 90                 │     │
                         │ │ weight: 40                          │     │
                         │ │ weighted_achievement: 36            │     │
                         │ └─────────────────────────────────────┘     │
                         └─────────────────────────┬───────────────────┘
                                                   │
                         ┌─────────────────────────▼───────────────────┐
                         │ 5. VIEWS: Transformar EAV para consumo      │
                         │                                             │
                         │ vw_card_metas_pivot:                       │
                         │ ┌──────┬─────────┬────────────┬────┐       │
                         │ │CRM_ID│CAPT_LIQ │CLIENT_300K │... │       │
                         │ ├──────┼─────────┼────────────┼────┤       │
                         │ │20471 │   90%   │    90%     │... │       │
                         │ └──────┴─────────┴────────────┴────┘       │
                         │                                             │
                         │ Score Final: 100.5%                         │
                         └─────────────────────────────────────────────┘
```

### 5.7 Governança de Dados

- **Classificação**: Confidencial (dados de performance e metas)
- **Retenção**: 5 anos histórico completo, agregados indefinidamente
- **Privacidade**: Dados pessoais de assessores (LGPD aplicável)
- **Auditoria**: Todas as execuções de fórmulas são logadas com timestamp

## 6. Arquitetura de Integração

### 6.1 Padrões de Integração

| Padrão | Uso | Justificativa |
|--------|-----|---------------|
| Batch ETL | Google Sheets → Bronze | Volume pequeno, mudanças infrequentes |
| Stored Procedures | Bronze → Silver → Gold | Performance e controle transacional |
| SQL Dinâmico | Execução de fórmulas | Flexibilidade total de cálculo |
| Views SQL | Consumo por BI/Apps | Abstração e flexibilidade |
| Materialized Views | Dados agregados | Performance para dashboards |

### 6.2 Mecânica de Execução de Fórmulas (Gold Layer)

#### 6.2.1 Processo de Execução Dinâmica
```
Para cada combinação Assessor + Indicador + Período:

1. RECUPERAR METADADOS
   ├─> indicator.formula (ex: "captacao_liquida_total")
   ├─> indicator.aggregation_method (ex: "SUM")
   ├─> indicator.is_inverted (ex: FALSE)
   └─> assignment.weight (ex: 40%)

2. CONSTRUIR QUERY DINÂMICA
   ├─> SELECT {formula}
   ├─> FROM {tabela_origem}
   ├─> WHERE cod_assessor = @crm_id
   └─> AND data_ref BETWEEN @period_start AND @period_end

3. EXECUTAR E PROCESSAR
   ├─> Execute SQL
   ├─> Apply aggregation if needed
   └─> Store as realized_value

4. CALCULAR ACHIEVEMENT
   ├─> Get target_value from targets table
   ├─> If normal: (realized/target) × 100
   └─> If inverted: (2 - realized/target) × 100

5. GRAVAR EM EAV
   └─> INSERT INTO gold.card_metas
```

#### 6.2.2 Exemplos de Fórmulas por Tipo

**Tipo 1: Agregação Simples**
- Indicador: CAPT_LIQ
- Formula: `captacao_liquida_total`
- Agregação: SUM
- Origem: Tabela pré-calculada

**Tipo 2: Contagem com Filtro**
- Indicador: CLIENT_300K_CDI
- Formula: `COUNT(*)`
- Agregação: CUSTOM (já é COUNT)
- Origem: Tabela de clientes com WHERE valor > 300000

**Tipo 3: Cálculo Complexo**
- Indicador: NPS_NOTA
- Formula: `(SUM(_PROMOTOR_) - SUM(_DETRATOR_)) / (SUM(_PROMOTOR_) + SUM(_NEUTROS_) + SUM(_DETRATOR_))`
- Agregação: CUSTOM
- Origem: Tabela de pesquisas NPS

**Tipo 4: Razão/Percentual**
- Indicador: NPS_TX_RESP
- Formula: `COUNT(_RESPOSTAS_) / COUNT(_ENVIOS_)`
- Agregação: CUSTOM
- Origem: Tabela de campanhas

### 6.3 Matriz de Integração

| Sistema Origem | Sistema Destino | Tipo | Protocolo | SLA |
|----------------|-----------------|------|-----------|-----|
| Google Sheets | Bronze Layer | Batch | API v4 | < 5 min |
| Bronze Layer | Silver Layer | Batch | T-SQL | < 10 min |
| Silver Layer | Gold Layer | Batch | T-SQL | < 15 min |
| Fact Tables | Gold Calculation | Real-time | SQL Join | < 1 seg |
| Gold Layer | Power BI | Real-time | DirectQuery | < 2 seg |
| Gold Layer | ML Platform | Batch | SQL Views | Diário |

## 7. Requisitos Não-Funcionais

### 7.1 Performance

| Métrica | Requisito | Medição |
|---------|-----------|---------|
| ETL Completo | < 30 minutos | End-to-end |
| Query Dashboard | < 2 segundos | P95 |
| Cálculo Mensal | < 15 minutos | Todos assessores |

### 7.2 Disponibilidade e Resiliência

- **SLA Target**: 99.0% (não crítico)
- **RTO**: 4 horas
- **RPO**: 24 horas
- **Estratégia DR**: Backup diário + Google Sheets como fonte

### 7.3 Segurança

| Controle | Implementação | Responsável |
|----------|---------------|-------------|
| Autenticação | Service Account Google | TI |
| Autorização | Roles SQL Server | DBA |
| Criptografia | TLS 1.2+ | Infraestrutura |
| Auditoria | Logs ETL + SQL | Sistema |

### 7.4 Escalabilidade

- **Horizontal**: Não aplicável (volume controlado)
- **Vertical**: SQL Server auto-scale
- **Limites**: ~500 assessores × 20 indicadores × 12 meses = 120K registros/ano

## 8. Arquitetura de Deployment

### 8.1 Topologia de Ambientes

| Ambiente | Propósito | Infraestrutura | Acesso |
|----------|-----------|----------------|--------|
| Dev | Desenvolvimento | VM Local | Desenvolvedores |
| Prod | Produção | SQL Server Prod | Restrito |

### 8.2 Diagrama de Deployment

```
┌─────────────────────────────────────────────────────────┐
│                    Google Cloud                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │           Google Sheets (3 planilhas)            │   │
│  └─────────────────────────┬───────────────────────┘   │
│                            │ API v4                      │
└────────────────────────────┼────────────────────────────┘
                             │
┌────────────────────────────┼────────────────────────────┐
│                 M7 Infrastructure                        │
│                            │                             │
│  ┌─────────────────────────▼─────────────────────────┐ │
│  │              ETL Server (Python)                   │ │
│  │  • etl_001_indicators.py                          │ │
│  │  • etl_002_assignments.py                         │ │
│  │  • etl_003_targets.py                             │ │
│  └─────────────────────────┬─────────────────────────┘ │
│                            │                             │
│  ┌─────────────────────────▼─────────────────────────┐ │
│  │         SQL Server (M7Medallion Database)         │ │
│  │  ┌─────────────────────────────────────────────┐  │ │
│  │  │ Bronze → Silver → Gold → Platinum           │  │ │
│  │  └─────────────────────────────────────────────┘  │ │
│  └─────────────────────────┬─────────────────────────┘ │
│                            │                             │
│  ┌─────────────────────────┴─────────────────────────┐ │
│  │              Consumers                             │ │
│  │  • Power BI Service                               │ │
│  │  • ML Platform                                    │ │
│  │  • Custom Applications                            │ │
│  └───────────────────────────────────────────────────┘ │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 8.3 Requisitos de Infraestrutura

| Componente | CPU | Memória | Storage | Rede | Quantidade |
|------------|-----|---------|---------|------|------------|
| ETL Server | 2 cores | 4GB | 20GB | 100Mbps | 1 |
| SQL Server | 8 cores | 32GB | 500GB | 1Gbps | 1 (compartilhado) |

## 9. Considerações de Implementação

### 9.1 Tecnologias e Ferramentas

| Categoria | Tecnologia | Versão | Justificativa |
|-----------|------------|--------|---------------|
| Fonte de Dados | Google Sheets | API v4 | Interface familiar usuários |
| ETL | Python | 3.8+ | Bibliotecas Google API |
| Database | SQL Server | 2019+ | Infraestrutura existente |
| BI | Power BI | Pro | Padrão corporativo |
| Orquestração | Scripts Bash | - | Simplicidade |

### 9.2 Padrões de Desenvolvimento

- **Padrão de Código**: PEP8 para Python, padrões T-SQL
- **Versionamento**: Git (sem GitFlow - simplicidade)
- **CI/CD**: Scripts manuais (baixa frequência mudanças)
- **Documentação**: Markdown + comentários inline

## 10. Migração e Transição

### 10.1 Estratégia de Migração

| Fase | Descrição | Duração | Riscos |
|------|-----------|---------|--------|
| Fase 1 | Setup inicial Bronze/Silver | 1 semana | Baixo |
| Fase 2 | Validação com gestão | 1 semana | Médio |
| Fase 3 | Go-live Gold/BI | 1 semana | Baixo |

### 10.2 Plano de Rollback

- **Trigger**: Erros críticos de cálculo
- **Procedimento**: Manter planilhas Excel paralelas por 3 meses
- **Tempo estimado**: Retorno imediato (planilhas disponíveis)

## 11. Operação e Monitoramento

### 11.1 Métricas Chave

| Métrica | Ferramenta | Threshold | Ação |
|---------|------------|-----------|------|
| ETL Success Rate | Logs Python | > 95% | Investigar falhas |
| Data Freshness | SQL Query | < 24h | Executar ETL |
| Validation Errors | Bronze tables | < 5% | Revisar planilhas |

### 11.2 Dashboards e Alertas

- **Dashboard Operacional**: Status ETL, erros, volume
- **Dashboard de Negócio**: Performance assessores, rankings
- **Alertas**: Email se ETL falhar, validações críticas

## 12. Custos e TCO

### 12.1 Custos de Implementação

| Item | Custo | Tipo | Observação |
|------|-------|------|------------|
| Desenvolvimento | 80 horas | CAPEX | Time interno |
| Google Workspace | Existente | - | Já licenciado |
| SQL Server | Existente | - | Infraestrutura compartilhada |

### 12.2 TCO Projetado (5 anos)

- **Ano 1**: R$ 0 (recursos existentes)
- **Ano 2-5**: R$ 0 (manutenção time interno)
- **Total**: Custo absorvido pela operação

## 13. Riscos e Mitigações

| Risco | Probabilidade | Impacto | Mitigação |
|-------|---------------|---------|-----------|
| Mudança estrutural indicadores | Média | Alto | Arquitetura flexível EAV |
| Erro manual planilhas | Alta | Médio | Validações Bronze/Silver |
| Performance com crescimento | Baixa | Baixo | Índices + Platinum cache |

## 14. Roadmap de Evolução

| Release | Features | Data | Dependências |
|---------|----------|------|--------------|
| v1.0 | MVP Bronze-Silver-Gold | Q1 2025 | Nenhuma |
| v1.1 | Integração Power BI | Q1 2025 | v1.0 |
| v2.0 | Platinum + ML Features | Q2 2025 | v1.1 |
| v3.0 | API REST | Q3 2025 | v2.0 |

## 15. Referências

### 15.1 Documentos Relacionados
- MOD-IND-001: Sistema Tracking Performance KPIs
- MOD-IND-002/003/004: Modelos de dados Silver
- ETL-IND-001/002/003: Processos de extração
- QRY-*: Scripts DDL e procedures

### 15.2 Padrões e Frameworks
- Medallion Architecture (Databricks)
- Entity-Attribute-Value (EAV) Model
- Slowly Changing Dimensions Type 2

### 15.3 Ferramentas de Documentação
- **Diagramas**: ASCII art (simplicidade)
- **Modelagem**: dbdiagram.io
- **Documentação**: Markdown + Git

## 16. Controle de Revisões

| Versão | Data | Autor | Descrição da Alteração |
|--------|------|-------|------------------------|
| 1.0.0 | 2025-01-18 | Bruno Chiaramonti | Criação do documento |

---

## Apêndice A - Tipos de Indicadores e Fórmulas

### A.1 Categorização de Indicadores

| Tipo | Categoria | Peso | Características | Exemplo |
|------|-----------|------|-----------------|---------|
| CARD | Principal | > 0% | Compõem score ponderado | CAPT_LIQ (40%) |
| GATILHO | Qualificador | 0% | Pré-requisito, não pondera | NPS_NOTA mínimo |
| KPI | Monitoramento | 0% | Acompanhamento gerencial | REC_TOTAL |
| PPI | Processo | 0% | Indicadores de processo | Tempo médio |

### A.2 Padrões de Fórmulas SQL

**Padrão 1: Lookup Direto**
```
Indicador: CAPT_LIQ
Formula: captacao_liquida_total
Tabela: gold.captacao_liquida_assessor
Execução: SELECT captacao_liquida_total WHERE crm_id = @assessor
```

**Padrão 2: Agregação com Filtro**
```
Indicador: ABERT_300K
Formula: COUNT(*)
Tabela: gold.captacao_liquida_assessor
Filtro: WHERE tipo_operacao = 'ABERTURA' AND valor >= 300000
```

**Padrão 3: Cálculo Complexo**
```
Indicador: NPS_NOTA
Formula: (SUM(PROMOTOR) - SUM(DETRATOR)) / (SUM(PROMOTOR) + SUM(NEUTROS) + SUM(DETRATOR))
Tabela: _SILVER_NPS_
Agregação: CUSTOM (formula completa)
```

**Padrão 4: Indicador Invertido**
```
Indicador: TAXA_CHURN
Formula: COUNT(clientes_perdidos) / COUNT(clientes_total)
Is_Inverted: TRUE
Cálculo Achievement: (2 - (realized/target)) × 100
```

### A.3 Fluxo de Processamento por Tipo

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ 1. CARD      │     │ 2. GATILHO   │     │ 3. KPI/PPI   │
├──────────────┤     ├──────────────┤     ├──────────────┤
│ • Peso > 0%  │     │ • Peso = 0%  │     │ • Peso = 0%  │
│ • Calcula    │     │ • Calcula    │     │ • Calcula    │
│ • Pondera    │     │ • Valida     │     │ • Monitora   │
│ • Score      │     │ • Não Score  │     │ • Não Score  │
└──────────────┘     └──────────────┘     └──────────────┘
       │                     │                     │
       └─────────────────────┴─────────────────────┘
                             │
                    ┌────────▼────────┐
                    │ gold.card_metas │
                    │   (EAV Model)   │
                    └─────────────────┘
```

---

**Notas de Implementação**:
- Sistema já operacional com Bronze e Silver implementados
- Gold layer em desenvolvimento
- Foco em simplicidade e manutenibilidade
- Performance não é preocupação principal dado o volume
- Fórmulas SQL são armazenadas como texto e executadas dinamicamente
- Cada indicador pode ter sua própria lógica de cálculo mantendo flexibilidade total