# ARQ-001-visao-geral-datawarehouse

---
**título**: Arquitetura Data Warehouse M7 - Visão Geral
**versão**: 2.2.0
**última_atualização**: 2025-06-13
**tags**: [arquitetura, datawarehouse, medallion, sql-server, estratégico, analytics]
**responsável**: bruno.chiaramonti@multisete.com
**status**: em_revisão
**tipo_documento**: ARQ
**nível_hierárquico**: estratégico
**repositório**: business-documentation/datawarehouse-docs/estrategico/arquiteturas

**dependências**:
  - tipo: política
    ref: POL-GOV-001
    repo: business-documentation/estrategico/politicas
  - tipo: manual
    ref: MAN-ANA-001
    repo: business-documentation/estrategico/manuais/ANA

**aprovações**:
  - nome: [Pendente]
    cargo: CEO
    data: [Pendente]
  - nome: Bruno Chiaramonti
    cargo: Head de Performance e Desempenho
    data: [Pendente]
---

## 1. Visão Geral

### 1.1 Objetivo
Esta arquitetura define a estrutura técnica e organizacional do Data Warehouse da M7 Investimentos, estabelecendo um sistema robusto e escalável para consolidação, processamento e disponibilização de dados para análises via PowerBI, modelos de Machine Learning e Large Language Models (LLMs), seguindo o padrão de arquitetura Medallion estendida (Bronze, Silver, Gold e Platinum).

### 1.2 Escopo
**Incluído:**
- Arquitetura de camadas Medallion (Bronze, Silver, Gold, Platinum)
- Schemas complementares (docs, metadados)
- Integração com sistemas fonte da M7
- Processamento ETL/ELT
- Disponibilização para PowerBI, LLMs e ML
- Governança e qualidade de dados
- Segurança e conformidade LGPD

**Excluído:**
- Detalhamento de implementação de modelos ML específicos
- Configuração detalhada de dashboards PowerBI
- Implementação de LLMs (apenas interface de dados)

### 1.3 Stakeholders

| Stakeholder | Papel | Interesse |
|-------------|-------|-----------|
| Diretoria M7 | Patrocinador | ROI e insights estratégicos |
| Performance e Desempenho | Ownership | Estratégia analítica e resultados |
| Equipe de Dados/ML/IA | Implementador | Arquitetura técnica e manutenção |
| Business Insight Partner | Consumidor | Acesso a dados confiáveis e insights |
| TI/Infraestrutura | Suporte | Recursos e disponibilidade |

**Nota**: A Equipe de Dados/ML/IA é uma subárea da área de Performance e Desempenho, responsável pela implementação técnica e evolução da plataforma.

## 2. Contexto de Negócio

### 2.1 Drivers de Negócio
Baseado nos OKRs da M7 Investimentos para 2025 (conforme POL-GOV-001):

- **Driver 1**: Alcançar receita em Investimentos de R$ 11,3MM (KR1)
- **Driver 2**: Alcançar receita de Seguros, Consórcio e Crédito Imobiliário de R$ 3,3MM (KR2)
- **Driver 3**: Alcançar captação líquida de R$ 421MM (KR3)
- **Driver 4**: Aumentar em 333 a base de clientes 300k (Total: 792) (KR4)
- **Driver 5**: Alcançar receita de Investment Banking de R$ 2,7MM (KR5)

**Papel Estratégico do Data Warehouse**: Acelerar o crescimento através do monitoramento inteligente dos processos comerciais, antecipando oportunidades e eliminando gargalos para maximizar receita e captação líquida.

### 2.2 Capacidades de Negócio

| Capacidade | Descrição | Prioridade |
|------------|-----------|------------|
| Análise de Performance | KPIs de investimentos em tempo real | Alta |
| Análise de Risco | Métricas de risco e compliance | Alta |
| Análise de Clientes | 360° view e segmentação | Alta |
| Previsões ML | Modelos preditivos de mercado | Média |
| Relatórios Regulatórios | Automação de reports obrigatórios | Alta |
| Self-Service Analytics | Dados acessíveis para analistas | Média |

## 3. Visão Arquitetural

### 3.1 Princípios Arquiteturais

| Princípio | Descrição | Rationale |
|-----------|-----------|-----------|
| Arquitetura Medallion | Camadas Bronze→Silver→Gold→Platinum | Processamento progressivo e qualidade incremental |
| Schema-on-Read/Write híbrido | Flexibilidade no Bronze, estrutura no Gold/Platinum | Balance entre agilidade e performance |
| Separação Compute/Storage | SQL Server com filegroups otimizados | Escalabilidade e otimização de custos |
| Idempotência | Processos ETL re-executáveis | Confiabilidade e recuperação |
| Versionamento de Dados | Histórico completo de mudanças | Auditoria e compliance |
| Segurança em Camadas | RBAC + RLS em todas as camadas | Proteção de dados sensíveis |

### 3.2 Decisões Arquiteturais Chave (ADRs)

| ADR# | Decisão | Alternativas Consideradas | Justificativa |
|------|---------|-------------------------|---------------|
| ADR-001 | SQL Server como plataforma principal | BigQuery, Snowflake, Databricks | Expertise interna, integração com stack Microsoft, TCO |
| ADR-002 | Arquitetura Medallion com 4 camadas | Lambda, Kappa, tradicional DW | Clareza conceitual, manutenibilidade, evolução gradual |
| ADR-003 | Python + SQL para ETL | SSIS, Talend, Matillion | Flexibilidade, código versionável, comunidade |
| ADR-004 | Airflow para orquestração | Azure Data Factory, Prefect | Open source, extensibilidade, maturidade |
| ADR-005 | PowerBI como ferramenta primária de BI | Tableau, Looker, Qlik | Integração Microsoft, licenciamento existente |

## 4. Arquitetura de Componentes

### 4.1 Diagrama de Contexto (C4 - Nível 1)

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Sistemas Fonte  │     │ Business Insight│     │ Aplicações IA   │
│ - CRM           │     │ Partner         │     │ - LLMs          │
│ - ERP           │────▶│ - PowerBI       │◀────│ - ML Models     │
│ - Trading       │     │ - Excel         │     │ - APIs          │
│ - Custódia      │     └────────┬────────┘     └────────┬────────┘
└────────┬────────┘              │                         │
         │                       │                         │
         ▼                       ▼                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Data Warehouse M7                             │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌─────────┐  ┌─────────┐  │
│  │ Bronze │─▶│ Silver │─▶│  Gold  │─▶│Platinum │  │  Docs   │  │
│  └────────┘  └────────┘  └────────┘  └─────────┘  │Metadados│  │
│                    SQL Server + Python ETL         └─────────┘  │
└──────────────────────────────────────────────────────────────────┘
         │                       │                         │
         ▼                       ▼                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Governança    │     │   Auditoria     │     │   Monitoramento │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### 4.2 Componentes Principais

| Componente | Responsabilidade | Tecnologia | Interfaces |
|------------|------------------|------------|------------|
| Ingestion Layer | Captura de dados dos sistemas fonte | Python/ODBC/APIs | REST/JDBC/Files |
| Bronze Layer | Armazenamento raw data | SQL Server (Schema Bronze) | Tables/Views |
| Silver Layer | Limpeza e padronização | SQL Server (Schema Silver) | Tables/Views |
| Gold Layer | Modelagem dimensional | SQL Server (Schema Gold) | Star Schema |
| Platinum Layer | Agregações e ML features | SQL Server (Schema Platinum) | Feature Store/APIs |
| Docs Schema | Documentação e catálogo | SQL Server (Schema Docs) | Metadata Tables |
| Metadados Schema | Linhagem e qualidade | SQL Server (Schema Metadados) | Control Tables |
| ETL Engine | Processamento de dados | Python (Pandas/PySpark) | Airflow DAGs |
| Orchestrator | Agendamento e monitoramento | Apache Airflow | REST API/UI |
| Security Layer | Autenticação e autorização | AD + SQL Server Security | RBAC/RLS |

### 4.3 Diagrama de Componentes (C4 - Nível 2)

```
┌─────────────────────── Data Warehouse M7 ───────────────────────┐
│                                                                  │
│  ┌─────────────┐        ┌─────────────┐      ┌──────────────┐ │
│  │   Airflow   │───────▶│ ETL Python  │─────▶│ Data Quality │ │
│  │ Orchestrator│        │   Engine    │      │   Framework  │ │
│  └─────────────┘        └──────┬──────┘      └──────────────┘ │
│                                │                                │
│  ┌─────────────────────────────┴──────────────────────────┐   │
│  │                    SQL Server Instance                   │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌──────────┐  │   │
│  │  │ BRONZE  │  │ SILVER  │  │  GOLD   │  │ PLATINUM │  │   │
│  │  │ Schema  │─▶│ Schema  │─▶│ Schema  │─▶│  Schema  │  │   │
│  │  │         │  │         │  │         │  │          │  │   │
│  │  │ Raw     │  │ Clean   │  │ Dim/Fact│  │ Features │  │   │
│  │  │ Tables  │  │ Tables  │  │ Tables  │  │ ML Ready │  │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └──────────┘  │   │
│  │                                                         │   │
│  │  ┌─────────┐  ┌──────────┐                            │   │
│  │  │  DOCS   │  │METADADOS │                            │   │
│  │  │ Schema  │  │  Schema  │                            │   │
│  │  │         │  │          │                            │   │
│  │  │ Catalog │  │ Lineage  │                            │   │
│  │  │ Docs    │  │ Quality  │                            │   │
│  │  └─────────┘  └──────────┘                            │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐  │
│  │ Security │  │Monitoring│  │ Logging  │  │ Alerting  │  │
│  │  Layer   │  │Dashboard │  │ Service  │  │  System   │  │
│  └──────────┘  └──────────┘  └──────────┘  └───────────┘  │
└──────────────────────────────────────────────────────────────┘
```

## 5. Arquitetura de Dados

### 5.1 Modelo Conceitual de Dados

```
BRONZE LAYER (Raw Zone)
├── source_crm_*          (Dados brutos do CRM)
├── source_erp_*          (Dados brutos do ERP)
├── source_trading_*      (Dados brutos de trading)
└── source_custodia_*     (Dados brutos de custódia)

SILVER LAYER (Trusted Zone)
├── cleaned_customers     (Clientes padronizados)
├── cleaned_transactions  (Transações validadas)
├── cleaned_products      (Produtos normalizados)
└── cleaned_positions     (Posições consolidadas)

GOLD LAYER (Refined Zone)
├── dim_customer          (Dimensão Cliente)
├── dim_product           (Dimensão Produto)
├── dim_date              (Dimensão Tempo)
├── fact_transactions     (Fato Transações)
└── fact_positions        (Fato Posições)

PLATINUM LAYER (Curated Zone)
├── agg_customer_360      (Visão 360 do cliente)
├── ml_customer_features  (Features para ML)
├── kpi_dashboard_cache   (KPIs pré-calculados)
└── llm_semantic_layer    (Camada semântica para LLMs)

DOCS SCHEMA (Documentation)
├── table_documentation   (Documentação de tabelas)
├── column_documentation  (Documentação de colunas)
├── business_glossary     (Glossário de negócios)
└── data_catalog          (Catálogo central)

METADADOS SCHEMA (Metadata)
├── data_lineage          (Linhagem de dados)
├── quality_metrics       (Métricas de qualidade)
├── execution_logs        (Logs de execução)
└── audit_trail           (Trilha de auditoria)
```

### 5.2 Fluxo de Dados

| Fonte | Dado | Transformação | Destino | Frequência |
|-------|------|---------------|---------|------------|
| CRM | Clientes | Full Load → Clean → Dimension | Bronze→Silver→Gold | Diário 2AM |
| ERP | Transações | Incremental → Validate → Fact | Bronze→Silver→Gold | Horário |
| Trading | Operações | Streaming → Aggregate | Bronze→Silver→Gold→Platinum | Real-time |
| Custódia | Posições | Delta Load → Reconcile | Bronze→Silver→Gold | Diário 6AM |
| PowerBI | Queries | Direct Query + Import | Gold/Platinum | On-demand |
| ML Pipeline | Features | Batch Transform | Platinum | Diário 4AM |
| All Layers | Metadata | Auto-capture | Metadados Schema | Real-time |

### 5.3 Governança de Dados

- **Classificação de Dados**:
  - **Público**: Dados agregados, KPIs gerais
  - **Interno**: Dados operacionais, métricas de negócio
  - **Confidencial**: Dados de clientes, informações financeiras
  - **Restrito**: Dados sensíveis LGPD, informações estratégicas

- **Políticas de Retenção**:
  - Bronze: 90 dias (raw data)
  - Silver: 2 anos (dados limpos)
  - Gold: 5 anos (dimensões e fatos)
  - Platinum: 7 anos (agregações e compliance)
  - Docs/Metadados: Indefinido (histórico completo)

- **Conformidade**:
  - LGPD: Anonimização, direito ao esquecimento
  - BACEN: Retenção obrigatória, auditoria
  - CVM: Rastreabilidade completa

## 6. Arquitetura de Integração

### 6.1 Padrões de Integração

| Padrão | Uso | Justificativa |
|--------|-----|---------------|
| Batch ETL | Cargas noturnas massivas | Volume alto, janela de manutenção |
| Mini-batch | Atualizações horárias | Near real-time com menor overhead |
| CDC (Change Data Capture) | Captura incremental | Eficiência e menor impacto |
| API REST | Integração sistemas modernos | Padrão de mercado, flexibilidade |
| File Transfer | Legado e parceiros | Compatibilidade, simplicidade |

### 6.2 Matriz de Integração

| Sistema Origem | Sistema Destino | Tipo | Protocolo | SLA | Volume/Dia |
|----------------|-----------------|------|-----------|-----|------------|
| CRM Dynamics | DW Bronze | Batch | ODBC | 99.5% | 1M registros |
| ERP SAP | DW Bronze | CDC | JDBC | 99.9% | 5M registros |
| Trading Platform | DW Bronze | Streaming | Kafka | 99.99% | 10M eventos |
| DW Gold | PowerBI | Direct Query | XMLA | 99.9% | 1000 queries |
| DW Platinum | ML Platform | API | REST | 99.5% | 100K requests |

## 7. Requisitos Não-Funcionais

### 7.1 Performance

| Métrica | Requisito | Medição | Camada |
|---------|-----------|---------|--------|
| Query Response Time | < 2 segundos | P95 | Gold |
| Query Response Time | < 5 segundos | P95 | Platinum |
| ETL Bronze→Silver | < 2 horas | Máximo | Silver |
| ETL Silver→Gold | < 1 hora | Máximo | Gold |
| PowerBI Refresh | < 30 minutos | Médio | Gold/Platinum |
| API Response | < 500ms | P99 | Platinum |

### 7.2 Disponibilidade e Resiliência

- **SLA Target**: 99.5% uptime (22h/dia considerando janela de manutenção)
- **RTO (Recovery Time Objective)**: 4 horas
- **RPO (Recovery Point Objective)**: 1 hora
- **Estratégia DR**: Ativo-Passivo com standby instance
- **Backup Strategy**:
  - Full backup: Semanal (Domingo)
  - Differential: Diário
  - Log backup: Horário

### 7.3 Segurança

| Controle | Implementação | Responsável |
|----------|---------------|-------------|
| Autenticação | Active Directory + SQL Auth | Time Infra |
| Autorização | RBAC + Row Level Security | Time Dados/ML/IA |
| Criptografia | TDE (Transparent Data Encryption) | Time Infra |
| Criptografia em Trânsito | TLS 1.3 | Time Infra |
| Mascaramento | Dynamic Data Masking | Time Dados/ML/IA |
| Auditoria | SQL Server Audit + Custom Logs | Time Dados/ML/IA |
| Monitoramento | SIEM Integration | Time Security |

### 7.4 Escalabilidade

- **Vertical**: 
  - CPU: Até 64 cores
  - RAM: Até 512 GB
  - Storage: Elastic pool até 100TB
  
- **Horizontal**:
  - Read replicas para queries
  - Particionamento por data
  - Filegroups por camada
  
- **Elasticidade**:
  - Auto-growth configurado
  - Resource Governor para workloads
  - Query Store para otimização

## 8. Arquitetura de Deployment

### 8.1 Topologia de Ambientes

| Ambiente | Propósito | Infraestrutura | Especificações |
|----------|-----------|----------------|----------------|
| Dev | Desenvolvimento | VM On-premise | 8 cores, 32GB RAM, 1TB SSD |
| QA | Testes integrados | VM On-premise | 16 cores, 64GB RAM, 2TB SSD |
| Staging | Pré-produção | Bare Metal | 32 cores, 128GB RAM, 5TB NVMe |
| Prod | Produção | Bare Metal HA | 64 cores, 256GB RAM, 20TB NVMe |

### 8.2 Diagrama de Deployment

```
┌─────────────────── Production Environment ───────────────────┐
│                                                              │
│  ┌─────────────┐         ┌─────────────┐                   │
│  │ Load        │         │   Airflow   │                   │
│  │ Balancer    │         │   Server    │                   │
│  └──────┬──────┘         └──────┬──────┘                   │
│         │                       │                           │
│  ┌──────┴────────────┐ ┌───────┴────────┐                 │
│  │ SQL Server Primary│ │ ETL Workers     │                 │
│  │ (Active)          │ │ (Python)        │                 │
│  │ 64 cores, 256GB   │ │ 3x (8c, 32GB)  │                 │
│  └─────────┬─────────┘ └────────────────┘                 │
│            │                                                │
│            │ Always On AG                                   │
│            │                                                │
│  ┌─────────┴─────────┐                                     │
│  │SQL Server Secondary│     ┌──────────────┐              │
│  │ (Passive)         │     │ Storage SAN   │              │
│  │ 64 cores, 256GB   │────▶│ 20TB NVMe     │              │
│  └───────────────────┘     │ RAID 10       │              │
│                            └──────────────┘                │
│                                                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
│  │Monitoring│  │ Backup   │  │ Security │               │
│  │Server    │  │ Server   │  │ Gateway  │               │
│  └──────────┘  └──────────┘  └──────────┘               │
└────────────────────────────────────────────────────────────┘
```

### 8.3 Requisitos de Infraestrutura

| Componente | CPU | Memória | Storage | Rede | Quantidade |
|------------|-----|---------|---------|------|------------|
| SQL Server Primary | 64 cores | 256GB | 20TB NVMe | 10Gbps | 1 |
| SQL Server Secondary | 64 cores | 256GB | 20TB NVMe | 10Gbps | 1 |
| Airflow Server | 8 cores | 32GB | 500GB SSD | 1Gbps | 1 |
| ETL Workers | 8 cores | 32GB | 500GB SSD | 1Gbps | 3 |
| Monitoring Server | 4 cores | 16GB | 1TB SSD | 1Gbps | 1 |

## 9. Considerações de Implementação

### 9.1 Tecnologias e Ferramentas

| Categoria | Tecnologia | Versão | Justificativa |
|-----------|------------|--------|---------------|
| Database | SQL Server Enterprise | 2022 | Features avançadas, HA, performance |
| ETL Runtime | Python | 3.11 | Ecosystem maduro, bibliotecas de dados |
| ETL Libraries | Pandas, SQLAlchemy | Latest | Padrão de mercado |
| Orchestration | Apache Airflow | 2.8 | Flexibilidade, comunidade ativa |
| Version Control | Git | Latest | Padrão de mercado |
| CI/CD | Azure DevOps | Latest | Integração com stack Microsoft |
| Monitoring | Grafana + Prometheus | Latest | Open source, flexibilidade |
| BI Tool | PowerBI | Latest | Licenciamento, integração |

### 9.2 Padrões de Desenvolvimento

- **Padrões de Código**:
  - Python: PEP8 + Black formatter
  - SQL: Estilo consistente, CTEs over subqueries
  - Nomenclatura: snake_case para objetos, PascalCase para schemas
  
- **Versionamento**:
  - Git Flow para código
  - Migrations numeradas para DDL
  - Semantic versioning para releases
  
- **Documentação**:
  - Docstrings em Python
  - Comments em SQL complexo
  - README por projeto
  - Data dictionary automático no schema docs

## 10. Migração e Transição

### 10.1 Estratégia de Migração

| Fase | Descrição | Duração | Riscos | Mitigação |
|------|-----------|---------|--------|-----------|
| Fase 1 | Setup infraestrutura e Bronze | 4 semanas | Baixo | POC primeiro |
| Fase 2 | Implementação Silver + Gold core | 8 semanas | Médio | Migração incremental |
| Fase 3 | PowerBI migration | 4 semanas | Médio | Dual-run period |
| Fase 4 | Platinum + ML integration | 6 semanas | Alto | Feature flags |
| Fase 5 | Docs + Metadados schemas | 2 semanas | Baixo | Automação |
| Fase 6 | Decommission legado | 4 semanas | Alto | Rollback plan |

### 10.2 Plano de Rollback

- **Triggers para Rollback**:
  - Perda de dados críticos
  - Performance degradada > 50%
  - Falhas de integração críticas
  
- **Procedimento**:
  1. Stop all ETL jobs
  2. Restore from checkpoint
  3. Redirect PowerBI to legacy
  4. Investigate root cause
  5. Fix and retry
  
- **Tempo estimado**: 2-4 horas

## 11. Operação e Monitoramento

### 11.1 Métricas Chave

| Métrica | Ferramenta | Threshold | Ação |
|---------|------------|-----------|------|
| CPU Usage SQL Server | Prometheus | > 80% | Alert + Scale investigation |
| Memory Pressure | SQL DMVs | > 90% | Clear cache + investigate |
| ETL Job Duration | Airflow | > 2x baseline | Alert + investigation |
| Failed Jobs | Airflow | > 5/day | Page on-call |
| Query Duration | Query Store | > 30s | Optimization review |
| Data Freshness | Custom | > SLA | Alert business users |
| Storage Growth | SQL Monitor | > 85% | Capacity planning |
| Data Quality Score | Metadados Schema | < 95% | Quality review |

### 11.2 Dashboards e Alertas

- **Dashboard Operacional**:
  - SQL Server performance metrics
  - ETL job status and duration
  - Data freshness indicators
  - Storage utilization
  - Data quality metrics (via metadados schema)
  
- **Dashboard de Negócio**:
  - Data quality scores
  - SLA compliance
  - Usage analytics
  - Cost metrics
  - Documentation coverage (via docs schema)
  
- **Alertas Críticos** (Page immediately):
  - SQL Server down
  - ETL framework failure
  - Data corruption detected
  - Security breach attempt

## 12. Custos e TCO

### 12.1 Custos de Implementação

| Item | Custo | Tipo | Observação |
|------|-------|------|------------|
| SQL Server License | R$ 450.000 | CAPEX | Enterprise Edition, 64 cores |
| Hardware Servers | R$ 380.000 | CAPEX | 2x servers + storage |
| Desenvolvimento | R$ 520.000 | CAPEX | 6 meses, 4 pessoas |
| Consultoria | R$ 120.000 | CAPEX | Arquitetura e best practices |
| **Total Implementação** | **R$ 1.470.000** | | |

### 12.2 TCO Projetado (5 anos)

- **Ano 1**: R$ 1.470.000 (implementação) + R$ 180.000 (operação) = R$ 1.650.000
- **Ano 2-5**: R$ 240.000/ano (operação + evolução)
- **Total 5 anos**: R$ 2.610.000
- **Economia estimada**: R$ 3.500.000 (redução de 4 FTEs em processos manuais)
- **ROI**: 134% em 5 anos

## 13. Riscos e Mitigações

| Risco | Probabilidade | Impacto | Mitigação |
|-------|---------------|---------|-----------|
| Qualidade dados fonte | Alta | Alto | Data quality framework desde início |
| Resistência mudança | Média | Médio | Change management, treinamentos |
| Performance inadequada | Baixa | Alto | POC, tuning contínuo, hardware adequado |
| Skills gap equipe | Média | Médio | Treinamento, consultoria, hiring |
| Crescimento além do esperado | Média | Médio | Arquitetura escalável, capacity planning |
| Falha de integração | Baixa | Alto | Testes extensivos, fallback procedures |
| Documentação incompleta | Média | Médio | Schema docs automatizado, políticas |

## 14. Roadmap de Evolução

| Release | Features | Data | Dependências |
|---------|----------|------|--------------|
| v1.0 | Bronze + Silver core | Q1 2025 | Infraestrutura |
| v1.1 | Gold dimensional model | Q2 2025 | v1.0 |
| v1.2 | PowerBI integration | Q2 2025 | v1.1 |
| v1.3 | Docs + Metadados schemas | Q2 2025 | v1.1 |
| v2.0 | Platinum layer | Q3 2025 | v1.3 |
| v2.1 | ML feature store | Q3 2025 | v2.0 |
| v2.2 | LLM semantic layer | Q4 2025 | v2.0 |
| v3.0 | Real-time streaming | Q1 2026 | v2.2 |

## 15. Referências

### 15.1 Documentos Relacionados
- [POL-GOV-001]: Política de Performance e Desempenho
- [MAN-ANA-001]: Manual de Analytics
- [MOD-001]: Modelo Dimensional Gold Layer
- [ETL-001]: Pipeline Bronze-Silver Patterns
- [GOV-001]: Política de Governança de Dados
- [SEC-001]: Política de Segurança DW

### 15.2 Padrões e Frameworks
- Microsoft Modern Data Warehouse Reference Architecture
- Medallion Architecture (Databricks)
- TDWI Data Warehouse Maturity Model
- ISO 27001/27701 (Segurança e Privacidade)

### 15.3 Ferramentas de Documentação
- **Diagramas**: Draw.io para C4 Model
- **Modelagem de Dados**: SQL Server Data Tools
- **Documentação**: Confluence + GitHub
- **Data Catalog**: Schema Docs (interno) + Azure Purview (futuro)

## 16. Controle de Revisões

| Versão | Data | Autor | Descrição da Alteração |
|--------|------|-------|------------------------|
| 1.0.0 | 2025-06-03 | Equipe Dados | Criação inicial do documento |
| 2.0.0 | 2025-01-13 | Arquiteto Dados | Revisão completa: SQL Server, camada Platinum, detalhamento completo |
| 2.1.0 | 2025-01-13 | Arquiteto Dados | Ajustes: dependências corretas, stakeholders atualizados, drivers baseados em OKRs, schemas docs e metadados |
| 2.2.0 | 2025-06-13 | Bruno Chiaramonti | Alinhamento organizacional: responsável atualizado, esclarecimento que Dados/ML/IA é subárea de Performance e Desempenho |

---

**Notas de Implementação**:
- A área de Performance e Desempenho lidera a estratégia analítica, com a equipe de Dados/ML/IA como implementadora técnica
- A camada Platinum é crítica para servir LLMs e ML models com dados pré-processados
- SQL Server foi escolhido pela expertise interna e integração com ecossistema Microsoft
- Schemas docs e metadados são fundamentais para governança e auto-documentação
- Performance tuning será contínuo, especialmente para queries PowerBI
- Considerar particionamento temporal para tabelas fato grandes
- Implementar Column Store Indexes para melhor performance analítica
- Schema docs deve ser populado automaticamente via triggers e procedures
- Schema metadados captura linhagem e qualidade em tempo real