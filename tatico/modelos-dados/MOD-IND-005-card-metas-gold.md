---
título: Modelo de Dados Card Metas Gold - Performance Tracking EAV
tipo: MOD
código: MOD-IND-005
versão: 1.0.0
data_criação: 2025-01-18
última_atualização: 2025-01-18
próxima_revisão: 2025-07-18
responsável: bruno.chiaramonti@multisete.com
aprovador: arquitetura.dados@m7investimentos.com.br
tags: [modelo, gold, performance, eav, indicadores, card, metas]
status: aprovado
confidencialidade: interno
---

# MOD-IND-005 - Modelo de Dados Card Metas Gold

## 1. Objetivo

Documentar o modelo de dados da tabela `gold.card_metas` e a procedure `gold.prc_process_performance_to_gold` que implementam a camada Gold do sistema de Performance Tracking. Este modelo utiliza o padrão EAV (Entity-Attribute-Value) para suportar indicadores dinâmicos e cálculos personalizados por pessoa, permitindo flexibilidade total sem alterações estruturais.

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: Gestão de Performance / Comercial / Analytics
- **Processos suportados**: 
  - Cálculo mensal de performance individualizada
  - Execução dinâmica de fórmulas SQL por indicador
  - Ponderação e scoring de performance
  - Rankings e análises comparativas
  - Suporte a dashboards e ML
- **Stakeholders**: 
  - Diretoria Comercial
  - Gestão de Performance
  - Controladoria (comissionamento)
  - Analytics e BI
  - Assessores de Investimento

### 2.2 Contexto Técnico
- **Tipo de modelo**: EAV (Entity-Attribute-Value)
- **Plataforma**: SQL Server 2019+
- **Schema**: gold
- **Layer**: Gold (dados calculados e agregados)
- **Padrão**: Modelo genérico com execução dinâmica de fórmulas

## 3. Visão Geral do Modelo

### 3.1 Diagrama Conceitual - Fluxo de Processamento
```
┌─────────────────────────┐         ┌─────────────────────────┐
│  silver.performance_    │         │  silver.performance_    │
│      indicators         │         │      assignments        │
├─────────────────────────┤         ├─────────────────────────┤
│ • indicator_id          │◄────────┤ • indicator_id (FK)     │
│ • calculation_formula   │         │ • codigo_assessor_crm   │
│ • aggregation_method    │         │ • indicator_weight      │
│ • is_inverted           │         │ • valid_from/to         │
└───────────┬─────────────┘         └───────────┬─────────────┘
            │                                   │
            └─────────────┬─────────────────────┘
                          ▼
            ┌─────────────────────────┐
            │ gold.prc_process_      │
            │ performance_to_gold     │
            ├─────────────────────────┤
            │ Executa fórmulas SQL   │
            │ dinâmicamente para      │
            │ cada pessoa/indicador  │
            └───────────┬─────────────┘
                        ▼
            ┌─────────────────────────┐         ┌─────────────────────────┐
            │   gold.card_metas      │         │ gold.processing_log     │
            ├─────────────────────────┤         ├─────────────────────────┤
            │ Modelo EAV:             │         │ Log de execuções        │
            │ • entity (assessor)     │◄────────┤ • processing_id         │
            │ • attribute (indicador) │         │ • statistics            │
            │ • value (realizado)     │         │ • errors                │
            └─────────────────────────┘         └─────────────────────────┘
```

### 3.2 Arquitetura EAV (Entity-Attribute-Value)
```
ENTITY (Quem?)          ATTRIBUTE (O quê?)       VALUE (Quanto?)
├─ entity_type     ──── attribute_type      ──── realized_value
├─ entity_id       ──── attribute_code      ──── target_value
│  (assessor CRM)       (indicator code)         achievement_%
│                                               weighted_achievement
│
Exemplo:
'ASSESSOR'/'AAI001' -- 'INDICATOR'/'CAPT_LIQ' -- 450000.00 / 90% / 36%
```

### 3.3 Principais Entidades
| Entidade | Tipo | Descrição | Volume Estimado |
|----------|------|-----------|-----------------|
| card_metas | Fato EAV | Resultados calculados de performance | 10K registros/mês |
| processing_log | Log | Rastreabilidade de execuções | 50-100 registros/mês |
| Indicators (Silver) | Dimensão | Metadados e fórmulas | 20-50 indicadores |
| Assignments (Silver) | Bridge | Pessoa × Indicador × Peso | 500-1000 ativos |

## 4. Dicionário de Dados Detalhado

### 4.1 Tabela: gold.card_metas

**Descrição**: Tabela principal em modelo EAV que armazena os resultados calculados de performance, suportando N indicadores dinâmicos sem alteração de estrutura.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| meta_id | INT | PK, IDENTITY, NOT NULL | ID único do registro | 12345 | Auto-incremento |
| period_start | DATE | NOT NULL | Início do período | "2025-01-01" | Sempre dia 01 |
| period_end | DATE | NOT NULL | Fim do período | "2025-01-31" | Último dia do mês |
| entity_type | VARCHAR(50) | NOT NULL, DEFAULT 'ASSESSOR' | Tipo da entidade | "ASSESSOR" | Fixo por enquanto |
| entity_id | VARCHAR(20) | NOT NULL | Código do assessor | "AAI001" | = codigo_assessor_crm |
| attribute_type | VARCHAR(50) | NOT NULL, DEFAULT 'INDICATOR' | Tipo do atributo | "INDICATOR" | Fixo por enquanto |
| attribute_code | VARCHAR(50) | NOT NULL | Código do indicador | "CAPT_LIQ" | = indicator_code |
| attribute_name | VARCHAR(200) | NOT NULL | Nome do indicador | "Captação Líquida" | Descritivo |
| indicator_type | VARCHAR(20) | NOT NULL, CHECK | Tipo do indicador | "CARD" | CARD, GATILHO, KPI, PPI |
| indicator_category | VARCHAR(50) | NULL | Categoria | "FINANCEIRO" | Classificação |
| target_value | DECIMAL(18,4) | NULL | Meta do período | 500000.0000 | Vem de targets |
| stretch_value | DECIMAL(18,4) | NULL | Meta desafio | 600000.0000 | Opcional |
| minimum_value | DECIMAL(18,4) | NULL | Meta mínima | 400000.0000 | Opcional |
| realized_value | DECIMAL(18,4) | NULL | Valor calculado | 450000.0000 | Via fórmula SQL |
| achievement_percentage | DECIMAL(5,2) | NULL, CHECK | % atingimento | 90.00 | -999.99 a 999.99 |
| indicator_weight | DECIMAL(5,2) | NOT NULL, DEFAULT 0 | Peso do indicador | 40.00 | 0-100, CARD soma 100% |
| weighted_achievement | DECIMAL(5,2) | NULL | Atingimento × Peso | 36.00 | Apenas para CARD |
| achievement_status | VARCHAR(20) | NULL, CHECK | Status | "ATINGIDO" | SUPERADO, ATINGIDO, etc |
| is_inverted | BIT | NOT NULL, DEFAULT 0 | Indicador invertido | 0 | 1 = menor é melhor |
| is_calculated | BIT | NOT NULL, DEFAULT 0 | Cálculo realizado | 1 | 1 = processado |
| has_error | BIT | NOT NULL, DEFAULT 0 | Erro no cálculo | 0 | 1 = fórmula falhou |
| calculation_formula | VARCHAR(MAX) | NULL | Fórmula SQL | "SELECT SUM..." | Auditoria |
| calculation_method | VARCHAR(20) | NULL | Método agregação | "SUM" | SUM, AVG, COUNT, etc |
| data_source | VARCHAR(100) | NULL | Tabela origem | "gold.captacao_liquida" | Rastreabilidade |
| processing_date | DATETIME | NOT NULL, DEFAULT | Data processamento | "2025-02-05 10:30" | Automático |
| processing_id | INT | NULL | ID da execução | 123 | Link com log |
| processing_duration_ms | INT | NULL | Tempo de cálculo | 245 | Milliseconds |
| processing_notes | VARCHAR(MAX) | NULL | Notas/erros | "Erro: divisão por zero" | Debug |
| created_date | DATETIME | NOT NULL, DEFAULT | Data criação | "2025-02-05 10:30" | Automático |
| created_by | VARCHAR(100) | NOT NULL, DEFAULT | Usuário criador | "prc_process_gold" | Sistema |
| modified_date | DATETIME | NULL | Data modificação | NULL | Se houver update |
| modified_by | VARCHAR(100) | NULL | Usuário modificador | NULL | Se houver update |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_gold_card_metas | CLUSTERED | meta_id | Chave primária |
| UQ_gold_card_metas_unique | UNIQUE | entity_id, attribute_code, period_start | Evitar duplicatas |
| IX_gold_card_metas_entity | NONCLUSTERED | entity_id, period_start | Queries por assessor |
| IX_gold_card_metas_period | NONCLUSTERED | period_start, period_end | Queries por período |
| IX_gold_card_metas_attribute | NONCLUSTERED | attribute_code | Análise por indicador |
| IX_gold_card_metas_card_type | FILTERED | indicator_type, period_start WHERE type='CARD' | Filtro CARD |
| IX_gold_card_metas_errors | FILTERED | has_error, processing_date WHERE error=1 | Debug erros |

### 4.2 Tabela: gold.processing_log

**Descrição**: Log detalhado de todas as execuções do processamento Gold para rastreabilidade e troubleshooting.

| Campo | Tipo | Constraint | Descrição | Exemplo |
|-------|------|------------|-----------|---------|
| log_id | INT | PK, IDENTITY | ID do log | 456 |
| processing_id | INT | NOT NULL | ID único da execução | 123 |
| processing_type | VARCHAR(50) | NOT NULL | Tipo processamento | "FULL" |
| period_start | DATE | NULL | Período processado | "2025-01-01" |
| period_end | DATE | NULL | Fim período | "2025-01-31" |
| entity_id | VARCHAR(20) | NULL | Se específico | "AAI001" |
| start_time | DATETIME | NOT NULL | Início execução | "2025-02-05 03:00:00" |
| end_time | DATETIME | NULL | Fim execução | "2025-02-05 03:15:23" |
| duration_seconds | INT | NULL | Duração total | 923 |
| total_entities | INT | NULL | Assessores processados | 485 |
| total_indicators | INT | NULL | Indicadores únicos | 18 |
| total_calculations | INT | NULL | Total cálculos | 8730 |
| successful_calculations | INT | NULL | Cálculos OK | 8650 |
| failed_calculations | INT | NULL | Cálculos com erro | 80 |
| status | VARCHAR(20) | NOT NULL | Status final | "WARNING" |
| error_message | VARCHAR(MAX) | NULL | Se erro geral | NULL |
| warning_messages | VARCHAR(MAX) | NULL | Avisos | "80 fórmulas falharam" |
| executed_by | VARCHAR(100) | NOT NULL | Usuário/sistema | "SQL Agent Job" |
| execution_notes | VARCHAR(MAX) | NULL | Observações | "Execução mensal" |

## 5. Relacionamentos e Integridade

### 5.1 Relacionamentos Lógicos (não são FKs físicas)
| Origem | Campo | Destino | Campo | Tipo | Descrição |
|--------|-------|---------|-------|------|-----------|
| card_metas | entity_id | dim_pessoas | codigo_assessor_crm | N:1 | Assessor |
| card_metas | attribute_code | performance_indicators | indicator_code | N:1 | Indicador |
| card_metas | processing_id | processing_log | processing_id | N:1 | Execução |

### 5.2 Cardinalidade
- **Pessoa × Período → N Indicadores**: Um assessor tem vários indicadores por mês
- **Indicador × Período → N Pessoas**: Um indicador é calculado para várias pessoas
- **Execução → N Registros**: Uma execução processa múltiplos cálculos

## 6. Regras de Negócio e Validações

### 6.1 Regras Críticas
| Regra | Implementação | Descrição | Severidade |
|-------|---------------|-----------|------------|
| RN001 | Procedure validation | Soma pesos CARD = 100% por pessoa | ERROR |
| RN002 | Unique constraint | Um registro por pessoa/indicador/período | ERROR |
| RN003 | Dynamic SQL | Fórmulas executadas com sp_executesql | SECURITY |
| RN004 | Check logic | Achievement invertido: (2 - real/target) × 100 | CRITICAL |
| RN005 | Processing rule | CARD tem peso > 0, outros = 0 | WARNING |
| RN006 | Status logic | Status baseado em % achievement | INFO |

### 6.2 Lógica de Achievement
```sql
-- Indicador Normal (maior é melhor)
achievement_pct = (realized_value / target_value) × 100

-- Indicador Invertido (menor é melhor)
achievement_pct = (2 - (realized_value / target_value)) × 100

-- Status
SUPERADO:      achievement >= 120%
ATINGIDO:      achievement >= 100%
PARCIAL:       achievement >= 80%
NAO_ATINGIDO:  achievement < 80%
```

## 7. Processamento Dinâmico

### 7.1 Fluxo da Procedure prc_process_performance_to_gold
```
1. INICIALIZAÇÃO
   ├─ Determinar período (default: mês anterior)
   ├─ Criar log de processamento
   └─ Limpar dados anteriores (se reprocessamento)

2. IDENTIFICAR PESSOAS
   ├─ Buscar assessores com assignments ativos
   └─ Filtrar por targets existentes no período

3. LOOP POR PESSOA
   ├─ Buscar indicadores atribuídos
   ├─ Recuperar fórmulas e metadados
   └─ LOOP POR INDICADOR
       ├─ Executar fórmula SQL dinâmica
       ├─ Calcular achievement (normal/invertido)
       ├─ Calcular weighted (se CARD)
       ├─ Determinar status
       └─ Gravar em card_metas

4. FINALIZAÇÃO
   ├─ Atualizar estatísticas no log
   ├─ Executar validações básicas
   └─ Retornar status
```

### 7.2 Execução de Fórmulas Dinâmicas
```sql
-- Exemplo de fórmula armazenada
formula = 'SELECT SUM(captacao_liquida_total) 
           FROM gold.captacao_liquida_assessor 
           WHERE codigo_assessor_crm = @entity_id 
             AND data_movimento BETWEEN @period_start AND @period_end'

-- Execução segura com parâmetros
EXEC sp_executesql 
    @formula,
    N'@entity_id VARCHAR(20), @period_start DATE, @period_end DATE',
    @entity_id = 'AAI001',
    @period_start = '2025-01-01',
    @period_end = '2025-01-31'
```

## 8. Performance e Otimização

### 8.1 Estratégias de Performance
| Estratégia | Implementação | Benefício |
|------------|---------------|-----------|
| Índices específicos | 6 índices otimizados | Queries < 100ms |
| Processamento em lote | Transação por pessoa | Isolamento de falhas |
| Paralelismo controlado | MAXDOP hint se necessário | Uso eficiente CPU |
| Estatísticas atualizadas | Update stats semanal | Planos otimizados |

### 8.2 Volumes e Métricas
- **Volume mensal**: ~10.000 registros (500 pessoas × 20 indicadores)
- **Tempo processamento**: 10-15 minutos total
- **Queries típicas**: < 2 segundos via views
- **Crescimento anual**: ~120.000 registros

## 9. Segurança e Governança

### 9.1 Classificação de Dados
| Dado | Classificação | Tratamento |
|------|---------------|------------|
| Metas e realizados | Confidencial | Acesso restrito |
| Fórmulas SQL | Restrito | Apenas admins |
| Rankings/scores | Interno | Gestores e acima |

### 9.2 Controles de Segurança
- **SQL Injection**: Prevenido com sp_executesql e binding
- **Permissões**: Execução apenas via procedure
- **Auditoria**: Todo processamento logado
- **RLS**: Pode ser aplicado por assessor

## 10. Integração e Dependências

### 10.1 Fluxo de Dados
```
Google Sheets → Bronze → Silver → GOLD → BI/ML
                                    │
Indicators ────────────────────────┤
Assignments ───────────────────────┤
Targets ───────────────────────────┘
```

### 10.2 Consumidores
| Sistema | Interface | Uso | SLA |
|---------|-----------|-----|-----|
| Power BI | Views Gold | Dashboards real-time | < 2s |
| Comissionamento | Views/Direct | Cálculo RV | 100% accuracy |
| ML Platform | Tables | Features engineering | Daily sync |
| APIs | Views | Consultas | < 100ms |

## 11. Exemplos de Uso

### 11.1 Processar Período
```sql
-- Processar mês anterior (padrão)
EXEC gold.prc_process_performance_to_gold;

-- Processar período específico
EXEC gold.prc_process_performance_to_gold 
    @period_start = '2025-01-01',
    @period_end = '2025-01-31',
    @debug = 1;

-- Reprocessar assessor específico
EXEC gold.prc_process_performance_to_gold 
    @period_start = '2025-01-01',
    @crm_id = 'AAI001';
```

### 11.2 Consultar Resultados
```sql
-- Score total por assessor
SELECT * FROM gold.vw_card_metas_weighted_score
WHERE period_start = '2025-01-01'
ORDER BY score_ponderado DESC;

-- Ranking por indicador
SELECT * FROM gold.vw_card_metas_ranking
WHERE period_start = '2025-01-01'
  AND indicador = 'CAPT_LIQ'
  AND ranking_indicador <= 10;

-- Dashboard completo
SELECT * FROM gold.vw_card_metas_dashboard
WHERE is_periodo_atual = 1;
```

## 12. Monitoramento e Troubleshooting

### 12.1 Queries de Monitoramento
```sql
-- Verificar última execução
SELECT TOP 1 * FROM gold.processing_log 
ORDER BY log_id DESC;

-- Identificar erros de fórmula
SELECT * FROM gold.card_metas
WHERE has_error = 1
  AND processing_date >= DATEADD(DAY, -7, GETDATE());

-- Validar pesos
EXEC gold.prc_validate_processing @debug = 1;
```

### 12.2 Problemas Comuns
| Problema | Diagnóstico | Solução |
|----------|-------------|---------|
| Fórmula falha | has_error = 1 | Revisar SQL no Silver |
| Peso != 100% | Validação falha | Ajustar assignments |
| Performance lenta | Duration > 20min | Revisar índices facts |
| Dados faltando | Assessor sem dados | Verificar targets |

## 13. Evolução e Roadmap

### 13.1 Melhorias Planejadas
- Cache de fórmulas compiladas
- Processamento paralelo por grupo
- Notificações automáticas pós-processamento
- API REST para consultas

### 13.2 Versionamento
- Modelo estável, mudanças via ALTER
- Novas features via procedures
- Backward compatibility garantido

## 14. Histórico de Mudanças

| Versão | Data | Autor | Descrição |
|--------|------|-------|-----------|
| 1.0.0 | 2025-01-18 | bruno.chiaramonti | Criação inicial do documento |
| 1.2.0 | 2025-01-20 | bruno.chiaramonti | Documentação do mapeamento XP→CRM, ProcessingSequence |

## 15. Referências

### 15.1 Scripts SQL
- [QRY-IND-005-create_gold_card_metas.sql](../../operacional/queries/gold/QRY-IND-005-create_gold_card_metas.sql)
- [QRY-IND-006-prc_process_performance_to_gold.sql](../../operacional/queries/gold/QRY-IND-006-prc_process_performance_to_gold.sql)
- [QRY-IND-007-create_gold_performance_views.sql](../../operacional/queries/gold/QRY-IND-007-create_gold_performance_views.sql)
- [QRY-IND-008-prc_validate_processing.sql](../../operacional/queries/gold/QRY-IND-008-prc_validate_processing.sql)

### 15.2 Documentação Relacionada
- [ARQ-IND-001 - Performance Tracking System](../../estrategico/arquiteturas/ARQ-IND-001-performance-tracking-system.md)
- [MOD-IND-002 - Performance Indicators Silver](MOD-IND-002-performance-indicators-silver.md)
- [MOD-IND-003 - Performance Assignments Silver](MOD-IND-003-performance-assignments-silver.md)
- [MOD-IND-004 - Performance Targets Silver](MOD-IND-004-performance-targets-silver.md)

---

**Documento criado por**: Bruno Chiaramonti  
**Data**: 2025-01-18  
**Versão**: 1.2.0  
**Status**: Aprovado