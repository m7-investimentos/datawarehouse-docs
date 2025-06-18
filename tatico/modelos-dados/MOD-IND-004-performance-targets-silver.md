---
título: Modelo de Dados Performance Targets Silver - Metas de Performance
tipo: MOD
código: MOD-IND-004
versão: 1.0.0
data_criação: 2025-01-18
última_atualização: 2025-01-18
próxima_revisão: 2025-07-18
responsável: bruno.chiaramonti@multisete.com
aprovador: arquitetura.dados@m7investimentos.com.br
tags: [modelo, performance, targets, metas, silver, dimensional]
status: aprovado
confidencialidade: interno
---

# MOD-IND-004 - Modelo de Dados Performance Targets Silver

## 1. Objetivo

Documentar o modelo de dados da tabela `silver.performance_targets` que armazena as metas mensais de performance por assessor e indicador, suportando análises de acompanhamento de metas, cálculo de atingimento e gestão de performance com dados limpos, validados e com integridade referencial garantida.

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: Comercial / Gestão de Performance
- **Processos suportados**: 
  - Planejamento anual de metas
  - Acompanhamento mensal de performance
  - Cálculo de comissionamento variável
  - Análise de atingimento de metas
- **Stakeholders**: 
  - Diretoria Comercial
  - Gestores de Performance
  - Controladoria
  - Assessores de Investimento

### 2.2 Contexto Técnico
- **Tipo de modelo**: Dimensional (Fato temporal)
- **Plataforma**: SQL Server 2016+
- **Schema**: silver
- **Layer**: Silver (dados limpos e validados)

## 3. Visão Geral do Modelo

### 3.1 Diagrama Entidade-Relacionamento
```
┌─────────────────────────┐         ┌──────────────────────────┐
│  performance_targets    │         │ performance_indicators   │
├─────────────────────────┤         ├──────────────────────────┤
│ PK: target_id          │         │ PK: indicator_id         │
│ FK: indicator_id       │────────<│ indicator_code           │
│ crm_id                 │         │ indicator_name           │
│ period_start           │         │ indicator_type           │
│ period_end             │         │ is_inverted              │
│ target_value           │         │ calculation_formula      │
│ stretch_target         │         └──────────────────────────┘
│ minimum_target         │
│ created_date           │         ┌──────────────────────────┐
│ modified_date          │         │  dim_calendario          │
└─────────────────────────┘         ├──────────────────────────┤
            │                       │ PK: date_key             │
            └──────────────────────>│ full_date                │
                                   │ year                     │
                                   │ month                    │
                                   │ quarter                  │
                                   └──────────────────────────┘
```

### 3.2 Principais Entidades
| Entidade | Tipo | Descrição | Volume Estimado |
|----------|------|-----------|-----------------|
| performance_targets | Fato | Metas mensais por assessor/indicador | 2.5K-3K/mês |
| performance_indicators | Dimensão | Indicadores de performance | 10-20 registros |
| dim_calendario | Dimensão | Dimensão temporal | 3650 registros (10 anos) |

## 4. Dicionário de Dados Detalhado

### 4.1 Tabela: silver.performance_targets

**Descrição**: Armazena metas mensais de performance por assessor e indicador com três níveis: mínimo, padrão e stretch, suportando análise temporal e cálculo de atingimento.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| target_id | INT | PK, IDENTITY(1,1), NOT NULL | ID único da meta | 12345 | Auto-incremento |
| crm_id | VARCHAR(20) | NOT NULL, INDEX | Código do assessor no CRM | "AAI000123" | Formato AAI + números |
| indicator_id | INT | FK, NOT NULL | ID do indicador | 5 | Ref: performance_indicators |
| period_type | VARCHAR(20) | NOT NULL, DEFAULT 'MENSAL' | Tipo do período | "MENSAL" | Sempre MENSAL atualmente |
| period_start | DATE | NOT NULL | Primeiro dia do mês | "2025-01-01" | Sempre dia 01 |
| period_end | DATE | NOT NULL | Último dia do mês | "2025-01-31" | Calculado automaticamente |
| target_value | DECIMAL(18,4) | NOT NULL | Meta padrão | 100000.0000 | Pode ser negativo para % |
| stretch_target | DECIMAL(18,4) | NULL | Meta stretch (desafio) | 120000.0000 | >= target (exceto invertidos) |
| minimum_target | DECIMAL(18,4) | NULL | Meta mínima | 80000.0000 | <= target (exceto invertidos) |
| is_active | BIT | NOT NULL, DEFAULT 1 | Registro ativo | 1 | Soft delete |
| created_date | DATETIME | NOT NULL, DEFAULT GETDATE() | Data de criação | "2025-01-15 10:30:00" | UTC-3 |
| created_by | VARCHAR(100) | NOT NULL, DEFAULT SUSER_SNAME() | Usuário criador | "etl_user" | Auditoria |
| modified_date | DATETIME | NULL | Data última modificação | "2025-01-16 14:20:00" | NULL se nunca modificado |
| modified_by | VARCHAR(100) | NULL | Usuário modificador | "admin_user" | Auditoria |
| source_system | VARCHAR(50) | NOT NULL, DEFAULT 'GoogleSheets' | Sistema origem | "GoogleSheets" | Rastreabilidade |
| source_id | VARCHAR(100) | NULL | ID no sistema origem | "ROW_1234" | Para reconciliação |
| bronze_load_id | INT | NULL | ID da carga bronze | 5678 | Ref: bronze.performance_targets |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_silver_performance_targets | CLUSTERED | target_id | Chave primária |
| UQ_performance_targets_unique | UNIQUE | crm_id, indicator_id, period_start | Evitar duplicatas |
| IX_targets_assessor_period | NONCLUSTERED | crm_id, period_start | Queries por assessor |
| IX_targets_indicator | NONCLUSTERED | indicator_id | Joins com indicators |
| IX_targets_temporal | NONCLUSTERED | period_start, period_end | Análises temporais |

**Particionamento**: Não implementado (volume não justifica)

**Constraints adicionais**:
```sql
-- Validar período
ALTER TABLE silver.performance_targets 
ADD CONSTRAINT CK_targets_period_valid 
CHECK (period_end >= period_start);

-- Validar valores não todos nulos
ALTER TABLE silver.performance_targets
ADD CONSTRAINT CK_targets_has_value
CHECK (target_value IS NOT NULL OR stretch_target IS NOT NULL OR minimum_target IS NOT NULL);
```

## 5. Relacionamentos e Integridade

### 5.1 Chaves Estrangeiras
| Tabela Origem | Campo(s) | Tabela Destino | Campo(s) | Tipo | On Delete | On Update |
|---------------|----------|----------------|----------|------|-----------|-----------|
| performance_targets | indicator_id | performance_indicators | indicator_id | N:1 | RESTRICT | CASCADE |

### 5.2 Cardinalidade dos Relacionamentos
- **Indicator → Target**: 1:N (Um indicador tem várias metas ao longo do tempo)
- **Assessor → Target**: Implícito 1:N via crm_id
- **Período → Target**: 1:N (Um período pode ter metas de vários assessores)

## 6. Regras de Negócio e Validações

### 6.1 Regras de Integridade
| Regra | Implementação | Descrição |
|-------|---------------|-----------|
| RN001 | Procedure Bronze→Silver | Indicador deve existir em performance_indicators |
| RN002 | Unique constraint | Combinação assessor/indicador/período única |
| RN003 | Procedure validação | 12 meses completos por assessor/indicador/ano |
| RN004 | Check no Bronze | Lógica stretch/target/minimum válida |
| RN005 | Default constraint | period_type sempre 'MENSAL' |

### 6.2 Implementação das Regras
```sql
-- RN003: Validação de completude anual (na procedure)
WITH monthly_coverage AS (
    SELECT 
        crm_id,
        indicator_id,
        YEAR(period_start) as target_year,
        COUNT(DISTINCT MONTH(period_start)) as months_count
    FROM silver.performance_targets
    WHERE is_active = 1
    GROUP BY crm_id, indicator_id, YEAR(period_start)
)
SELECT * FROM monthly_coverage
WHERE months_count < 12;

-- RN004: Validação lógica de valores (aplicada no Bronze)
-- Para indicadores normais: stretch >= target >= minimum
-- Para indicadores invertidos: stretch <= target <= minimum
```

## 7. Histórico e Auditoria

### 7.1 Estratégia de Historização
- **Tipo**: Auditoria simples (created/modified)
- **Campos de controle**:
  - `created_date/by`: Registro inicial
  - `modified_date/by`: Última alteração
  - `is_active`: Soft delete
  - `bronze_load_id`: Rastreabilidade à origem

### 7.2 Tabela de Auditoria
```sql
-- Log de mudanças críticas
CREATE TABLE audit.performance_targets_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    target_id INT NOT NULL,
    operation CHAR(1) NOT NULL, -- I/U/D
    operation_date DATETIME NOT NULL DEFAULT GETDATE(),
    operation_user VARCHAR(100) NOT NULL DEFAULT SUSER_SNAME(),
    old_values NVARCHAR(MAX), -- JSON com valores anteriores
    new_values NVARCHAR(MAX), -- JSON com valores novos
    change_reason VARCHAR(500)
);
```

## 8. Performance e Otimização

### 8.1 Estratégias de Indexação
| Padrão de Query | Índice Recomendado | Justificativa |
|-----------------|-------------------|---------------|
| Metas por assessor/mês | IX_targets_assessor_period | Queries mais comuns |
| Análise por indicador | IX_targets_indicator | Agregações por KPI |
| Séries temporais | IX_targets_temporal | Análises de tendência |

### 8.2 Estatísticas e Manutenção
```sql
-- Atualizar estatísticas semanalmente
UPDATE STATISTICS silver.performance_targets WITH FULLSCAN;

-- Rebuild índices mensalmente (baixa fragmentação esperada)
ALTER INDEX ALL ON silver.performance_targets REBUILD;
```

## 9. Segurança e Privacidade

### 9.1 Classificação de Dados
| Campo | Classificação | Tratamento |
|-------|---------------|------------|
| crm_id | Interno | Acesso controlado |
| target_value | Confidencial | Não expor publicamente |
| stretch_target | Confidencial | Restrito a gestão |
| minimum_target | Confidencial | Restrito a gestão |

### 9.2 Políticas de Acesso (RLS)
```sql
-- Row Level Security por assessor
CREATE FUNCTION dbo.fn_security_targets(@crm_id VARCHAR(20))
RETURNS TABLE
WITH SCHEMABINDING
AS RETURN
    SELECT 1 AS access_granted
    WHERE @crm_id = USER_NAME()
       OR IS_ROLEMEMBER('PerformanceManager') = 1
       OR IS_ROLEMEMBER('db_owner') = 1;

-- Aplicar política
CREATE SECURITY POLICY TargetsSecurityPolicy
ADD FILTER PREDICATE dbo.fn_security_targets(crm_id)
ON silver.performance_targets
WITH (STATE = ON);
```

## 10. Integração e Dependências

### 10.1 Sistemas Fonte
| Sistema | Tabelas/APIs | Frequência Sync | Método |
|---------|--------------|-----------------|---------|
| Google Sheets | m7_performance_targets | Mensal | ETL-IND-003 |
| Bronze Layer | bronze.performance_targets | Sob demanda | Procedure QRY-TAR-003 |

### 10.2 Sistemas Consumidores
| Sistema | Uso | Requisitos |
|---------|-----|------------|
| Gold Performance | Cálculo de atingimento | Dados completos 12 meses |
| BI/Dashboards | Acompanhamento de metas | Latência < 1 dia |
| Sistema Comissionamento | Cálculo variável | 100% accuracy |

## 11. Evolução e Versionamento

### 11.1 Estratégia de Versionamento
- **Schema versioning**: Procedures versionadas (v1.0.0, v2.0.0)
- **Compatibilidade**: Manter retrocompatibilidade por 6 meses
- **Deprecação**: Comunicação com 30 dias de antecedência

### 11.2 Processo de Mudança
1. **Análise de impacto**: Verificar dependências Gold/Platinum
2. **Teste em DEV**: Validar com dados de produção anonimizados
3. **Aprovação**: Gestão de Performance + Arquitetura de Dados
4. **Deploy**: Durante janela de manutenção
5. **Validação**: Reconciliação antes/depois

## 12. Qualidade e Governança

### 12.1 Regras de Qualidade
| Dimensão | Métrica | Target | Medição |
|----------|---------|--------|---------|
| Completude | % assessores com 12 meses | > 95% | Mensal |
| Unicidade | % duplicatas | 0% | Real-time |
| Consistência | % lógica válida | 100% | Diário |
| Atualidade | Lag do Bronze | < 24h | Diário |
| Acurácia | % reconciliado com fonte | 100% | Mensal |

### 12.2 Data Lineage
```
Google Sheets (m7_performance_targets)
    ↓ [ETL-IND-003: Python extração]
Bronze.performance_targets
    ↓ [QRY-TAR-003: Procedure transformação]
Silver.performance_targets
    ↓ [Procedures Gold]
Gold.performance_metrics
```

## 13. Exemplos e Casos de Uso

### 13.1 Queries Comuns
```sql
-- Metas do mês atual por assessor
SELECT 
    t.crm_id,
    i.indicator_name,
    t.target_value,
    t.stretch_target,
    t.minimum_target
FROM silver.performance_targets t
INNER JOIN silver.performance_indicators i ON t.indicator_id = i.indicator_id
WHERE t.period_start = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
  AND t.is_active = 1
ORDER BY t.crm_id, i.indicator_code;

-- Análise YoY de evolução de metas
WITH yearly_targets AS (
    SELECT 
        crm_id,
        indicator_id,
        YEAR(period_start) as target_year,
        SUM(target_value) as annual_target
    FROM silver.performance_targets
    WHERE is_active = 1
    GROUP BY crm_id, indicator_id, YEAR(period_start)
)
SELECT 
    curr.crm_id,
    curr.indicator_id,
    curr.target_year,
    curr.annual_target,
    prev.annual_target as prev_year_target,
    CASE 
        WHEN prev.annual_target > 0 
        THEN ((curr.annual_target - prev.annual_target) / prev.annual_target * 100)
        ELSE NULL 
    END as growth_percent
FROM yearly_targets curr
LEFT JOIN yearly_targets prev 
    ON curr.crm_id = prev.crm_id 
    AND curr.indicator_id = prev.indicator_id
    AND curr.target_year = prev.target_year + 1
ORDER BY curr.crm_id, curr.indicator_id, curr.target_year;
```

### 13.2 Padrões de Uso
- **Planejamento**: Carga anual em dezembro para ano seguinte
- **Acompanhamento**: Queries mensais para dashboards
- **Comissionamento**: Join com realizados para cálculo de atingimento

## 14. Troubleshooting

### 14.1 Problemas Comuns
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| Metas faltando | < 12 meses | Query completude | Completar no Google Sheets |
| Duplicatas | Erro unique constraint | Verificar Bronze | Limpar Bronze e reprocessar |
| Indicador inválido | FK violation | indicator_code não existe | Cadastrar indicador primeiro |
| Valores inconsistentes | Stretch < Target (normal) | Validação Bronze | Corrigir na planilha origem |

## 15. Referências e Anexos

### 15.1 Scripts DDL Completos
- [QRY-TAR-002-create_silver_performance_targets.sql](../../operacional/queries/silver/QRY-TAR-002-create_silver_performance_targets.sql)

### 15.2 Documentação Relacionada
- [ETL-IND-003 - Processo de Extração de Metas](../processos-etl/ETL-IND-003-extracao-metas-performance.md)
- [QRY-TAR-003 - Procedure Bronze to Silver](../../operacional/queries/bronze/QRY-TAR-003-prc_bronze_to_silver_performance_targets.sql)
- [MOD-IND-001 - Sistema Tracking Performance KPIs](MOD-IND-001-sistema-tracking-performance-kpis.md)
- [MOD-IND-002 - Performance Indicators Silver](MOD-IND-002-performance-indicators-silver.md)

### 15.3 Ferramentas e Recursos
- SQL Server Management Studio para administração
- Power BI para visualizações
- Python scripts para carga (etl_003_targets.py)

---

**Documento criado por**: Arquitetura de Dados M7 Investimentos  
**Última revisão**: 2025-01-18