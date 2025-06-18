---
título: Modelo de Dados Performance Assignments - Camada Silver
tipo: MOD
código: MOD-IND-003
versão: 1.0.0
data_criação: 2025-01-18
última_atualização: 2025-01-18
próxima_revisão: 2025-07-18
responsável: bruno.chiaramonti@multisete.com
aprovador: arquitetura.dados@m7investimentos.com.br
tags: [modelo, dados, performance, assignments, silver, atribuições]
status: aprovado
confidencialidade: interno
---

# MOD-IND-003 - Modelo de Dados Performance Assignments Silver

## 1. Objetivo

Definir o modelo de dados da tabela `silver.performance_assignments` que armazena as atribuições validadas e processadas de indicadores de performance por assessor. Esta tabela é fundamental para o sistema de gestão de performance, estabelecendo quais indicadores cada assessor deve ser avaliado e com qual peso, servindo como base para cálculos de performance, remuneração variável e rankings.

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: Gestão de Performance / Comercial / RH
- **Processos suportados**: 
  - Atribuição de indicadores a assessores
  - Definição de pesos por indicador
  - Cálculo de performance individual
  - Remuneração variável
  - Rankings e comparações
- **Stakeholders**: 
  - Gestão de Performance
  - Diretoria Comercial
  - Recursos Humanos
  - Assessores de Investimento

### 2.2 Contexto Técnico
- **Tipo de modelo**: Fato (relaciona dimensões Assessor e Indicador)
- **Plataforma**: SQL Server 2019+
- **Schema**: silver
- **Layer**: Silver (Dados validados e conformados)
- **Padrão**: Modelo relacional com versionamento temporal

## 3. Visão Geral do Modelo

### 3.1 Diagrama Entidade-Relacionamento
```
┌─────────────────────────┐
│ bronze.performance_     │
│      assignments        │
├─────────────────────────┤
│ Dados brutos do Google  │
│ Sheets (staging)        │
└───────────┬─────────────┘
            │ prc_bronze_to_silver_assignments
            ▼
┌─────────────────────────┐         ┌─────────────────────────┐
│  silver.performance_    │◄────────┤ silver.performance_     │
│      assignments        │ N:1     │      indicators         │
├─────────────────────────┤         ├─────────────────────────┤
│ PK: assignment_id       │         │ PK: indicator_id        │
│ FK: indicator_id        │         │ UK: indicator_code      │
│ Atribuições validadas   │         │ Metadados indicadores   │
└───────────┬─────────────┘         └─────────────────────────┘
            │                                   
            ▼                                   
┌─────────────────────────┐         ┌─────────────────────────┐
│  silver.dim_pessoas     │         │   gold.performance_     │
├─────────────────────────┤         │      calculations       │
│ PK: pessoa_id          │         ├─────────────────────────┤
│ UK: crm_id             │◄────────┤ Cálculos de performance │
│ Dimensão assessores    │ 1:N     │ usando assignments      │
└─────────────────────────┘         └─────────────────────────┘
```

### 3.2 Principais Entidades
| Entidade | Tipo | Descrição | Volume Estimado |
|----------|------|-----------|-----------------|
| performance_assignments | Fato | Atribuições indicador-assessor | 200-500 registros ativos |
| performance_indicators | Dimensão | Configuração de indicadores | 10-50 registros |
| dim_pessoas | Dimensão | Dados de assessores | 100-200 assessores ativos |
| performance_targets | Fato | Metas por período | 500-1000 registros/mês |

## 4. Dicionário de Dados Detalhado

### 4.1 Tabela: silver.performance_assignments

**Descrição**: Tabela que armazena as atribuições validadas de indicadores de performance por assessor, incluindo pesos, vigência e controles de aprovação.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| assignment_id | INT | PK, IDENTITY(1,1), NOT NULL | ID único da atribuição | 1 | Auto-incremento |
| crm_id | VARCHAR(20) | NOT NULL | CRM do assessor | "AAI001" | Formato AAI### ou similar |
| indicator_id | INT | FK, NOT NULL | ID do indicador | 5 | Referência silver.performance_indicators |
| indicator_weight | DECIMAL(5,2) | NOT NULL, DEFAULT 0.00 | Peso do indicador (%) | 35.00 | 0.00-100.00, soma CARD = 100% |
| valid_from | DATE | NOT NULL | Início da vigência | "2025-01-01" | Não pode ser futuro |
| valid_to | DATE | NULL | Fim da vigência | NULL | NULL = vigente, > valid_from |
| created_date | DATETIME | NOT NULL, DEFAULT GETDATE() | Data de criação | "2025-01-01 10:30:00" | Automático |
| created_by | VARCHAR(100) | NOT NULL | Usuário criador | "ETL_SYSTEM" | User ou processo |
| modified_date | DATETIME | NULL | Data de modificação | "2025-01-15 14:20:00" | Atualizado em UPDATE |
| modified_by | VARCHAR(100) | NULL | Usuário modificador | "gestor.performance" | User ou processo |
| approved_date | DATETIME | NULL | Data de aprovação | "2025-01-02 09:00:00" | Quando aprovado |
| approved_by | VARCHAR(100) | NULL | Usuário aprovador | "diretor.comercial" | Email do aprovador |
| is_active | BIT | NOT NULL, DEFAULT 1 | Status ativo | 1 | 0 = desativado |
| comments | NVARCHAR(1000) | NULL | Comentários | "Ajuste trimestral" | Texto livre |
| bronze_load_id | INT | NULL | Referência carga Bronze | 123 | Para rastreabilidade |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_silver_performance_assignments | CLUSTERED | assignment_id | Chave primária |
| FK_performance_assignments_indicators | NONCLUSTERED | indicator_id | Foreign key |
| UQ_performance_assignments_active | UNIQUE NONCLUSTERED | crm_id, indicator_id, valid_from | Evitar duplicatas ativas |
| IX_performance_assignments_assessor | NONCLUSTERED | crm_id, valid_from, valid_to | Busca por assessor |
| IX_performance_assignments_indicator | NONCLUSTERED | indicator_id, is_active | Busca por indicador |
| IX_performance_assignments_current | NONCLUSTERED | valid_from, valid_to | Vigência atual |

**Constraints adicionais**:
```sql
-- CHECK constraint para peso
CONSTRAINT [CK_performance_assignments_weight] CHECK (
    [indicator_weight] >= 0.00 AND [indicator_weight] <= 100.00
)

-- CHECK constraint para datas
CONSTRAINT [CK_performance_assignments_dates] CHECK (
    [valid_to] IS NULL OR [valid_to] > [valid_from]
)
```

## 5. Relacionamentos e Integridade

### 5.1 Chaves Estrangeiras
| Tabela Origem | Campo(s) | Tabela Destino | Campo(s) | Tipo | On Delete | On Update |
|---------------|----------|----------------|----------|------|-----------|-----------|
| performance_assignments | indicator_id | performance_indicators | indicator_id | N:1 | RESTRICT | CASCADE |
| performance_assignments | crm_id | dim_pessoas | crm_id | N:1 | - | - |

*Nota: Relacionamento com dim_pessoas é lógico, não físico (FK)*

### 5.2 Cardinalidade dos Relacionamentos
- **assignments → indicators**: N:1 (Muitas atribuições para um indicador)
- **assignments → assessores**: N:1 (Muitas atribuições para um assessor)
- **assessor + período → assignments**: 1:N (Um assessor tem várias atribuições por período)

## 6. Regras de Negócio e Validações

### 6.1 Regras de Integridade
| Regra | Implementação | Descrição |
|-------|---------------|-----------|
| RN001 | Procedure validation | Soma de pesos CARD deve ser 100% por assessor/período |
| RN002 | UNIQUE constraint | Não pode haver duplicata de crm_id + indicator_id + valid_from |
| RN003 | FK constraint | indicator_id deve existir em performance_indicators |
| RN004 | CHECK constraint | Peso deve estar entre 0 e 100 |
| RN005 | Business logic | Indicadores não-CARD devem ter peso = 0 |
| RN006 | Trigger | Log de mudanças em tabela de auditoria |

### 6.2 Implementação das Regras
```sql
-- RN001: Validação de soma de pesos (na procedure)
WITH weight_check AS (
    SELECT 
        a.crm_id,
        a.valid_from,
        SUM(CASE WHEN i.category = 'CARD' THEN a.indicator_weight ELSE 0 END) as total_weight
    FROM silver.performance_assignments a
    INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
    WHERE a.is_active = 1
      AND a.valid_to IS NULL
    GROUP BY a.crm_id, a.valid_from
)
SELECT * FROM weight_check
WHERE ABS(total_weight - 100.00) >= 0.01;

-- RN005: Peso zero para não-CARD (na inserção)
CASE 
    WHEN i.indicator_type = 'CARD' THEN 
        ISNULL(TRY_CAST(b.weight AS DECIMAL(5,2)), 0.00)
    ELSE 0.00
END as indicator_weight
```

## 7. Histórico e Auditoria

### 7.1 Estratégia de Historização
- **Tipo**: Versionamento temporal com valid_from/valid_to
- **Campos de controle**:
  - `valid_from`: Início da validade da atribuição
  - `valid_to`: Fim da validade (NULL para registro atual)
  - `is_active`: Flag indicando se está ativo
  - `bronze_load_id`: Referência à carga original
  
### 7.2 View para Atribuições Vigentes
```sql
CREATE VIEW silver.vw_performance_assignments_current
AS
WITH AssignmentsSummary AS (
    SELECT 
        a.crm_id,
        i.category,
        SUM(a.indicator_weight) as total_weight,
        COUNT(*) as indicator_count,
        STRING_AGG(CAST(i.indicator_code AS NVARCHAR(MAX)), ', ') 
            WITHIN GROUP (ORDER BY a.indicator_weight DESC) as indicators
    FROM silver.performance_assignments a
    INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
    WHERE a.is_active = 1
      AND a.valid_to IS NULL
      AND GETDATE() >= a.valid_from
    GROUP BY a.crm_id, i.category
)
SELECT 
    crm_id,
    category,
    total_weight,
    indicator_count,
    indicators,
    CASE 
        WHEN ABS(total_weight - 100.00) < 0.01 THEN 'VÁLIDO'
        ELSE 'INVÁLIDO - Soma: ' + CAST(total_weight AS VARCHAR(10))
    END as weight_validation
FROM AssignmentsSummary;
```

## 8. Performance e Otimização

### 8.1 Estratégias de Indexação
| Padrão de Query | Índice Recomendado | Justificativa |
|-----------------|-------------------|---------------|
| Busca por assessor + período | IX em (crm_id, valid_from) | Queries de cálculo individual |
| Atribuições vigentes | IX filtrado WHERE valid_to IS NULL | Maioria das queries |
| Por indicador | IX em (indicator_id, is_active) | Análise de distribuição |
| Validação de pesos | IX em (crm_id, valid_from) + INCLUDE weight | Performance validação |

### 8.2 Estatísticas e Manutenção
```sql
-- Atualizar estatísticas após carga batch
UPDATE STATISTICS silver.performance_assignments WITH FULLSCAN;

-- Verificar fragmentação dos índices
SELECT 
    i.name AS IndexName,
    ps.avg_fragmentation_in_percent
FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('silver.performance_assignments'), NULL, NULL, 'DETAILED') ps
INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
WHERE ps.avg_fragmentation_in_percent > 10;
```

## 9. Segurança e Privacidade

### 9.1 Classificação de Dados
| Campo | Classificação | Tratamento |
|-------|---------------|------------|
| crm_id | Interno | Identificador de funcionário |
| indicator_weight | Confidencial | Impacta remuneração |
| approved_by | Interno | Dados de gestão |

### 9.2 Políticas de Acesso
```sql
-- Roles de acesso
-- Leitura para gestores
GRANT SELECT ON silver.performance_assignments TO db_performance_reader;
GRANT SELECT ON silver.vw_performance_assignments_current TO db_performance_reader;

-- Escrita apenas para ETL
GRANT INSERT, UPDATE ON silver.performance_assignments TO db_etl_writer;

-- Deny DELETE para todos (apenas desativação)
DENY DELETE ON silver.performance_assignments TO PUBLIC;

-- Row Level Security para assessores verem apenas seus dados
CREATE SECURITY POLICY AssignmentPolicy
ADD FILTER PREDICATE dbo.fn_security_predicate(crm_id)
ON silver.performance_assignments
WITH (STATE = ON);
```

### 9.3 Mascaramento de Dados
```sql
-- View mascarada para ambientes não-produção
CREATE VIEW silver.vw_performance_assignments_masked
AS
SELECT 
    assignment_id,
    'AAI' + RIGHT('000' + CAST(assignment_id % 999 AS VARCHAR(3)), 3) as crm_id_masked,
    indicator_id,
    indicator_weight,
    valid_from,
    valid_to,
    is_active
FROM silver.performance_assignments;
```

## 10. Integração e Dependências

### 10.1 Sistemas Fonte
| Sistema | Tabelas/APIs | Frequência Sync | Método |
|---------|--------------|-----------------|---------|
| Google Sheets | m7_performance_assignments | Sob demanda | ETL-IND-002 |
| Bronze Layer | bronze.performance_assignments | Após carga | Stored Procedure |

### 10.2 Sistemas Consumidores
| Sistema | Uso | Requisitos |
|---------|-----|------------|
| Performance Calculations | Base para cálculos | Pesos válidos (soma = 100%) |
| Remuneração Variável | Input para cálculo RV | Dados aprovados |
| BI/Analytics | Análise de distribuição | Dados atualizados |
| APIs | Consulta de atribuições | Response < 100ms |

## 11. Evolução e Versionamento

### 11.1 Estratégia de Versionamento
- **Mudanças permitidas**: Novos pesos, vigências, aprovações
- **Histórico**: Mantido via valid_to (nunca deletar)
- **Compatibilidade**: crm_id + indicator_id + valid_from garantem unicidade

### 11.2 Processo de Mudança
1. **Proposta**: Atualização no Google Sheets com justificativa
2. **Validação**: ETL valida regras de negócio
3. **Aprovação**: Registro de approved_by e approved_date
4. **Ativação**: Nova vigência com valid_from
5. **Histórico**: Registro anterior recebe valid_to

## 12. Qualidade e Governança

### 12.1 Regras de Qualidade
| Dimensão | Métrica | Target | Medição |
|----------|---------|--------|---------|
| Completude | % assessores com atribuições | 100% | Diária |
| Validade | % com soma pesos = 100% | 100% | A cada carga |
| Integridade | % com indicator_id válido | 100% | FK constraint |
| Consistência | Sem sobreposição de vigências | 100% | Unique constraint |
| Atualidade | Última atualização | < 24h | Timestamp |

### 12.2 Data Lineage
```
Google Sheets → ETL-IND-002 → bronze.performance_assignments → prc_bronze_to_silver_assignments → silver.performance_assignments
                                                                                                              ↓
                                                                              ← gold.performance_calculations
                                                                              ← Remuneração Variável
                                                                              ← Rankings e Dashboards
```

## 13. Exemplos e Casos de Uso

### 13.1 Queries Comuns
```sql
-- Atribuições vigentes de um assessor
SELECT 
    a.assignment_id,
    i.indicator_code,
    i.indicator_name,
    i.category,
    a.indicator_weight,
    a.valid_from,
    a.approved_by,
    a.approved_date
FROM silver.performance_assignments a
INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
WHERE a.crm_id = 'AAI001'
  AND a.is_active = 1
  AND a.valid_to IS NULL
  AND GETDATE() BETWEEN a.valid_from AND ISNULL(a.valid_to, '9999-12-31')
ORDER BY i.category, a.indicator_weight DESC;

-- Validação de pesos por assessor
WITH WeightSummary AS (
    SELECT 
        a.crm_id,
        p.nome_pessoa,
        a.valid_from,
        SUM(CASE WHEN i.indicator_type = 'CARD' THEN a.indicator_weight ELSE 0 END) as total_weight_card,
        COUNT(CASE WHEN i.indicator_type = 'CARD' THEN 1 END) as count_card,
        COUNT(*) as total_indicators
    FROM silver.performance_assignments a
    INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
    LEFT JOIN silver.dim_pessoas p ON a.crm_id = p.crm_id
    WHERE a.is_active = 1 AND a.valid_to IS NULL
    GROUP BY a.crm_id, p.nome_pessoa, a.valid_from
)
SELECT *,
    CASE 
        WHEN ABS(total_weight_card - 100.00) < 0.01 THEN 'OK'
        ELSE 'ERRO - Peso: ' + CAST(total_weight_card AS VARCHAR(10)) + '%'
    END as validation_status
FROM WeightSummary
WHERE ABS(total_weight_card - 100.00) >= 0.01
ORDER BY crm_id;

-- Histórico de mudanças de um assessor
SELECT 
    a.valid_from,
    a.valid_to,
    i.indicator_code,
    a.indicator_weight,
    a.created_date,
    a.created_by,
    a.approved_date,
    a.approved_by,
    a.comments
FROM silver.performance_assignments a
INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
WHERE a.crm_id = 'AAI001'
ORDER BY a.valid_from DESC, i.indicator_code;
```

### 13.2 Padrões de Uso
- **Cálculo mensal**: JOIN com targets e resultados para performance
- **Remuneração**: Pesos aplicados sobre atingimento de metas
- **Rankings**: Comparação entre assessores com mesmos indicadores
- **Auditoria**: Rastreamento de mudanças via campos de controle

## 14. Troubleshooting

### 14.1 Problemas Comuns
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| Peso não soma 100% | Erro no cálculo | Query WeightSummary | Ajustar pesos no Google Sheets |
| Indicador não encontrado | FK violation | Verificar indicator_code | Executar ETL-IND-001 primeiro |
| Duplicata na inserção | UNIQUE violation | Check valid_from | Verificar vigências sobrepostas |
| Performance lenta | Timeout em queries | Estatísticas desatualizadas | UPDATE STATISTICS |

## 15. Referências e Anexos

### 15.1 Scripts DDL Completos
- [QRY-ASS-002-create_silver_performance_assignments.sql](../../operacional/queries/silver/QRY-ASS-002-create_silver_performance_assignments.sql)
- [QRY-ASS-003-prc_bronze_to_silver_assignments.sql](../../operacional/queries/bronze/QRY-ASS-003-prc_bronze_to_silver_assignments.sql)

### 15.2 Documentação Relacionada
- [ETL-IND-002 - Processo de Extração de Atribuições](../processos-etl/ETL-IND-002-extracao-atribuicoes-performance.md)
- [MOD-IND-002 - Modelo Performance Indicators Silver](MOD-IND-002-performance-indicators-silver.md)
- [POL-GOV-001 - Política de Gestão de Performance](../../../business-documentation/estrategico/politicas/POL-GOV-001-performance-desempenho.md)
- [MAN-GES-001 - Manual de Gestão de Performance](../../../business-documentation/estrategico/manuais/performance-desempenho/MAN-GES-001-manual-gestao-performance.md)

### 15.3 Ferramentas e Recursos
- SQL Server Management Studio para administração
- Google Sheets para manutenção de atribuições
- Power BI para visualização de distribuição de pesos

---

**Documento criado por**: Bruno Chiaramonti  
**Data**: 2025-01-18  
**Versão**: 1.0.0