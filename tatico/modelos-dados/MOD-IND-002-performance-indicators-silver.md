# MOD-IND-002-performance-indicators-silver

---
título: Modelo de Dados Performance Indicators - Camada Silver
tipo: MOD - Modelo de Dados
versão: 1.0.0
última_atualização: 2025-01-18
autor: bruno.chiaramonti@multisete.com
aprovador: arquitetura.dados@m7investimentos.com.br
tags: [modelo, dados, performance, indicadores, silver, metadados]
status: aprovado
dependências:
  - tipo: processo
    ref: [ETL-IND-001]
    repo: datawarehouse-docs
  - tipo: query
    ref: [QRY-IND-003]
    repo: datawarehouse-docs
---

## 1. Objetivo

Definir o modelo de dados da tabela `silver.performance_indicators` que armazena a configuração validada e processada dos indicadores de performance. Esta tabela serve como fonte autoritativa para todos os cálculos de performance no Data Warehouse, garantindo consistência e versionamento dos indicadores utilizados no sistema de gestão de performance.

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: Gestão de Performance / Comercial
- **Processos suportados**: 
  - Definição e manutenção de KPIs
  - Atribuição de indicadores a assessores
  - Cálculo de performance mensal
  - Ranking e comparações
- **Stakeholders**: 
  - Gestão de Performance
  - Diretoria Comercial
  - Assessores de Investimento
  - TI/Analytics

### 2.2 Contexto Técnico
- **Tipo de modelo**: Dimensional (Dimensão de metadados)
- **Plataforma**: SQL Server 2019+
- **Schema**: silver
- **Layer**: Silver (Dados validados e conformados)
- **Padrão**: Slowly Changing Dimension Type 2 (com versionamento)

## 3. Visão Geral do Modelo

### 3.1 Diagrama Entidade-Relacionamento
```
┌─────────────────────────┐
│ bronze.performance_     │
│      indicators         │
├─────────────────────────┤
│ Dados brutos do Google  │
│ Sheets (staging)        │
└───────────┬─────────────┘
            │ prc_process_indicators_to_silver
            ▼
┌─────────────────────────┐         ┌─────────────────────────┐
│  silver.performance_    │         │ silver.performance_     │
│      indicators         │◄────────┤     assignments         │
├─────────────────────────┤ 1:N     ├─────────────────────────┤
│ PK: indicator_id        │         │ FK: indicator_id        │
│ UK: indicator_code      │         │ Pesos por assessor      │
│ Metadados validados     │         └─────────────────────────┘
└───────────┬─────────────┘                     │
            │                                   │
            ▼                                   ▼
┌─────────────────────────┐         ┌─────────────────────────┐
│  silver.performance_    │         │   gold.performance_     │
│       targets           │         │      calculations       │
├─────────────────────────┤         ├─────────────────────────┤
│ FK: indicator_code      │         │ Resultados calculados   │
│ Metas por período       │         │ usando os indicadores   │
└─────────────────────────┘         └─────────────────────────┘
```

### 3.2 Principais Entidades
| Entidade | Tipo | Descrição | Volume Estimado |
|----------|------|-----------|-----------------|
| performance_indicators | Dimensão SCD2 | Configuração de indicadores | 10-50 registros ativos |
| performance_assignments | Fato | Atribuições indicador-assessor | 500-1000 registros/mês |
| performance_targets | Fato | Metas por período | 100-500 registros/mês |

## 4. Dicionário de Dados Detalhado

### 4.1 Tabela: silver.performance_indicators

**Descrição**: Tabela de metadados contendo a configuração oficial e validada dos indicadores de performance. Fonte autoritativa para todos os cálculos de performance no DW.

| Campo | Tipo | Constraint | Descrição | Exemplo | Regras de Negócio |
|-------|------|------------|-----------|---------|-------------------|
| indicator_id | INT | PK, IDENTITY(1,1), NOT NULL | ID único do indicador | 1 | Auto-incremento |
| indicator_code | VARCHAR(50) | UK, NOT NULL | Código único do indicador | "CAPTACAO_LIQUIDA" | UPPER_CASE, sem espaços |
| indicator_name | VARCHAR(200) | NOT NULL | Nome descritivo | "Captação Líquida" | Mínimo 3 caracteres |
| category | VARCHAR(50) | NOT NULL, CHECK | Categoria do indicador | "FINANCEIRO" | Valores permitidos definidos |
| unit | VARCHAR(20) | NOT NULL, CHECK | Unidade de medida | "R$" | Valores: R$, %, QTD, etc. |
| aggregation_method | VARCHAR(20) | NOT NULL, DEFAULT 'SUM' | Método de agregação | "SUM" | SUM, AVG, COUNT, MAX, MIN, LAST, CUSTOM |
| calculation_formula | VARCHAR(MAX) | NULL | Fórmula SQL validada | "captacao_bruta - resgates" | Deve referenciar tabelas do DW |
| is_inverted | BIT | NOT NULL, DEFAULT 0 | Indicador invertido | 0 | 1 = menor é melhor |
| is_active | BIT | NOT NULL, DEFAULT 1 | Status ativo | 1 | 0 = desativado (histórico) |
| description | VARCHAR(2000) | NULL | Descrição completa | "Diferença entre captação..." | Texto livre |
| business_rules | VARCHAR(MAX) | NULL | Regras de negócio | "Considerar apenas..." | JSON ou texto estruturado |
| notes | VARCHAR(MAX) | NULL | Observações gerais | "Atualizado em jan/2025" | Texto livre |
| version | INT | NOT NULL, DEFAULT 1 | Versão do registro | 1 | Incrementa a cada mudança |
| valid_from | DATETIME | NOT NULL, DEFAULT GETDATE() | Início da validade | "2025-01-01 00:00:00" | Data de criação/alteração |
| valid_to | DATETIME | NULL | Fim da validade | NULL | NULL = registro atual |
| created_date | DATETIME | NOT NULL, DEFAULT GETDATE() | Data de criação | "2025-01-01 10:30:00" | Automático |
| created_by | VARCHAR(100) | NOT NULL, DEFAULT SYSTEM_USER | Usuário criador | "ETL_BRONZE_TO_SILVER" | User ou processo |
| modified_date | DATETIME | NULL | Data de modificação | "2025-01-15 14:20:00" | Atualizado em UPDATE |
| modified_by | VARCHAR(100) | NULL | Usuário modificador | "bruno.chiaramonti" | User ou processo |

**Índices**:
| Nome | Tipo | Campos | Propósito |
|------|------|--------|-----------|
| PK_silver_performance_indicators | CLUSTERED | indicator_id | Chave primária |
| UQ_silver_performance_indicators_code | UNIQUE NONCLUSTERED | indicator_code | Garantir código único |
| IX_silver_performance_indicators_category | NONCLUSTERED | category, is_active | Busca por categoria ativa |
| IX_silver_performance_indicators_active | NONCLUSTERED | is_active, indicator_code, indicator_name | Queries de indicadores ativos |
| IX_silver_performance_indicators_temporal | NONCLUSTERED | valid_from, valid_to, indicator_code | Versionamento temporal |

**Particionamento**: Não aplicável (volume pequeno)

**Constraints adicionais**:
```sql
-- CHECK constraint para categoria
CONSTRAINT [CK_silver_performance_indicators_category] CHECK (
    [category] IN ('FINANCEIRO', 'QUALIDADE', 'VOLUME', 'COMPORTAMENTAL', 'PROCESSO', 'GATILHO')
)

-- CHECK constraint para unidade
CONSTRAINT [CK_silver_performance_indicators_unit] CHECK (
    [unit] IN ('R$', '%', 'QTD', 'SCORE', 'HORAS', 'DIAS', 'RATIO')
)

-- CHECK constraint para método de agregação
CONSTRAINT [CK_silver_performance_indicators_aggregation] CHECK (
    [aggregation_method] IN ('SUM', 'AVG', 'COUNT', 'MAX', 'MIN', 'LAST', 'CUSTOM')
)
```

## 5. Relacionamentos e Integridade

### 5.1 Chaves Estrangeiras
| Tabela Origem | Campo(s) | Tabela Destino | Campo(s) | Tipo | On Delete | On Update |
|---------------|----------|----------------|----------|------|-----------|-----------|
| performance_assignments | indicator_id | performance_indicators | indicator_id | N:1 | RESTRICT | CASCADE |
| performance_targets | indicator_code | - | - | - | - | - |

*Nota: performance_targets usa indicator_code ao invés de indicator_id para flexibilidade*

### 5.2 Cardinalidade dos Relacionamentos
- **indicators → assignments**: 1:N (Um indicador pode ter várias atribuições)
- **indicators → targets**: 1:N (Um indicador pode ter várias metas por período)
- **indicators → calculations**: 1:N (Um indicador gera vários cálculos)

## 6. Regras de Negócio e Validações

### 6.1 Regras de Integridade
| Regra | Implementação | Descrição |
|-------|---------------|-----------|
| RN001 | CHECK constraint | Categoria deve estar na lista permitida |
| RN002 | CHECK constraint | Unidade deve estar na lista permitida |
| RN003 | UNIQUE constraint | indicator_code deve ser único |
| RN004 | Procedure validation | Fórmula SQL deve ser sintaticamente válida |
| RN005 | Trigger | version incrementa automaticamente em UPDATE |
| RN006 | Business logic | Indicadores CARD devem ter peso total = 100% por assessor |

### 6.2 Implementação das Regras
```sql
-- RN004: Validação de fórmula SQL (na procedure)
BEGIN TRY
    EXEC sp_executesql N'SELECT TOP 1 ' + @calculation_formula + ' FROM sys.tables WHERE 1=0'
    -- Se não der erro, fórmula é sintaticamente válida
END TRY
BEGIN CATCH
    -- Registrar como inválida mas aceitar (validação completa posterior)
    SET @notes = 'Fórmula precisa revisão: ' + ERROR_MESSAGE()
END CATCH

-- RN005: Trigger para incrementar versão
CREATE TRIGGER trg_silver_indicators_version
ON silver.performance_indicators
AFTER UPDATE
AS
BEGIN
    UPDATE silver.performance_indicators
    SET version = version + 1,
        modified_date = GETDATE(),
        modified_by = SYSTEM_USER
    WHERE indicator_id IN (SELECT indicator_id FROM inserted)
END
```

## 7. Histórico e Auditoria

### 7.1 Estratégia de Historização
- **Tipo**: SCD Type 2 (Slowly Changing Dimension)
- **Campos de controle**:
  - `version`: Número da versão (incrementa a cada mudança)
  - `valid_from`: Início da validade do registro
  - `valid_to`: Fim da validade (NULL para registro atual)
  - `is_active`: Flag indicando se está ativo (independente de versionamento)
  
### 7.2 View para Registro Atual
```sql
CREATE VIEW silver.vw_active_indicators
AS
SELECT 
    indicator_id,
    indicator_code,
    indicator_name,
    category,
    unit,
    aggregation_method,
    calculation_formula,
    is_inverted,
    description
FROM silver.performance_indicators
WHERE is_active = 1
  AND valid_to IS NULL;
```

## 8. Performance e Otimização

### 8.1 Estratégias de Indexação
| Padrão de Query | Índice Recomendado | Justificativa |
|-----------------|-------------------|---------------|
| Busca por código | IX em indicator_code | Queries JOIN frequentes |
| Filtro por categoria | IX em (category, is_active) | Dashboards por tipo |
| Indicadores ativos | IX filtrado WHERE is_active=1 | Maioria das queries |
| Análise temporal | IX em (valid_from, valid_to) | Queries históricas |

### 8.2 Estatísticas e Manutenção
```sql
-- Atualizar estatísticas após mudanças significativas
UPDATE STATISTICS silver.performance_indicators WITH FULLSCAN;

-- Compressão de página para economia
ALTER TABLE silver.performance_indicators
REBUILD WITH (DATA_COMPRESSION = PAGE);
```

## 9. Segurança e Privacidade

### 9.1 Classificação de Dados
| Campo | Classificação | Tratamento |
|-------|---------------|------------|
| Todos | Interno | Acesso controlado por role |
| calculation_formula | Confidencial | Pode conter lógica de negócio |
| business_rules | Confidencial | Regras estratégicas |

### 9.2 Políticas de Acesso
```sql
-- Roles de acesso
-- Leitura para analistas
GRANT SELECT ON silver.performance_indicators TO db_datareader;
GRANT SELECT ON silver.vw_active_indicators TO db_datareader;

-- Escrita apenas para ETL
GRANT INSERT, UPDATE ON silver.performance_indicators TO db_etl_writer;

-- Deny DELETE para todos (soft delete apenas)
DENY DELETE ON silver.performance_indicators TO PUBLIC;
```

### 9.3 Mascaramento de Dados
Não aplicável - tabela contém apenas metadados, sem dados sensíveis

## 10. Integração e Dependências

### 10.1 Sistemas Fonte
| Sistema | Tabelas/APIs | Frequência Sync | Método |
|---------|--------------|-----------------|---------|
| Google Sheets | m7_performance_indicators | Sob demanda | ETL-IND-001 |
| Bronze Layer | bronze.performance_indicators | Após carga | Stored Procedure |

### 10.2 Sistemas Consumidores
| Sistema | Uso | Requisitos |
|---------|-----|------------|
| Performance Assignments | JOIN para validar indicadores | Integridade referencial |
| Performance Targets | Lookup de metadados | Código deve existir |
| Gold Calculations | Executar fórmulas | Fórmulas válidas |
| BI/Analytics | Dimensão em relatórios | Dados atualizados |

## 11. Evolução e Versionamento

### 11.1 Estratégia de Versionamento
- **Mudanças permitidas**: Todos os campos exceto indicator_id e indicator_code
- **Versionamento**: Automático via trigger (version + 1)
- **Histórico**: Mantido indefinidamente (valid_to preenchido)
- **Compatibilidade**: indicator_code imutável garante retrocompatibilidade

### 11.2 Processo de Mudança
1. **Proposta**: Atualização no Google Sheets
2. **Validação**: ETL valida mudanças
3. **Versionamento**: Procedure cria nova versão se houver mudança
4. **Notificação**: Log de mudanças em processing_notes
5. **Ativação**: Nova versão ativa imediatamente

## 12. Qualidade e Governança

### 12.1 Regras de Qualidade
| Dimensão | Métrica | Target | Medição |
|----------|---------|--------|---------|
| Completude | % campos obrigatórios preenchidos | 100% | A cada carga |
| Unicidade | indicator_code único | 100% | Constraint |
| Validade | Categorias/unidades válidas | 100% | Check constraint |
| Consistência | Fórmulas sintaticamente corretas | > 95% | Validação ETL |
| Atualidade | Sincronização com Google Sheets | < 24h | Manual |

### 12.2 Data Lineage
```
Google Sheets → ETL-IND-001 → bronze.performance_indicators → prc_process_indicators_to_silver → silver.performance_indicators
                                                                                                              ↓
                                                                              ← silver.performance_assignments
                                                                              ← silver.performance_targets
                                                                              ← gold.performance_calculations
```

## 13. Exemplos e Casos de Uso

### 13.1 Queries Comuns
```sql
-- Buscar indicadores ativos por categoria
SELECT 
    indicator_code,
    indicator_name,
    unit,
    calculation_formula
FROM silver.vw_active_indicators
WHERE category = 'FINANCEIRO'
ORDER BY indicator_name;

-- Histórico de mudanças de um indicador
SELECT 
    version,
    valid_from,
    valid_to,
    modified_by,
    indicator_name,
    calculation_formula
FROM silver.performance_indicators
WHERE indicator_code = 'CAPTACAO_LIQUIDA'
ORDER BY version DESC;

-- Indicadores com fórmulas customizadas
SELECT 
    indicator_code,
    indicator_name,
    calculation_formula
FROM silver.performance_indicators
WHERE aggregation_method = 'CUSTOM'
  AND calculation_formula IS NOT NULL
  AND is_active = 1
  AND valid_to IS NULL;
```

### 13.2 Padrões de Uso
- **Configuração**: Definir novos indicadores via Google Sheets
- **Cálculo**: Gold layer executa calculation_formula
- **Análise**: Joins com assignments e targets para performance
- **Auditoria**: Consultar versões anteriores para compliance

## 14. Troubleshooting

### 14.1 Problemas Comuns
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| Indicador não aparece | JOIN retorna NULL | Verificar is_active e valid_to | Ativar ou usar versão correta |
| Fórmula com erro | Cálculo falha | Testar fórmula isoladamente | Corrigir no Sheets e recarregar |
| Código duplicado | Insert falha | Violação de UNIQUE | Verificar e corrigir no fonte |
| Categoria inválida | Insert falha | CHECK constraint | Atualizar lista permitida |

## 15. Referências e Anexos

### 15.1 Scripts DDL Completos
- [QRY-IND-002-create_silver_performance_indicators.sql]
- [QRY-IND-003-prc_bronze_to_silver_indicators.sql]

### 15.2 Documentação Relacionada
- [ETL-IND-001 - Processo de Extração de Indicadores]
- [POL-GOV-001 - Política de Governança de Performance]
- [MAN-GES-001 - Manual de Gestão de Performance]

### 15.3 Ferramentas e Recursos
- SQL Server Management Studio para administração
- Google Sheets para manutenção de metadados
- Power BI para visualização de indicadores

---

**Documento criado por**: Bruno Chiaramonti  
**Data**: 2025-01-18  
**Versão**: 1.0.0