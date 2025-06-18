---
título: Modelo de Validação de Performance Gold - Qualidade e Integridade
tipo: MOD
código: MOD-IND-007
versão: 1.0.0
data_criação: 2025-01-18
última_atualização: 2025-01-18
próxima_revisão: 2025-07-18
responsável: bruno.chiaramonti@multisete.com
aprovador: arquitetura.dados@m7investimentos.com.br
tags: [modelo, gold, validação, qualidade, procedure, monitoramento]
status: aprovado
confidencialidade: interno
---

# MOD-IND-007 - Validação e Qualidade Performance Gold

## 1. Objetivo

Documentar o processo de validação e garantia de qualidade implementado pela procedure `gold.prc_validate_processing` e a tabela `gold.processing_log`. Este modelo estabelece controles críticos para assegurar a integridade, completude e acurácia dos dados de performance calculados na camada Gold, incluindo mecanismos de auto-correção e monitoramento contínuo.

## 2. Escopo e Contexto

### 2.1 Domínio de Negócio
- **Área**: Governança de Dados / Qualidade / Compliance
- **Processos suportados**: 
  - Validação pós-processamento de performance
  - Auditoria de cálculos e metas
  - Garantia de qualidade para remuneração
  - Troubleshooting e correções
  - Compliance e rastreabilidade
- **Stakeholders**: 
  - Governança de Dados
  - Controladoria (accuracy crítica)
  - TI/Analytics (operação)
  - Auditoria Interna
  - Gestão de Performance

### 2.2 Contexto Técnico
- **Tipo**: Stored Procedure + Logging
- **Criticidade**: Alta (impacta remuneração)
- **Frequência**: Após cada processamento
- **SLA**: Validação em < 2 minutos
- **Automação**: Integrado no pipeline

## 3. Arquitetura de Validação

### 3.1 Fluxo de Validação
```
┌─────────────────────────┐
│  Processamento Gold     │
│  (prc_process_to_gold)  │
└───────────┬─────────────┘
            │ Trigger automático
            ▼
┌─────────────────────────┐
│  Validação Automática   │
│  (prc_validate)         │
├─────────────────────────┤
│ • 8 validações core     │
│ • 3 níveis: FULL/BASIC │
│ • Auto-fix opcional     │
└───────────┬─────────────┘
            │
    ┌───────┴────────┐
    │                │
    ▼                ▼
┌──────────┐    ┌─────────────┐
│ SUCESSO  │    │   FALHA     │
│ Liberar  │    │ • Notificar │
│ para BI  │    │ • Corrigir  │
│          │    │ • Reproces. │
└──────────┘    └─────────────┘
```

### 3.2 Níveis de Severidade
| Severidade | Descrição | Ação Requerida | Impacto |
|------------|-----------|----------------|---------|
| ERROR | Problema crítico | Correção imediata | Bloqueia consumo |
| WARNING | Inconsistência | Investigar | Permite consumo |
| INFO | Informativo | Monitorar | Sem impacto |

## 4. Validações Implementadas

### 4.1 Matriz de Validações

| ID | Validação | Tipo | Severidade | Auto-Fix | Descrição |
|----|-----------|------|------------|----------|-----------|
| V01 | COMPLETUDE_ASSESSOR | FULL, BASIC | ERROR | Não | Todos assessores ativos processados |
| V02 | SOMA_PESOS_CARD | FULL, WEIGHTS | ERROR | Sim | Pesos CARD somam 100% |
| V03 | CARD_SEM_VALOR | FULL, BASIC | WARNING | Não | Indicadores CARD com valor NULL |
| V04 | TARGET_AUSENTE | FULL | ERROR | Não | Indicadores sem meta definida |
| V05 | ACHIEVEMENT_ANOMALO | FULL | WARNING/ERROR | Não | Achievement fora do range |
| V06 | FORMULA_ERRO | FULL, BASIC | ERROR | Não | Fórmulas que falharam |
| V07 | WEIGHTED_INCORRETO | FULL | WARNING | Sim | Cálculo weighted incorreto |
| V08 | DUPLICATA | FULL, BASIC | ERROR | Não | Registros duplicados |

### 4.2 Detalhamento das Validações

#### V01 - Completude de Assessores
**Objetivo**: Garantir que todos assessores com indicadores ativos foram processados.

**Query de Validação**:
```sql
-- Assessores esperados
SELECT DISTINCT codigo_assessor_crm
FROM silver.performance_assignments
WHERE is_active = 1
  AND @period_start >= valid_from

-- MINUS assessores processados
SELECT DISTINCT entity_id
FROM gold.card_metas
WHERE period_start = @period_start
```

**Critério de Falha**: Qualquer assessor faltando = ERROR

#### V02 - Validação de Pesos CARD
**Objetivo**: Garantir que indicadores tipo CARD somam exatamente 100%.

**Regra**: |SUM(indicator_weight) - 100| < 0.01

**Auto-Correção**:
```sql
-- Normalizar pesos proporcionalmente
new_weight = (current_weight / sum_weights) * 100
```

**Impacto**: Crítico para cálculo de score

#### V03 - Indicadores CARD sem Valor
**Objetivo**: Identificar indicadores principais sem cálculo.

**Critérios**:
- indicator_type = 'CARD'
- realized_value IS NULL
- has_error = 0

**Ação**: Investigar fórmula ou dados fonte

#### V04 - Targets Ausentes
**Objetivo**: Garantir que todos indicadores têm meta.

**Exceções**: KPI/PPI podem não ter meta

**Impacto**: Impossível calcular achievement

#### V05 - Achievement Anômalo
**Objetivo**: Detectar cálculos fora do esperado.

**Thresholds**:
- ERROR: > 500% ou < -100%
- WARNING: > 300% ou < -50%

**Causas Comuns**:
- Divisão por target muito pequeno
- Erro na fórmula
- Dados incorretos na fonte

#### V06 - Fórmulas com Erro
**Objetivo**: Identificar falhas na execução SQL.

**Diagnóstico**: Campo processing_notes contém erro

**Ações**:
1. Revisar sintaxe da fórmula
2. Verificar permissões
3. Validar objetos referenciados

#### V07 - Weighted Achievement Incorreto
**Objetivo**: Validar cálculo matemático.

**Fórmula Esperada**:
```
weighted_achievement = (achievement_percentage * indicator_weight) / 100
```

**Tolerância**: 0.01 (arredondamento)

**Auto-Fix**: Recalcular e atualizar

#### V08 - Duplicatas
**Objetivo**: Garantir unicidade.

**Chave**: (entity_id, attribute_code, period_start)

**Causa**: Erro no processamento ou reprocessamento parcial

## 5. Estrutura de Dados

### 5.1 Tabela Temporária: #validation_results

| Campo | Tipo | Descrição |
|-------|------|-----------|
| validation_id | INT IDENTITY | ID sequencial |
| validation_type | VARCHAR(50) | Código da validação |
| severity | VARCHAR(20) | ERROR/WARNING/INFO |
| entity_id | VARCHAR(20) | Assessor afetado |
| indicator_code | VARCHAR(50) | Indicador afetado |
| issue_description | VARCHAR(1000) | Descrição do problema |
| details | VARCHAR(MAX) | Detalhes técnicos |
| records_affected | INT | Quantidade afetada |
| can_be_fixed | BIT | Suporta auto-fix |
| fixed | BIT | Foi corrigido |

### 5.2 Processing Log - Campos de Validação

| Campo | Descrição | Exemplo |
|-------|-----------|---------|
| processing_type | 'VALIDATION_FULL' | Tipo executado |
| status | Resultado geral | 'ERROR', 'WARNING', 'SUCCESS' |
| execution_notes | Sumário | "Erros: 5, Avisos: 10, Corrigidos: 2" |

## 6. Processo de Validação

### 6.1 Fluxo de Execução

```sql
-- 1. Determinar período (último se não informado)
IF @period_start IS NULL
    SELECT TOP 1 @period_start = period_start FROM gold.card_metas

-- 2. Criar estrutura temporária
CREATE TABLE #validation_results (...)

-- 3. Executar validações conforme tipo
IF @validation_type IN ('FULL', 'BASIC')
    EXEC validation_01_completude
    EXEC validation_03_card_sem_valor
    ...

IF @validation_type IN ('FULL', 'WEIGHTS')
    EXEC validation_02_pesos

-- 4. Aplicar correções se @fix_issues = 1
IF @fix_issues = 1
    EXEC apply_fixes

-- 5. Gerar sumário e log
INSERT INTO processing_log ...

-- 6. Retornar status
RETURN 0 -- Sucesso
RETURN 1 -- Falha com erros
```

### 6.2 Modos de Execução

| Modo | Validações | Tempo | Uso |
|------|------------|-------|-----|
| BASIC | V01,V03,V06,V08 | ~30s | Verificação rápida |
| WEIGHTS | V02 apenas | ~10s | Foco em pesos |
| FULL | Todas (V01-V08) | ~2min | Validação completa |

## 7. Auto-Correção

### 7.1 Correções Implementadas

| Problema | Correção Automática | SQL |
|----------|---------------------|-----|
| Pesos != 100% | Normalização proporcional | `weight = (weight/sum)*100` |
| Weighted incorreto | Recálculo matemático | `weighted = (ach*weight)/100` |

### 7.2 Processo de Correção

```sql
-- Exemplo: Normalizar pesos
UPDATE gold.card_metas
SET 
    indicator_weight = (indicator_weight / @sum_weights) * 100,
    modified_date = GETDATE(),
    modified_by = SYSTEM_USER + '_VALIDATION_FIX',
    processing_notes = 'Peso ajustado de ' + CAST(@old_weight AS VARCHAR)
WHERE entity_id IN (SELECT entity_id FROM #validation_results WHERE can_be_fixed = 1)
```

### 7.3 Auditoria de Correções

- Todas correções são logadas
- Campo modified_by identifica correção automática
- Processing_notes detalha mudança
- Validação marca fixed = 1

## 8. Integração e Automação

### 8.1 Pipeline de Processamento

```sql
-- SQL Agent Job: Process_Gold_Performance
-- Step 1: Processar dados
EXEC gold.prc_process_performance_to_gold

-- Step 2: Validar (automático)
DECLARE @validation_result INT
EXEC @validation_result = gold.prc_validate_processing 
    @validation_type = 'FULL',
    @fix_issues = 1

-- Step 3: Decisão baseada em resultado
IF @validation_result = 0
    -- Prosseguir para BI refresh
ELSE
    -- Notificar equipe e bloquear
```

### 8.2 Notificações

| Evento | Canal | Destinatários | Template |
|--------|-------|---------------|----------|
| Erro crítico | Email | data-governance@ | "Validação Gold falhou: X erros" |
| Warnings > 10 | Teams | analytics-team | "Atenção: Y warnings detectados" |
| Sucesso | Log | - | Registro apenas |

## 9. Monitoramento e KPIs

### 9.1 Métricas de Qualidade

| Métrica | Fórmula | Meta | Frequência |
|---------|---------|------|------------|
| Taxa de Sucesso | Validações OK / Total | > 95% | Diária |
| Erros por Execução | AVG(error_count) | < 5 | Semanal |
| Tempo de Validação | AVG(duration_seconds) | < 120s | Mensal |
| Taxa Auto-Fix | Fixed / Can_be_fixed | > 90% | Mensal |

### 9.2 Dashboard de Monitoramento

```sql
-- View para monitoramento
CREATE VIEW gold.vw_validation_monitoring AS
SELECT 
    CAST(start_time AS DATE) as validation_date,
    COUNT(*) as total_validations,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as failed,
    AVG(duration_seconds) as avg_duration_sec,
    MAX(duration_seconds) as max_duration_sec
FROM gold.processing_log
WHERE processing_type LIKE 'VALIDATION%'
GROUP BY CAST(start_time AS DATE)
```

## 10. Troubleshooting

### 10.1 Problemas Comuns

| Sintoma | Causa Provável | Diagnóstico | Solução |
|---------|----------------|-------------|---------|
| Validação timeout | Volume grande | Check duration | Aumentar batch size |
| Fix não funciona | Constraint | Check error msg | Correção manual |
| Falsos positivos | Threshold baixo | Revisar regra | Ajustar validação |
| Loop infinito | Fix cria novo problema | Monitor iterations | Limite tentativas |

### 10.2 Queries de Diagnóstico

```sql
-- Histórico de problemas recorrentes
WITH RecurringIssues AS (
    SELECT 
        validation_type,
        entity_id,
        indicator_code,
        COUNT(*) as occurrence_count,
        MAX(validation_date) as last_occurrence
    FROM gold.processing_log_details -- Vista detalhada
    WHERE severity = 'ERROR'
      AND validation_date >= DATEADD(MONTH, -3, GETDATE())
    GROUP BY validation_type, entity_id, indicator_code
    HAVING COUNT(*) >= 3
)
SELECT * FROM RecurringIssues
ORDER BY occurrence_count DESC;

-- Performance das validações
SELECT 
    processing_type,
    AVG(duration_seconds) as avg_duration,
    MIN(duration_seconds) as min_duration,
    MAX(duration_seconds) as max_duration,
    COUNT(*) as execution_count
FROM gold.processing_log
WHERE processing_type LIKE 'VALIDATION%'
  AND start_time >= DATEADD(DAY, -30, GETDATE())
GROUP BY processing_type;
```

## 11. Melhores Práticas

### 11.1 Operacionais

1. **Sempre validar após processamento**
   - Integrar no pipeline
   - Não permitir skip

2. **Usar modo apropriado**
   - BASIC para verificações rápidas
   - FULL para fechamento mensal

3. **Revisar logs regularmente**
   - Identificar padrões
   - Ajustar thresholds

4. **Documentar exceções**
   - Justificar overrides
   - Manter audit trail

### 11.2 Governança

1. **Aprovar mudanças em validações**
   - Comitê de governança
   - Teste em DEV primeiro

2. **Manter histórico**
   - Não deletar logs
   - Arquivar após 1 ano

3. **Treinar equipe**
   - Interpretar resultados
   - Ações corretivas

## 12. Evolução e Roadmap

### 12.1 Melhorias Planejadas

| Feature | Descrição | Prazo |
|---------|-----------|-------|
| ML Anomaly Detection | Detectar outliers automaticamente | Q2 2025 |
| Real-time Validation | Validar durante processamento | Q3 2025 |
| Self-healing | Correções mais inteligentes | Q4 2025 |
| API de Validação | Expor via REST API | Q1 2026 |

### 12.2 Novas Validações Propostas

- Consistência temporal (MoM variations)
- Validação cruzada com sistemas fonte
- Detecção de Gaming (manipulação)
- Benchmarking automático

## 13. Exemplos de Uso

### 13.1 Validação Pós-Processamento
```sql
-- Executar validação completa com correções
EXEC gold.prc_validate_processing 
    @validation_type = 'FULL',
    @fix_issues = 1,
    @debug = 1;
```

### 13.2 Validação Específica
```sql
-- Validar apenas pesos de um período
EXEC gold.prc_validate_processing 
    @period_start = '2025-01-01',
    @validation_type = 'WEIGHTS',
    @fix_issues = 1;
```

### 13.3 Integração em Pipeline
```sql
-- Verificar resultado para decisão
DECLARE @result INT;
EXEC @result = gold.prc_validate_processing;

IF @result = 0
    PRINT 'Prosseguir com refresh BI'
ELSE
BEGIN
    -- Buscar detalhes dos erros
    SELECT * FROM gold.processing_log
    WHERE log_id = SCOPE_IDENTITY();
    
    -- Notificar equipe
    EXEC msdb.dbo.sp_send_dbmail 
        @recipients = 'data-governance@m7.com',
        @subject = 'Validação Gold Falhou',
        @body = 'Verificar erros no processing_log';
END
```

## 14. Referências

### 14.1 Scripts SQL
- [QRY-IND-008-prc_validate_processing.sql](../../operacional/queries/gold/QRY-IND-008-prc_validate_processing.sql)

### 14.2 Documentação Relacionada
- [MOD-IND-005 - Card Metas Gold](MOD-IND-005-card-metas-gold.md)
- [POL-GOV-001 - Política de Governança](../../../business-documentation/estrategico/politicas/POL-GOV-001-performance-desempenho.md)

### 14.3 Padrões de Mercado
- ISO 8000 - Data Quality
- DAMA-DMBOK - Data Quality Management
- Six Sigma - Statistical Process Control

---

**Documento criado por**: Bruno Chiaramonti  
**Data**: 2025-01-18  
**Versão**: 1.0.0  
**Status**: Aprovado