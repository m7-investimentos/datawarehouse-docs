---
título: Extração de Metas de Performance - Google Sheets para Bronze
tipo: ETL
código: ETL-IND-003
versão: 2.0.0
data_criação: 2025-01-17
última_atualização: 2025-01-18
próxima_revisão: 2025-04-18
responsável: bruno.chiaramonti@multisete.com
aprovador: diretoria.ti@m7investimentos.com.br
tags: [etl, performance, targets, metas, google-sheets, bronze, silver]
status: aprovado
confidencialidade: interno
---

# ETL-IND-003 - Extração de Metas de Performance

## 1. Objetivo

Extrair dados de metas mensais de performance por assessor e indicador da planilha Google Sheets `m7_performance_targets` para a camada Bronze do Data Warehouse, incluindo validações de integridade de valores e relacionamentos temporais.

### Dependências
- **Modelo de dados**: [MOD-IND-004 - Performance Targets Silver](../modelos-dados/MOD-IND-004-performance-targets-silver.md)
- **Procedure**: [QRY-TAR-003 - Bronze to Silver Targets](../../operacional/queries/bronze/QRY-TAR-003-prc_bronze_to_silver_performance_targets.sql)
- **ETLs**: 
  - [ETL-IND-001 - Extração de Indicadores](ETL-IND-001-extracao-indicadores-performance.md)
  - [ETL-IND-002 - Extração de Atribuições](ETL-IND-002-extracao-atribuicoes-performance.md)

## 2. Escopo e Aplicabilidade

### 2.1 Escopo
- **Fonte de dados**: Google Sheets - m7_performance_targets
- **Destino**: M7Medallion.bronze.performance_targets
- **Volume esperado**: ~2500-3000 registros (12 meses × assessores × indicadores)
- **Frequência**: Mensal ou sob demanda (planejamento anual com ajustes)

### 2.2 Fora de Escopo
- Cálculo de metas intermediárias
- Projeções ou extrapolações
- Ajustes sazonais automáticos

## 3. Pré-requisitos e Dependências

### 3.1 Técnicos
- **Conectividade**: 
  - Google Sheets API v4 habilitada
  - Service Account com permissões de leitura
  - Credenciais JSON armazenadas seguramente
- **Recursos computacionais**: Mínimos (< 10MB de dados)
- **Software/Ferramentas**: 
  - Python 3.8+
  - google-api-python-client
  - pandas
  - numpy
  - pyodbc ou sqlalchemy

### 3.2 Negócio
- **Aprovações necessárias**: 
  - Diretoria Comercial para metas anuais
  - Gestão de Performance para ajustes mensais
- **Janelas de execução**: 
  - Início do ano (carga completa)
  - Mensalmente (ajustes)
- **SLAs dependentes**: 
  - Disponível antes do dia 5 de cada mês
  - Crítico para cálculo de performance mensal

## 4. Arquitetura do Pipeline

### 4.1 Diagrama de Fluxo
```
[Google Sheets] ─→ [API Extract] ─→ [Validation] ─→ [Transform] ─→ [Bronze Load] ─→ [Integrity Check]
                                           ↓                                              ↓
                                    [Quarantine]                                    [Performance Report]
```

### 4.2 Componentes
| Componente | Tecnologia | Função | Configuração |
|------------|------------|--------|--------------|
| Extractor | Google Sheets API v4 | Ler 2500-3000 linhas | Batch de 500 linhas |
| Validator | Python/Pandas | Validar metas e períodos | Classe PerformanceTargetsETL |
| Transformer | Python/Pandas | Padronizar datas e valores | Métodos transform internos |
| Loader | SQLAlchemy/PyODBC | TRUNCATE + INSERT otimizado | batch_size=500 |

## 5. Processo de Extração

### 5.1 Fontes de Dados

#### Fonte: Google Sheets - m7_performance_targets
- **Tipo**: Google Sheets via API
- **ID da Planilha**: `1nm-z2Fbp7pasHx5gmVbm7JPNBRWp4iRElYCbVfEFpOE`
- **Range**: `'Página1!A:I'` (todas as colunas)
- **Particularidades**: Volume alto requer paginação
- **Conexão**: 
  ```python
  # Configuração para volume alto
  SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
  SERVICE_ACCOUNT_FILE = 'credentials/google_sheets_api.json'
  SPREADSHEET_ID = '1nm-z2Fbp7pasHx5gmVbm7JPNBRWp4iRElYCbVfEFpOE'
  RANGE_NAME = 'Página1!A:I'
  BATCH_SIZE = 500  # Otimizado para SQL Server
  ```

### 5.2 Estratégia de Extração
- **Tipo**: Full mensal (substitui dados do ano corrente)
- **Controle**: Timestamp + hash por assessor/indicador/mês
- **Performance**: Leitura em batches para evitar timeout
- **Validação prévia**: 
  - Mínimo 1000 registros esperados
  - Verificar 12 meses para cada combinação assessor/indicador

## 6. Processo de Transformação

### 6.1 Limpeza de Dados
| Validação | Regra | Ação se Falha |
|-----------|-------|---------------|
| Campos obrigatórios | Todos campos NOT NULL | Quarentena |
| Formato codigo_assessor_crm | Padrão AAI + números | Log warning + aceitar |
| Period_type | Sempre 'MENSAL' | Ajustar automaticamente |
| Period_start formato | YYYY-MM-01 (primeiro dia) | Ajustar para dia 01 |
| Period_end formato | Último dia do mês | Calcular automaticamente |
| Target_value numérico | > 0 (exceto % e invertidos) | Quarentena se inválido |
| Stretch > Target > Minimum | Validar lógica (exceto invertidos) | Log warning |
| 12 meses completos | Jan-Dez para cada assessor/indicador | Warning se incompleto |

### 6.2 Transformações Aplicadas

#### T1: Padronização de Períodos
```python
def standardize_periods(df):
    """Garante que períodos estejam corretos"""
    # Converter para datetime
    df['period_start'] = pd.to_datetime(df['period_start'])
    
    # Forçar primeiro dia do mês
    df['period_start'] = df['period_start'].apply(
        lambda x: x.replace(day=1)
    )
    
    # Calcular último dia do mês
    df['period_end'] = df['period_start'] + pd.offsets.MonthEnd(0)
    
    # Period type sempre MENSAL
    df['period_type'] = 'MENSAL'
    
    return df
```

#### T2: Conversão e Validação de Valores
```python
def convert_and_validate_values(df):
    """Converte valores e valida lógica de metas"""
    # Converter para numérico
    value_cols = ['target_value', 'stretch_value', 'minimum_value']
    for col in value_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Identificar indicadores invertidos (precisa join com indicators)
    inverted_indicators = self.get_inverted_indicators()
    df['is_inverted'] = df['indicator_code'].isin(inverted_indicators)
    
    # Validar lógica stretch > target > minimum (exceto invertidos)
    def validate_target_logic(row):
        if pd.isna(row['stretch_value']) or pd.isna(row['minimum_value']):
            return True  # OK se valores opcionais não existem
        
        if row['is_inverted']:
            # Para invertidos: stretch < target < minimum
            return row['stretch_value'] <= row['target_value'] <= row['minimum_value']
        else:
            # Normal: stretch > target > minimum
            return row['stretch_value'] >= row['target_value'] >= row['minimum_value']
    
    df['target_logic_valid'] = df.apply(validate_target_logic, axis=1)
    
    return df
```

#### T3: Validação de Completude Anual
```python
def validate_annual_completeness(df):
    """Verifica se cada assessor/indicador tem 12 meses"""
    validation_errors = []
    
    # Agrupar por assessor e indicador
    grouped = df.groupby(['codigo_assessor_crm', 'indicator_code'])
    
    for (assessor, indicator), group in grouped:
        months = group['period_start'].dt.month.unique()
        missing_months = set(range(1, 13)) - set(months)
        
        if missing_months:
            validation_errors.append({
                'error_type': 'INCOMPLETE_YEAR',
                'codigo_assessor_crm': assessor,
                'indicator_code': indicator,
                'missing_months': sorted(missing_months),
                'months_found': len(months)
            })
    
    return validation_errors
```

#### T4: Enriquecimento com Metadados
```python
def add_metadata(df):
    """Adiciona metadados de controle"""
    df['extraction_timestamp'] = datetime.now()
    df['source_file'] = 'google_sheets:m7_performance_targets'
    
    # Hash por registro único
    df['row_hash'] = df.apply(lambda x: hashlib.md5(
        f"{x['codigo_assessor_crm']}_{x['indicator_code']}_{x['period_start'].strftime('%Y-%m')}".encode()
    ).hexdigest(), axis=1)
    
    # Adicionar ano de referência
    df['target_year'] = df['period_start'].dt.year
    
    # Adicionar quarter para análises
    df['target_quarter'] = df['period_start'].dt.quarter
    
    return df
```

## 7. Processo de Carga

### 7.1 Destino
- **Sistema**: SQL Server - M7Medallion
- **Schema.Tabela**: bronze.performance_targets
- **Método de carga**: 
  - TRUNCATE completo + INSERT (área de staging Bronze)
  - Bronze não mantém histórico (Silver mantém)

### 7.2 Estrutura da Tabela Bronze
```sql
CREATE TABLE bronze.performance_targets (
    load_id INT IDENTITY(1,1) PRIMARY KEY,
    load_timestamp DATETIME NOT NULL DEFAULT GETDATE(),
    load_source VARCHAR(200) NOT NULL DEFAULT 'GoogleSheets:m7_performance_targets',
    
    -- Campos da planilha
    codigo_assessor_crm VARCHAR(MAX),
    nome_assessor VARCHAR(MAX),
    indicator_code VARCHAR(MAX),
    period_type VARCHAR(MAX),
    period_start VARCHAR(MAX),
    period_end VARCHAR(MAX),
    target_value VARCHAR(MAX),
    stretch_value VARCHAR(MAX),
    minimum_value VARCHAR(MAX),
    
    -- Metadados de controle
    row_number INT,
    row_hash VARCHAR(32),
    target_year INT,
    target_quarter INT,
    is_processed BIT DEFAULT 0,
    processing_date DATETIME NULL,
    processing_status VARCHAR(50) NULL,
    processing_notes VARCHAR(MAX) NULL,
    
    -- Validações
    target_logic_valid BIT,
    is_inverted BIT,
    validation_errors VARCHAR(MAX)
);

-- Índices para performance com volume alto
CREATE INDEX IX_bronze_targets_year 
ON bronze.performance_targets (target_year, codigo_assessor_crm, indicator_code)
WHERE is_processed = 0;

CREATE INDEX IX_bronze_targets_lookup
ON bronze.performance_targets (codigo_assessor_crm, indicator_code, period_start)
INCLUDE (target_value, stretch_value, minimum_value);
```

### 7.3 Estratégia de Carga Otimizada
```python
def bulk_load_targets(df, connection, batch_size=500):
    """Carga otimizada para grande volume"""
    # Deletar dados do ano se for recarga completa
    target_year = df['target_year'].iloc[0]
    
    with connection.begin():
        # Delete existing year data
        connection.execute(
            f"DELETE FROM bronze.performance_targets WHERE target_year = {target_year}"
        )
        
        # Bulk insert in batches
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            batch.to_sql(
                'performance_targets',
                connection,
                schema='bronze',
                if_exists='append',
                index=False,
                method='multi'  # Mais eficiente para SQL Server
            )
            
            logger.info(f"Carregado batch {i//batch_size + 1} de {len(df)//batch_size + 1}")
```

## 8. Tratamento de Erros

### 8.1 Tipos de Erro e Ações
| Tipo de Erro | Detecção | Ação | Notificação |
|--------------|----------|------|-------------|
| Google Sheets indisponível | API timeout | Retry 3x com backoff | Email + Log |
| Volume inesperado | < 1000 ou > 5000 registros | Warning + continuar | Email gestão |
| Metas inconsistentes | Stretch < Target (normal) | Carregar + flag erro | Report detalhado |
| Ano incompleto | < 12 meses por assessor | Carregar parcial | Email urgente |
| Valores negativos indevidos | Target < 0 (exceto %) | Quarentena linha | Log erro |
| Período inválido | Fora do ano corrente | Skip + warning | Log |

### 8.2 Processo de Validação em Lote
```python
def validate_batch(df):
    """Validações em lote para performance"""
    errors = {
        'critical': [],
        'warning': [],
        'info': []
    }
    
    # Validação 1: Volume total
    if len(df) < 1000:
        errors['warning'].append(f"Volume baixo: apenas {len(df)} registros")
    elif len(df) > 5000:
        errors['warning'].append(f"Volume alto: {len(df)} registros")
    
    # Validação 2: Assessores únicos
    unique_assessors = df['codigo_assessor_crm'].nunique()
    if unique_assessors < 20:
        errors['warning'].append(f"Poucos assessores: {unique_assessors}")
    
    # Validação 3: Metas zeradas
    zero_targets = df[df['target_value'] == 0]
    if len(zero_targets) > 0:
        errors['info'].append(f"{len(zero_targets)} metas com valor zero")
    
    # Validação 4: Lógica de valores
    invalid_logic = df[~df['target_logic_valid']]
    if len(invalid_logic) > 0:
        errors['critical'].append(
            f"{len(invalid_logic)} registros com lógica stretch/target/minimum inválida"
        )
    
    return errors
```

## 9. Monitoramento e Auditoria

### 9.1 Métricas de Performance
| Métrica | Threshold | Alerta |
|---------|-----------|--------|
| Tempo total execução | < 5 minutos | > 10 minutos |
| Registros processados | 2000-3000 | Fora do range |
| Registros/segundo | > 100 | < 50 |
| Memória utilizada | < 500MB | > 1GB |
| Taxa erro validação | < 1% | > 5% |

### 9.2 Logs Detalhados com Estatísticas
```python
logger = logging.getLogger('ETL-003')

# Log inicial
logger.info(f"===== INICIANDO ETL-003 - TARGETS =====")
logger.info(f"Timestamp: {datetime.now()}")
logger.info(f"Planilha: {SPREADSHEET_ID}")

# Durante processamento
logger.info(f"Registros extraídos: {len(df)}")
logger.info(f"Período: {df['period_start'].min()} a {df['period_start'].max()}")
logger.info(f"Assessores únicos: {df['codigo_assessor_crm'].nunique()}")
logger.info(f"Indicadores únicos: {df['indicator_code'].nunique()}")
logger.info(f"Combinações assessor/indicador: {df.groupby(['codigo_assessor_crm', 'indicator_code']).ngroups}")

# Estatísticas de valores
logger.info("=== Estatísticas de Metas ===")
logger.info(f"Target médio: R$ {df['target_value'].mean():,.2f}")
logger.info(f"Target total anual: R$ {df['target_value'].sum():,.2f}")
logger.info(f"Metas com stretch: {df['stretch_value'].notna().sum()} ({df['stretch_value'].notna().mean():.1%})")

# Validações
if validation_errors:
    logger.warning(f"=== Erros de Validação: {len(validation_errors)} ===")
    for error in validation_errors[:10]:
        logger.warning(f"  - {error}")

# Performance
logger.info(f"Tempo de processamento: {elapsed_time:.2f} segundos")
logger.info(f"Taxa de processamento: {len(df)/elapsed_time:.0f} registros/segundo")
```

### 9.3 Relatório de Auditoria Mensal
```sql
-- Procedure para gerar relatório de metas
CREATE PROCEDURE audit.prc_generate_targets_report
    @year INT = NULL
AS
BEGIN
    SET @year = ISNULL(@year, YEAR(GETDATE()));
    
    -- Resumo geral
    SELECT 
        'RESUMO GERAL' as section,
        COUNT(DISTINCT codigo_assessor_crm) as total_assessores,
        COUNT(DISTINCT indicator_code) as total_indicadores,
        COUNT(*) as total_metas,
        SUM(CASE WHEN target_logic_valid = 0 THEN 1 ELSE 0 END) as metas_invalidas,
        AVG(CAST(target_value AS FLOAT)) as target_medio
    FROM bronze.performance_targets
    WHERE target_year = @year;
    
    -- Por indicador
    SELECT 
        'POR INDICADOR' as section,
        indicator_code,
        COUNT(DISTINCT codigo_assessor_crm) as assessores,
        COUNT(*) as metas,
        AVG(CAST(target_value AS FLOAT)) as target_medio,
        MIN(CAST(target_value AS FLOAT)) as target_min,
        MAX(CAST(target_value AS FLOAT)) as target_max
    FROM bronze.performance_targets
    WHERE target_year = @year
    GROUP BY indicator_code
    ORDER BY indicator_code;
    
    -- Validações críticas
    SELECT 
        'VALIDAÇÕES CRÍTICAS' as section,
        codigo_assessor_crm,
        indicator_code,
        COUNT(*) as meses_definidos,
        12 - COUNT(*) as meses_faltantes,
        STRING_AGG(MONTH(CAST(period_start AS DATE)), ',') as meses_existentes
    FROM bronze.performance_targets
    WHERE target_year = @year
    GROUP BY codigo_assessor_crm, indicator_code
    HAVING COUNT(*) < 12;
END;
```

## 10. Qualidade de Dados

### 10.1 Validações Pós-Carga
| Validação | Query/Método | Threshold | Ação se Falha |
|-----------|--------------|-----------|---------------|
| Completude anual | 12 meses por assessor/indicador | 100% | Notificar gestão |
| Targets positivos | target_value > 0 (exceto %) | > 99% | Revisar zeros |
| Lógica stretch/minimum | Validação por tipo | 100% | Corrigir planilha |
| Crescimento YoY razoável | < 100% aumento | > 95% | Verificar outliers |
| Consistência temporal | Sem gaps nos meses | 100% | Preencher gaps |

### 10.2 Queries de Validação de Qualidade
```sql
-- Verificar completude anual
WITH monthly_coverage AS (
    SELECT 
        codigo_assessor_crm,
        indicator_code,
        target_year,
        COUNT(DISTINCT MONTH(CAST(period_start AS DATE))) as months_count,
        MIN(CAST(period_start AS DATE)) as first_month,
        MAX(CAST(period_start AS DATE)) as last_month
    FROM bronze.performance_targets
    WHERE is_processed = 0
    GROUP BY codigo_assessor_crm, indicator_code, target_year
)
SELECT 
    codigo_assessor_crm,
    indicator_code,
    target_year,
    months_count,
    CASE 
        WHEN months_count = 12 THEN 'COMPLETO'
        WHEN months_count >= 9 THEN 'QUASE COMPLETO'
        ELSE 'INCOMPLETO'
    END as status
FROM monthly_coverage
WHERE months_count < 12
ORDER BY months_count, codigo_assessor_crm;

-- Análise de crescimento mensal
WITH monthly_growth AS (
    SELECT 
        codigo_assessor_crm,
        indicator_code,
        CAST(period_start AS DATE) as month_date,
        CAST(target_value AS FLOAT) as target,
        LAG(CAST(target_value AS FLOAT)) OVER (
            PARTITION BY codigo_assessor_crm, indicator_code 
            ORDER BY period_start
        ) as prev_target
    FROM bronze.performance_targets
    WHERE is_processed = 0
)
SELECT 
    codigo_assessor_crm,
    indicator_code,
    month_date,
    target,
    prev_target,
    CASE 
        WHEN prev_target > 0 THEN ((target - prev_target) / prev_target * 100)
        ELSE NULL 
    END as growth_percent
FROM monthly_growth
WHERE ABS((target - prev_target) / NULLIF(prev_target, 0)) > 0.5 -- Variação > 50%
ORDER BY growth_percent DESC;
```

## 11. Agendamento e Triggers

### 11.1 Schedule
- **Ferramenta**: SQL Server Agent / Airflow
- **Frequência**: 
  - **Anual**: Dezembro (carga completa ano seguinte)
  - **Mensal**: Dia 5 (ajustes e correções)
  - **Sob demanda**: Via procedure
- **Dependências**: 
  - ETL-001 e ETL-002 devem estar atualizados
  - Executar antes do cálculo mensal (dia 10)

### 11.2 Execução Diferenciada por Período
```sql
CREATE PROCEDURE bronze.prc_extract_performance_targets
    @execution_mode VARCHAR(20) = 'MONTHLY', -- ANNUAL, MONTHLY, CUSTOM
    @target_year INT = NULL,
    @force_reload BIT = 0
AS
BEGIN
    SET @target_year = ISNULL(@target_year, YEAR(GETDATE()));
    
    IF @execution_mode = 'ANNUAL'
    BEGIN
        -- Carga completa do ano
        EXEC bronze.prc_extract_targets_annual @year = @target_year + 1;
    END
    ELSE IF @execution_mode = 'MONTHLY'
    BEGIN
        -- Ajustes do mês corrente
        EXEC bronze.prc_extract_targets_monthly 
            @year = @target_year,
            @month = MONTH(GETDATE());
    END
    ELSE IF @execution_mode = 'CUSTOM'
    BEGIN
        -- Recarga específica
        EXEC bronze.prc_extract_targets_custom
            @year = @target_year,
            @force = @force_reload;
    END
END;
```

## 12. Manutenção e Operação

### 12.1 Procedimentos Operacionais
- **Carga anual inicial**: 
  ```sql
  -- Executar em dezembro para ano seguinte
  EXEC bronze.prc_extract_performance_targets 
      @execution_mode = 'ANNUAL',
      @target_year = YEAR(GETDATE()) + 1;
  ```
- **Ajuste mensal**: Interface para correções pontuais
- **Recálculo completo**: Quando houver mudança estrutural

### 12.2 Troubleshooting Comum
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| Metas faltando | < 12 meses | Query monthly_coverage | Completar na planilha |
| Performance lenta | > 10 min | Check índices e batches | Aumentar batch_size |
| Valores inconsistentes | Stretch < Target | Query validation | Corrigir lógica na planilha |
| Timeout Google API | Erro 503 | Muitos dados de uma vez | Implementar paginação |
| Memória excedida | Out of memory | DataFrame muito grande | Processar em chunks |

### 12.3 Scripts de Manutenção Específicos
```sql
-- Preencher gaps de meses faltantes com interpolação
WITH month_gaps AS (
    SELECT 
        a.codigo_assessor_crm,
        a.indicator_code,
        m.month_date
    FROM (
        SELECT DISTINCT codigo_assessor_crm, indicator_code 
        FROM bronze.performance_targets
    ) a
    CROSS JOIN (
        SELECT DATEADD(MONTH, n, '2024-01-01') as month_date
        FROM (SELECT TOP 12 ROW_NUMBER() OVER (ORDER BY object_id) - 1 as n FROM sys.objects) x
    ) m
    LEFT JOIN bronze.performance_targets t
        ON a.codigo_assessor_crm = t.codigo_assessor_crm
        AND a.indicator_code = t.indicator_code
        AND t.period_start = m.month_date
    WHERE t.load_id IS NULL
)
-- Inserir registros faltantes com interpolação
INSERT INTO bronze.performance_targets_interpolated
SELECT /* lógica de interpolação */;

-- Análise de sazonalidade
SELECT 
    indicator_code,
    MONTH(CAST(period_start AS DATE)) as month_num,
    AVG(CAST(target_value AS FLOAT)) as avg_target,
    STDEV(CAST(target_value AS FLOAT)) as std_target,
    COUNT(*) as sample_size
FROM bronze.performance_targets
WHERE is_processed = 1
GROUP BY indicator_code, MONTH(CAST(period_start AS DATE))
ORDER BY indicator_code, month_num;
```

## 13. Segurança e Compliance

### 13.1 Classificação de Dados
- **Nível de sensibilidade**: Confidencial (metas estratégicas)
- **PII/PCI**: Contém nomes de funcionários
- **Dados financeiros**: Metas de captação sensíveis

### 13.2 Controles de Segurança
- **Autenticação**: Service Account Google (read-only)
- **Criptografia em trânsito**: HTTPS/TLS 1.2+
- **Acesso banco**: 
  - Role ETL_BRONZE_WRITER para carga
  - Role TARGETS_READER para consulta
- **Mascaramento**: 
  - Valores em ambientes não-produção
  - Nomes de assessores para externos
- **Auditoria**: 
  - Log de todas as alterações de metas
  - Tracking de ajustes manuais

## 14. Versionamento e Mudanças

### 14.1 Controle de Versão
- **Script Python**: `/etl/performance/etl_003_targets.py`
- **Config**: `/config/etl_003_config.json`
- **SQL Objects**: `/sql/bronze/targets/`

### 14.2 Processo de Mudança para Volume Alto
1. Testar com subset (1 assessor, 12 meses)
2. Testar com 10% dos dados
3. Validar performance e memória
4. Deploy incremental
5. Monitorar primeira carga completa

## 15. Anexos

### 15.1 Script Python Principal
```python
# /etl/performance/etl_003_targets.py
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine
import logging
from datetime import datetime
import hashlib
import json
from typing import List, Dict, Tuple

class PerformanceTargetsETL:
    """ETL para processar metas mensais de performance"""
    
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.setup_logging()
        self.setup_connections()
        self.validation_errors = []
        self.batch_size = self.config.get('batch_size', 500)
        
    def extract_in_batches(self) -> pd.DataFrame:
        """Extrai dados em lotes para melhor performance"""
        logger.info(f"Iniciando extração em lotes de {self.batch_size}")
        
        service = build('sheets', 'v4', credentials=self.creds)
        sheet = service.spreadsheets()
        
        # Primeiro, pegar o range total
        result = sheet.values().get(
            spreadsheetId=self.config['spreadsheet_id'],
            range='Página1!A1:A1'
        ).execute()
        
        # Determinar número de linhas
        sheet_metadata = sheet.get(
            spreadsheetId=self.config['spreadsheet_id']
        ).execute()
        
        total_rows = sheet_metadata['sheets'][0]['properties']['gridProperties']['rowCount']
        logger.info(f"Total de linhas na planilha: {total_rows}")
        
        # Extrair em batches
        all_data = []
        headers = None
        
        for start_row in range(1, total_rows, self.batch_size):
            end_row = min(start_row + self.batch_size - 1, total_rows)
            range_name = f'Página1!A{start_row}:I{end_row}'
            
            try:
                result = sheet.values().get(
                    spreadsheetId=self.config['spreadsheet_id'],
                    range=range_name
                ).execute()
                
                values = result.get('values', [])
                
                if start_row == 1 and values:
                    headers = values[0]
                    values = values[1:]
                
                all_data.extend(values)
                logger.info(f"Extraído batch: linhas {start_row} a {end_row}")
                
            except Exception as e:
                logger.error(f"Erro ao extrair batch {start_row}-{end_row}: {e}")
                raise
        
        # Converter para DataFrame
        df = pd.DataFrame(all_data, columns=headers)
        logger.info(f"Total de registros extraídos: {len(df)}")
        
        return df
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica todas as transformações necessárias"""
        logger.info("Iniciando transformações")
        
        # T1: Padronizar períodos
        df = self.standardize_periods(df)
        
        # T2: Converter e validar valores
        df = self.convert_and_validate_values(df)
        
        # T3: Validar completude anual
        annual_errors = self.validate_annual_completeness(df)
        if annual_errors:
            self.validation_errors.extend(annual_errors)
            logger.warning(f"{len(annual_errors)} assessores com ano incompleto")
        
        # T4: Adicionar metadados
        df = self.add_metadata(df)
        
        # Validação em lote
        batch_errors = self.validate_batch(df)
        self.log_batch_validation_results(batch_errors)
        
        return df
    
    def load_optimized(self, df: pd.DataFrame) -> int:
        """Carga otimizada para grande volume"""
        logger.info(f"Iniciando carga otimizada de {len(df)} registros")
        
        start_time = datetime.now()
        records_loaded = 0
        
        # Determinar se é carga completa ou incremental
        target_year = df['target_year'].iloc[0]
        
        with self.engine.begin() as conn:
            # Se for janeiro, assumir carga anual completa
            if datetime.now().month == 1 or self.config.get('force_annual_reload'):
                logger.info(f"Executando carga completa para o ano {target_year}")
                conn.execute(
                    f"DELETE FROM bronze.performance_targets WHERE target_year = {target_year}"
                )
            
            # Carregar em batches
            for i in range(0, len(df), self.batch_size):
                batch_start = datetime.now()
                batch = df.iloc[i:i+self.batch_size].copy()
                
                # Adicionar metadados do batch
                batch['load_timestamp'] = datetime.now()
                batch['batch_number'] = i // self.batch_size + 1
                
                # Inserir batch
                batch.to_sql(
                    'performance_targets',
                    conn,
                    schema='bronze',
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                records_loaded += len(batch)
                batch_time = (datetime.now() - batch_start).total_seconds()
                
                logger.info(
                    f"Batch {i//self.batch_size + 1}/{(len(df)-1)//self.batch_size + 1} "
                    f"carregado em {batch_time:.2f}s "
                    f"({len(batch)/batch_time:.0f} records/s)"
                )
        
        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Carga completa: {records_loaded} registros em {total_time:.2f}s "
            f"({records_loaded/total_time:.0f} records/s)"
        )
        
        return records_loaded
    
    def generate_quality_report(self, df: pd.DataFrame) -> Dict:
        """Gera relatório de qualidade dos dados"""
        report = {
            'summary': {
                'total_records': len(df),
                'unique_assessors': df['codigo_assessor_crm'].nunique(),
                'unique_indicators': df['indicator_code'].nunique(),
                'date_range': f"{df['period_start'].min()} to {df['period_start'].max()}",
                'extraction_time': datetime.now().isoformat()
            },
            'validation': {
                'records_with_invalid_logic': len(df[~df['target_logic_valid']]),
                'records_missing_stretch': df['stretch_value'].isna().sum(),
                'records_missing_minimum': df['minimum_value'].isna().sum(),
                'zero_targets': len(df[df['target_value'] == 0])
            },
            'statistics': {
                'avg_target_value': float(df['target_value'].mean()),
                'total_annual_target': float(df['target_value'].sum()),
                'targets_by_indicator': df.groupby('indicator_code')['target_value'].agg(['count', 'mean', 'sum']).to_dict()
            }
        }
        
        return report
    
    def run(self):
        """Executa o pipeline completo com tratamento de erro robusto"""
        start_time = datetime.now()
        
        try:
            # Extract
            df = self.extract_in_batches()
            
            # Validação inicial de volume
            if len(df) < 1000:
                logger.warning(f"Volume muito baixo: {len(df)} registros")
            elif len(df) > 5000:
                logger.warning(f"Volume muito alto: {len(df)} registros")
            
            # Transform
            df = self.transform(df)
            
            # Generate quality report
            quality_report = self.generate_quality_report(df)
            logger.info(f"Quality Report: {json.dumps(quality_report['summary'], indent=2)}")
            
            # Load
            records_loaded = self.load_optimized(df)
            
            # Post-load validation
            self.execute_post_load_validation(df['target_year'].iloc[0])
            
            # Log success
            self.log_execution(
                status='SUCCESS',
                records=records_loaded,
                duration=(datetime.now() - start_time).seconds,
                quality_report=quality_report
            )
            
            # Send notifications if needed
            if self.validation_errors:
                self.send_validation_report()
            
        except Exception as e:
            logger.error(f"Erro crítico no ETL: {str(e)}")
            self.log_execution(
                status='ERROR',
                error_message=str(e),
                duration=(datetime.now() - start_time).seconds
            )
            raise

if __name__ == "__main__":
    etl = PerformanceTargetsETL('config/etl_003_config.json')
    etl.run()
```

### 15.2 Configuração JSON
```json
{
    "etl_name": "ETL-IND-003-performance-targets",
    "spreadsheet_id": "1nm-z2Fbp7pasHx5gmVbm7JPNBRWp4iRElYCbVfEFpOE",
    "range_name": "Página1!A:I",
    "credentials_path": "credentials/google_sheets_api.json",
    "batch_size": 500,
    "database": {
        "server": "m7-dw-server",
        "database": "M7Medallion",
        "driver": "ODBC Driver 17 for SQL Server",
        "fast_executemany": true
    },
    "validation": {
        "min_records": 1000,
        "max_records": 5000,
        "required_months": 12,
        "max_growth_percent": 100,
        "target_value_threshold": {
            "min": 0,
            "max": 100000000
        }
    },
    "execution_mode": {
        "annual_reload_month": 12,
        "force_annual_reload": false,
        "incremental_updates": true
    },
    "notifications": {
        "email_on_error": [
            "gestao.performance@m7investimentos.com.br",
            "controladoria@m7investimentos.com.br"
        ],
        "email_on_warning": ["gestao.performance@m7investimentos.com.br"],
        "slack_webhook": "https://hooks.slack.com/..."
    }
}
```

### 15.3 Documentação Relacionada
- **MOD-IND-004**: [Modelo de dados Performance Targets Silver](../modelos-dados/MOD-IND-004-performance-targets-silver.md) (transformação Bronze → Silver)
- **QRY-TAR-003**: [Procedure prc_bronze_to_silver_performance_targets](../../operacional/queries/bronze/QRY-TAR-003-prc_bronze_to_silver_performance_targets.sql)

### 15.4 Referências
- [Google Sheets API - Batch Operations](https://developers.google.com/sheets/api/guides/batchupdate)
- [SQL Server Bulk Insert Best Practices](https://docs.microsoft.com/en-us/sql/relational-databases/import-export/bulk-import-large-amounts-of-data)
- [ARQ-DWH-001 - Visão Geral do Data Warehouse](../../estrategico/arquiteturas/ARQ-DWH-001-visao-geral-datawarehouse.md)
- [ARQ-IND-001 - Performance Tracking System](../../estrategico/arquiteturas/ARQ-IND-001-performance-tracking-system.md)
- [ETL-IND-001 - Performance Indicators](ETL-IND-001-extracao-indicadores-performance.md)
- [ETL-IND-002 - Performance Assignments](ETL-IND-002-extracao-atribuicoes-performance.md)
- [MOD-IND-001 - Sistema Tracking Performance KPIs](../modelos-dados/MOD-IND-001-sistema-tracking-performance-kpis.md)

---

**Documento criado por**: Arquitetura de Dados M7 Investimentos  
**Data**: 2025-01-18  
**Revisão**: Mensal ou sob mudança de metas