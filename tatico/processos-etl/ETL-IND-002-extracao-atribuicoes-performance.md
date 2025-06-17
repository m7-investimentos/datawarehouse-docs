# ETL-002-performance-assignments-extraction

---
título: Extração de Atribuições de Performance - Google Sheets para Bronze
tipo: ETL - Processo ETL
versão: 1.0.0
última_atualização: 2025-01-16
autor: arquitetura.dados@m7investimentos.com.br
aprovador: diretoria.ti@m7investimentos.com.br
tags: [etl, performance, assignments, google-sheets, bronze, metadados]
status: aprovado
dependências:
  - tipo: arquitetura
    ref: [ARQ-001]
    repo: datawarehouse-docs
  - tipo: modelo
    ref: [MOD-001]
    repo: datawarehouse-docs
  - tipo: etl
    ref: [ETL-001]
    repo: datawarehouse-docs
  - tipo: planilha
    ref: [m7_performance_assignments]
    repo: google-sheets
---

## 1. Objetivo

Extrair dados de atribuições de indicadores de performance por assessor da planilha Google Sheets `m7_performance_assignments` para a camada Bronze do Data Warehouse, incluindo validações de integridade de pesos e relacionamentos.

## 2. Escopo e Aplicabilidade

### 2.1 Escopo
- **Fonte de dados**: Google Sheets - m7_performance_assignments
- **Destino**: M7Medallion.bronze.performance_assignments
- **Volume esperado**: ~200-500 registros
- **Frequência**: Diária ou sob demanda (mudanças trimestrais)

### 2.2 Fora de Escopo
- Execução de cálculos de performance
- Processamento de metas (targets)
- Validação de fórmulas SQL dos indicadores

## 3. Pré-requisitos e Dependências

### 3.1 Técnicos
- **Conectividade**: 
  - Google Sheets API v4 habilitada
  - Service Account com permissões de leitura
  - Credenciais JSON armazenadas seguramente
- **Recursos computacionais**: Mínimos (< 5MB de dados)
- **Software/Ferramentas**: 
  - Python 3.8+
  - google-api-python-client
  - pandas
  - numpy
  - pyodbc ou sqlalchemy

### 3.2 Negócio
- **Aprovações necessárias**: 
  - Gestão de Performance para alterações de pesos
  - RH para novos assessores
- **Janelas de execução**: Preferencialmente fora do horário comercial
- **SLAs dependentes**: 
  - Processamento de metas mensais
  - Cálculo de remuneração variável

## 4. Arquitetura do Pipeline

### 4.1 Diagrama de Fluxo
```
[Google Sheets] ─→ [API Extract] ─→ [Validation] ─→ [Transform] ─→ [Bronze Load] ─→ [Post-Validation]
                                           ↓                                              ↓
                                    [Quarantine]                                   [Alert if Issues]
```

### 4.2 Componentes
| Componente | Tecnologia | Função | Configuração |
|------------|------------|--------|--------------|
| Extractor | Google Sheets API v4 | Ler dados da planilha | credentials.json |
| Validator | Python/Pandas | Validar pesos e relacionamentos | validation_rules.py |
| Transformer | Python/Pandas | Padronizar e enriquecer | transform_config.json |
| Loader | PyODBC/SQLAlchemy | Inserir em Bronze | connection_string |

## 5. Processo de Extração

### 5.1 Fontes de Dados

#### Fonte: Google Sheets - m7_performance_assignments
- **Tipo**: Google Sheets via API
- **ID da Planilha**: `1nm-z2Fbp7pasHx5gmVbm7JPNBRWp4iRElYCbVfEFpOE`
- **Range**: `'Página1!A:J'` (todas as colunas)
- **Conexão**: 
  ```python
  # Configuração
  SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
  SERVICE_ACCOUNT_FILE = 'path/to/credentials.json'
  SPREADSHEET_ID = '1nm-z2Fbp7pasHx5gmVbm7JPNBRWp4iRElYCbVfEFpOE'
  RANGE_NAME = 'Página1!A:J'
  ```

### 5.2 Estratégia de Extração
- **Tipo**: Full (sempre lê toda a planilha)
- **Controle de watermark**: timestamp de extração + hash do conteúdo
- **Validação prévia**: Verificar se há pelo menos 10 linhas (mínimo esperado)

## 6. Processo de Transformação

### 6.1 Limpeza de Dados
| Validação | Regra | Ação se Falha |
|-----------|-------|---------------|
| Campos obrigatórios | cod_assessor, indicator_code, indicator_type NOT NULL | Quarentena |
| Formato cod_assessor | Padrão AAI + números (ex: AAI001) | Log warning + aceitar |
| indicator_type válido | IN ('CARD', 'GATILHO', 'KPI', 'PPI', 'METRICA') | Quarentena |
| Weight para CARD | Entre 0.01 e 100.00 | Quarentena |
| Weight para não-CARD | Deve ser 0.00 ou NULL | Ajustar para 0.00 |
| valid_from formato | Data válida YYYY-MM-DD | Quarentena |
| valid_to lógica | NULL ou > valid_from | Quarentena |

### 6.2 Transformações Aplicadas

#### T1: Padronização de Códigos e Tipos
```python
def standardize_assignments(df):
    # Padronizar cod_assessor
    df['cod_assessor'] = df['cod_assessor'].str.upper().str.strip()
    
    # Padronizar indicator_code
    df['indicator_code'] = df['indicator_code'].str.upper().str.replace(' ', '_')
    
    # Padronizar indicator_type
    df['indicator_type'] = df['indicator_type'].str.upper().str.strip()
    
    # Converter weight para numérico
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(0.0)
    
    # Para não-CARD, garantir weight = 0
    df.loc[df['indicator_type'] != 'CARD', 'weight'] = 0.0
    
    return df
```

#### T2: Validação de Soma de Pesos
```python
def validate_weights(df):
    """Valida que soma dos pesos CARD = 100% por assessor/período"""
    validation_errors = []
    
    # Filtrar apenas registros CARD ativos
    card_df = df[(df['indicator_type'] == 'CARD') & 
                 (df['valid_to'].isna())]
    
    # Agrupar por assessor e valid_from
    weight_sums = card_df.groupby(['cod_assessor', 'valid_from'])['weight'].sum()
    
    # Verificar somas diferentes de 100
    invalid_weights = weight_sums[abs(weight_sums - 100.0) > 0.01]
    
    for (assessor, valid_from), total_weight in invalid_weights.items():
        validation_errors.append({
            'error_type': 'INVALID_WEIGHT_SUM',
            'cod_assessor': assessor,
            'valid_from': valid_from,
            'total_weight': total_weight,
            'expected': 100.0
        })
    
    return validation_errors
```

#### T3: Validação de Relacionamentos
```python
def validate_relationships(df, indicators_df):
    """Valida que todos indicator_codes existem em indicators"""
    validation_errors = []
    
    # Obter lista de códigos válidos
    valid_codes = set(indicators_df['indicator_code'].unique())
    
    # Verificar códigos inválidos
    invalid_codes = df[~df['indicator_code'].isin(valid_codes)]
    
    for _, row in invalid_codes.iterrows():
        validation_errors.append({
            'error_type': 'INVALID_INDICATOR_CODE',
            'cod_assessor': row['cod_assessor'],
            'indicator_code': row['indicator_code'],
            'message': 'Código não existe em performance_indicators'
        })
    
    return validation_errors
```

#### T4: Enriquecimento de Metadados
```python
def add_metadata(df):
    df['extraction_timestamp'] = datetime.now()
    df['source_file'] = 'google_sheets:m7_performance_assignments'
    df['row_hash'] = df.apply(lambda x: hashlib.md5(
        f"{x['cod_assessor']}_{x['indicator_code']}_{x['valid_from']}".encode()
    ).hexdigest(), axis=1)
    
    # Adicionar flag de vigência
    df['is_current'] = df['valid_to'].isna()
    
    return df
```

## 7. Processo de Carga

### 7.1 Destino
- **Sistema**: SQL Server - M7Medallion
- **Schema.Tabela**: bronze.performance_assignments
- **Método de carga**: MERGE (upsert baseado em chave natural)

### 7.2 Estrutura da Tabela Bronze
```sql
CREATE TABLE bronze.performance_assignments (
    load_id INT IDENTITY(1,1) PRIMARY KEY,
    load_timestamp DATETIME NOT NULL DEFAULT GETDATE(),
    load_source VARCHAR(200) NOT NULL DEFAULT 'GoogleSheets:m7_performance_assignments',
    
    -- Campos da planilha
    cod_assessor VARCHAR(MAX),
    nome_assessor VARCHAR(MAX),
    indicator_code VARCHAR(MAX),
    indicator_type VARCHAR(MAX),
    weight VARCHAR(MAX),
    valid_from VARCHAR(MAX),
    valid_to VARCHAR(MAX),
    created_by VARCHAR(MAX),
    approved_by VARCHAR(MAX),
    comments VARCHAR(MAX),
    
    -- Metadados de controle
    row_number INT,
    row_hash VARCHAR(32),
    is_current BIT,
    is_processed BIT DEFAULT 0,
    processing_date DATETIME NULL,
    processing_status VARCHAR(50) NULL,
    processing_notes VARCHAR(MAX) NULL,
    
    -- Validações
    weight_sum_valid BIT,
    indicator_exists BIT,
    validation_errors VARCHAR(MAX)
);

-- Índice para performance
CREATE INDEX IX_bronze_assignments_lookup 
ON bronze.performance_assignments (cod_assessor, indicator_code, valid_from)
WHERE is_processed = 0;
```

## 8. Tratamento de Erros

### 8.1 Tipos de Erro e Ações
| Tipo de Erro | Detecção | Ação | Notificação |
|--------------|----------|------|-------------|
| Google Sheets indisponível | API timeout/401/403 | Retry 3x com backoff | Email + Log |
| Planilha vazia | < 10 registros | Parar execução | Email urgente |
| Soma pesos ≠ 100% | Validação T2 | Carregar mas marcar erro | Email gestão |
| Indicator_code inválido | Validação T3 | Carregar mas marcar erro | Warning log |
| Assessor duplicado/período | Chave única | Log último valor | Warning log |

### 8.2 Processo de Retry e Recuperação
```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    retry=retry_if_exception_type((HttpError, ConnectionError))
)
def extract_from_sheets():
    try:
        service = build('sheets', 'v4', credentials=creds)
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()
        return result.get('values', [])
    except HttpError as e:
        if e.resp.status == 404:
            raise Exception("Planilha não encontrada - verificar ID")
        raise
```

## 9. Monitoramento e Auditoria

### 9.1 Métricas de Performance
| Métrica | Threshold | Alerta |
|---------|-----------|--------|
| Tempo total execução | < 2 minutos | > 5 minutos |
| Registros processados | > 100 | < 50 |
| Taxa de erro validação | < 5% | > 10% |
| Assessores com peso inválido | = 0 | > 0 |

### 9.2 Logs Detalhados
```python
# Estrutura de logging
logger = logging.getLogger('ETL-002')
logger.info(f"===== INICIANDO ETL-002 =====")
logger.info(f"Timestamp: {datetime.now()}")
logger.info(f"Planilha: {SPREADSHEET_ID}")

# Durante processamento
logger.info(f"Registros extraídos: {len(df)}")
logger.info(f"Assessores únicos: {df['cod_assessor'].nunique()}")
logger.info(f"Indicadores únicos: {df['indicator_code'].nunique()}")

# Validações
if validation_errors:
    logger.warning(f"Validações com erro: {len(validation_errors)}")
    for error in validation_errors[:5]:  # Primeiros 5
        logger.warning(f"  - {error}")

# Final
logger.info(f"Registros carregados: {records_loaded}")
logger.info(f"===== ETL-002 CONCLUÍDO =====")
```

### 9.3 Auditoria Detalhada
```sql
-- Tabela específica para auditoria de assignments
CREATE TABLE audit.assignment_weight_history (
    audit_id INT IDENTITY(1,1) PRIMARY KEY,
    audit_timestamp DATETIME DEFAULT GETDATE(),
    cod_assessor VARCHAR(20),
    valid_from DATE,
    indicator_type VARCHAR(20),
    total_weight DECIMAL(5,2),
    is_valid BIT,
    indicators_detail NVARCHAR(MAX), -- JSON com breakdown
    etl_load_id INT
);

-- Procedure para registrar validações
CREATE PROCEDURE audit.prc_log_assignment_validation
    @load_id INT
AS
BEGIN
    INSERT INTO audit.assignment_weight_history
    SELECT 
        GETDATE(),
        cod_assessor,
        valid_from,
        'CARD',
        SUM(CAST(weight AS DECIMAL(5,2))),
        CASE WHEN ABS(SUM(CAST(weight AS DECIMAL(5,2))) - 100) < 0.01 
             THEN 1 ELSE 0 END,
        (SELECT indicator_code, weight 
         FROM bronze.performance_assignments b2
         WHERE b2.cod_assessor = b1.cod_assessor
           AND b2.valid_from = b1.valid_from
           AND b2.indicator_type = 'CARD'
           AND b2.load_id = @load_id
         FOR JSON AUTO),
        @load_id
    FROM bronze.performance_assignments b1
    WHERE indicator_type = 'CARD'
      AND load_id = @load_id
    GROUP BY cod_assessor, valid_from;
END;
```

## 10. Qualidade de Dados

### 10.1 Validações Pós-Carga
| Validação | Query/Método | Threshold | Ação se Falha |
|-----------|--------------|-----------|---------------|
| Todos assessores têm indicadores | COUNT(DISTINCT cod_assessor) | > 30 | Verificar dados fonte |
| Soma pesos CARD = 100% | Query complexa | 100% compliance | Notificar gestão |
| Indicator codes válidos | JOIN com indicators | 100% match | Revisar novos códigos |
| Sem duplicatas ativas | Unique check | 0 duplicatas | Investigar |

### 10.2 Queries de Validação
```sql
-- Validar soma de pesos por assessor
WITH weight_check AS (
    SELECT 
        cod_assessor,
        valid_from,
        SUM(CAST(weight AS DECIMAL(5,2))) as total_weight,
        COUNT(*) as indicator_count
    FROM bronze.performance_assignments
    WHERE indicator_type = 'CARD'
      AND valid_to IS NULL
      AND is_processed = 0
    GROUP BY cod_assessor, valid_from
)
SELECT 
    cod_assessor,
    valid_from,
    total_weight,
    CASE WHEN ABS(total_weight - 100.0) < 0.01 
         THEN 'OK' 
         ELSE 'ERRO' END as status
FROM weight_check
WHERE ABS(total_weight - 100.0) >= 0.01
ORDER BY cod_assessor;

-- Verificar códigos órfãos
SELECT DISTINCT 
    a.indicator_code,
    COUNT(DISTINCT a.cod_assessor) as assessores_afetados
FROM bronze.performance_assignments a
LEFT JOIN bronze.performance_indicators i
    ON a.indicator_code = i.indicator_code
WHERE i.indicator_code IS NULL
  AND a.is_processed = 0
GROUP BY a.indicator_code;
```

## 11. Agendamento e Triggers

### 11.1 Schedule
- **Ferramenta**: SQL Server Agent / Airflow
- **Frequência**: 
  - Diária às 23:00 (capturar mudanças)
  - Sob demanda via procedure
- **Dependências**: 
  - ETL-001 deve executar com sucesso primeiro
  - Antes do ETL-003 (targets)

### 11.2 Comando de Execução Manual
```sql
-- Executar ETL manualmente
EXEC bronze.prc_extract_performance_assignments
    @force_reload = 1,
    @validate_weights = 1,
    @send_notifications = 1;
```

### 11.3 Integração com Pipeline
```python
# Airflow DAG example
assignment_etl = PythonOperator(
    task_id='etl_002_assignments',
    python_callable=run_assignments_etl,
    depends_on_past=False,
    retries=2,
    retry_delay=timedelta(minutes=5)
)

# Dependências
indicators_etl >> assignment_etl >> targets_etl
```

## 12. Manutenção e Operação

### 12.1 Procedimentos Operacionais
- **Re-extração completa**: 
  ```sql
  EXEC bronze.prc_reprocess_assignments @months_back = 3;
  ```
- **Limpeza**: Bronze mantém 30 dias de histórico
- **Correção de pesos**: Interface para ajustes manuais

### 12.2 Troubleshooting Comum
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| Pesos não somam 100% | Erro validação | Query weight_check | Ajustar na planilha |
| Assessor sem indicadores | Missing data | Check assignments | Verificar com RH |
| Performance lenta | > 5 min execução | Check índices | Rebuild índices |
| Indicador novo não aparece | Validation fail | Check indicators ETL | Rodar ETL-001 primeiro |

### 12.3 Scripts de Manutenção
```sql
-- Limpar dados antigos
DELETE FROM bronze.performance_assignments
WHERE load_timestamp < DATEADD(DAY, -30, GETDATE())
  AND is_processed = 1;

-- Reprocessar registros com erro
UPDATE bronze.performance_assignments
SET is_processed = 0,
    processing_status = NULL,
    processing_notes = 'Reprocessamento manual'
WHERE processing_status = 'ERROR'
  AND load_timestamp > DATEADD(DAY, -7, GETDATE());
```

## 13. Segurança e Compliance

### 13.1 Classificação de Dados
- **Nível de sensibilidade**: Confidencial (dados de RH)
- **PII/PCI**: Contém nomes de funcionários

### 13.2 Controles de Segurança
- **Autenticação**: Service Account Google (read-only)
- **Criptografia em trânsito**: HTTPS/TLS 1.2+
- **Acesso banco**: Apenas role ETL_BRONZE_WRITER
- **Mascaramento**: Nomes em ambientes não-produção
- **Auditoria**: Log completo de acessos e mudanças

## 14. Versionamento e Mudanças

### 14.1 Controle de Versão
- **Script Python**: `/etl/performance/etl_002_assignments.py`
- **Config**: `/config/etl_002_config.json`
- **SQL Objects**: Versionados com migrations

### 14.2 Processo de Mudança
1. Mudanças de peso requerem aprovação em planilha
2. Novos tipos de indicador requerem atualização do ETL
3. Deploy sempre com backup da última versão válida
4. Rollback automático se validações críticas falharem

## 15. Anexos

### 15.1 Script Python Principal
```python
# /etl/performance/etl_002_assignments.py
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine
import logging
from datetime import datetime
import hashlib
import json

class PerformanceAssignmentsETL:
    """ETL para processar atribuições de indicadores de performance"""
    
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.setup_logging()
        self.setup_connections()
        self.validation_errors = []
    
    def load_config(self, path):
        with open(path, 'r') as f:
            return json.load(f)
    
    def extract(self):
        """Extrai dados do Google Sheets"""
        logger.info("Iniciando extração do Google Sheets")
        
        service = build('sheets', 'v4', credentials=self.creds)
        result = service.spreadsheets().values().get(
            spreadsheetId=self.config['spreadsheet_id'],
            range=self.config['range_name']
        ).execute()
        
        values = result.get('values', [])
        if len(values) < 10:
            raise ValueError(f"Poucos dados na planilha: {len(values)} linhas")
        
        # Converter para DataFrame
        df = pd.DataFrame(values[1:], columns=values[0])
        logger.info(f"Extraídos {len(df)} registros")
        
        return df
    
    def transform(self, df):
        """Aplica transformações e validações"""
        logger.info("Iniciando transformações")
        
        # T1: Padronização
        df = self.standardize_assignments(df)
        
        # T2: Validação de pesos
        weight_errors = self.validate_weights(df)
        if weight_errors:
            self.validation_errors.extend(weight_errors)
            logger.warning(f"{len(weight_errors)} erros de validação de peso")
        
        # T3: Validação de relacionamentos
        if hasattr(self, 'indicators_df'):
            rel_errors = self.validate_relationships(df, self.indicators_df)
            if rel_errors:
                self.validation_errors.extend(rel_errors)
        
        # T4: Metadados
        df = self.add_metadata(df)
        
        return df
    
    def load(self, df):
        """Carrega dados no Bronze"""
        logger.info("Iniciando carga no Bronze")
        
        # Adicionar colunas de controle
        df['load_timestamp'] = datetime.now()
        df['load_source'] = 'GoogleSheets:m7_performance_assignments'
        
        # Adicionar erros de validação
        if self.validation_errors:
            error_dict = {f"{e['cod_assessor']}_{e.get('valid_from', '')}": e 
                         for e in self.validation_errors}
            df['validation_errors'] = df.apply(
                lambda x: json.dumps(
                    error_dict.get(f"{x['cod_assessor']}_{x['valid_from']}", {})
                ), axis=1
            )
        
        # Carregar no banco
        df.to_sql(
            'performance_assignments',
            self.engine,
            schema='bronze',
            if_exists='append',
            index=False
        )
        
        logger.info(f"Carregados {len(df)} registros no Bronze")
        
        # Executar validações pós-carga
        self.post_load_validations()
        
        return len(df)
    
    def run(self):
        """Executa o pipeline completo"""
        start_time = datetime.now()
        
        try:
            # Extract
            df = self.extract()
            
            # Carregar indicadores para validação
            self.load_indicators()
            
            # Transform
            df = self.transform(df)
            
            # Load
            records_loaded = self.load(df)
            
            # Log sucesso
            self.log_execution(
                status='SUCCESS',
                records=records_loaded,
                duration=(datetime.now() - start_time).seconds
            )
            
            # Notificar se houver erros de validação
            if self.validation_errors:
                self.send_validation_alerts()
            
        except Exception as e:
            logger.error(f"Erro no ETL: {str(e)}")
            self.log_execution(
                status='ERROR',
                error_message=str(e),
                duration=(datetime.now() - start_time).seconds
            )
            raise

if __name__ == "__main__":
    etl = PerformanceAssignmentsETL('config/etl_002_config.json')
    etl.run()
```

### 15.2 Configuração JSON
```json
{
    "spreadsheet_id": "1nm-z2Fbp7pasHx5gmVbm7JPNBRWp4iRElYCbVfEFpOE",
    "range_name": "Página1!A:J",
    "credentials_path": "credentials/google_sheets_api.json",
    "database": {
        "server": "m7-dw-server",
        "database": "M7Medallion",
        "driver": "ODBC Driver 17 for SQL Server"
    },
    "validation": {
        "min_records": 100,
        "max_weight_deviation": 0.01,
        "required_indicator_types": ["CARD", "GATILHO"]
    },
    "notifications": {
        "email_on_error": ["gestao.performance@m7investimentos.com.br"],
        "slack_webhook": "https://hooks.slack.com/..."
    }
}
```

### 15.3 Procedure de Processamento
```sql
CREATE PROCEDURE bronze.prc_process_assignments_to_metadata
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- 1. Validar dados no bronze
        EXEC bronze.prc_validate_assignments_batch @load_id = NULL;
        
        -- 2. Transformar tipos de dados
        WITH transformed AS (
            SELECT 
                cod_assessor,
                nome_assessor,
                indicator_code,
                indicator_type,
                CAST(weight AS DECIMAL(5,2)) as indicator_weight,
                CAST(valid_from AS DATE) as valid_from,
                CAST(valid_to AS DATE) as valid_to,
                created_by,
                approved_by,
                comments,
                load_id,
                load_timestamp
            FROM bronze.performance_assignments
            WHERE is_processed = 0
              AND processing_status IS NULL
        )
        -- 3. Merge com metadados
        MERGE metadados.performance_assignments AS target
        USING transformed AS source
            ON target.cod_assessor = source.cod_assessor
           AND target.indicator_code = source.indicator_code
           AND target.valid_from = source.valid_from
        WHEN MATCHED AND target.valid_to IS NULL THEN
            UPDATE SET 
                indicator_weight = source.indicator_weight,
                modified_date = GETDATE(),
                modified_by = source.created_by
        WHEN NOT MATCHED THEN
            INSERT (cod_assessor, indicator_id, indicator_weight, 
                   valid_from, valid_to, created_by, created_date)
            VALUES (source.cod_assessor, 
                   (SELECT indicator_id FROM metadados.performance_indicators 
                    WHERE indicator_code = source.indicator_code),
                   source.indicator_weight,
                   source.valid_from,
                   source.valid_to,
                   source.created_by,
                   GETDATE());
        
        -- 4. Marcar como processado
        UPDATE bronze.performance_assignments
        SET is_processed = 1,
            processing_date = GETDATE(),
            processing_status = 'SUCCESS'
        WHERE is_processed = 0;
        
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        -- Marcar com erro
        UPDATE bronze.performance_assignments
        SET processing_status = 'ERROR',
            processing_notes = ERROR_MESSAGE()
        WHERE is_processed = 0;
        
        THROW;
    END CATCH
END;
```

### 15.4 Referências
- [Google Sheets API Documentation](https://developers.google.com/sheets/api)
- [ARQ-001 - Arquitetura Performance Tracking]
- [ETL-001 - Performance Indicators]
- [MOD-001 - Modelo Performance Tracking]
- [Processo de Gestão de Metas - RH]

---

**Documento criado por**: Arquitetura de Dados M7 Investimentos  
**Data**: 2025-01-16  
**Revisão**: Trimestral ou sob mudança de estrutura