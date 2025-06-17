# ETL-003-performance-indicators-extraction

---
título: Extração de Indicadores de Performance - Google Sheets para Bronze
tipo: ETL - Processo ETL
versão: 1.0.0
última_atualização: 2025-01-16
autor: arquitetura.dados@m7investimentos.com.br
aprovador: diretoria.ti@m7investimentos.com.br
tags: [etl, performance, indicadores, google-sheets, bronze, metadados]
status: aprovado
dependências:
  - tipo: modelo
    ref: [MOD-001]
    repo: datawarehouse-docs
  - tipo: planilha
    ref: [m7_performance_indicators]
    repo: google-sheets
---

## 1. Objetivo

Extrair dados de configuração de indicadores de performance da planilha Google Sheets `m7_performance_indicators` para a camada Bronze do Data Warehouse, permitindo posterior validação e carga na camada de metadados.

## 2. Escopo e Aplicabilidade

### 2.1 Escopo
- **Fonte de dados**: Google Sheets - m7_performance_indicators
- **Destino**: M7Medallion.bronze.performance_indicators
- **Volume esperado**: ~10-50 registros
- **Frequência**: Sob demanda (mudanças são raras)

### 2.2 Fora de Escopo
- Validação complexa de fórmulas SQL (feita na transformação Bronze → Metadados)
- Processamento de outras planilhas (assignments, targets)
- Execução das fórmulas de cálculo

## 3. Pré-requisitos e Dependências

### 3.1 Técnicos
- **Conectividade**: 
  - Google Sheets API v4 habilitada
  - Service Account com permissões de leitura
  - Credenciais JSON armazenadas seguramente
- **Recursos computacionais**: Mínimos (< 1MB de dados)
- **Software/Ferramentas**: 
  - Python 3.8+
  - google-api-python-client
  - pandas
  - pyodbc ou sqlalchemy

### 3.2 Negócio
- **Aprovações necessárias**: Gestão de Performance deve aprovar mudanças
- **Janelas de execução**: Qualquer horário (baixo impacto)
- **SLAs dependentes**: Nenhum crítico

## 4. Arquitetura do Pipeline

### 4.1 Diagrama de Fluxo
```
[Google Sheets] ─→ [API Extract] ─→ [Validation] ─→ [Transform] ─→ [Bronze Load] ─→ [Audit Log]
                                           ↓
                                    [Quarantine]
```

### 4.2 Componentes
| Componente | Tecnologia | Função | Configuração |
|------------|------------|--------|--------------|
| Extractor | Google Sheets API v4 | Ler dados da planilha | credentials.json |
| Validator | Python/Pandas | Validar estrutura e tipos | validation_rules.py |
| Loader | PyODBC/SQLAlchemy | Inserir em Bronze | connection_string |

## 5. Processo de Extração

### 5.1 Fontes de Dados

#### Fonte: Google Sheets - m7_performance_indicators
- **Tipo**: Google Sheets via API
- **ID da Planilha**: `1WUNRULdlREtaD817VdNM4zToZu8RtRw-kz7-ffHHIWo`
- **Range**: `'Página1!A:K'` (todas as colunas)
- **Conexão**: 
  ```python
  # Exemplo de configuração
  SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
  SERVICE_ACCOUNT_FILE = 'path/to/credentials.json'
  SPREADSHEET_ID = '1WUNRULdlREtaD817VdNM4zToZu8RtRw-kz7-ffHHIWo'
  ```

### 5.2 Estratégia de Extração
- **Tipo**: Full (sempre lê toda a planilha)
- **Controle de watermark**: timestamp de extração
- **Paralelização**: Não aplicável (volume pequeno)

## 6. Processo de Transformação

### 6.1 Limpeza de Dados
| Validação | Regra | Ação se Falha |
|-----------|-------|---------------|
| Campos obrigatórios | indicator_code, indicator_name NOT NULL | Quarentena |
| Formato indicator_code | UPPER_CASE, sem espaços, max 50 chars | Ajustar automático |
| Valores boolean | is_inverted, is_active in (TRUE, FALSE, 1, 0) | Converter para BIT |
| Category válida | IN ('FINANCEIRO', 'QUALIDADE', 'VOLUME', 'COMPORTAMENTAL', 'PROCESSO', 'GATILHO') | Log warning |
| Unit válida | IN ('R$', '%', 'QTD', 'SCORE', 'HORAS', 'DIAS', 'RATIO') | Log warning |
| Aggregation válida | IN ('SUM', 'AVG', 'COUNT', 'MAX', 'MIN', 'LAST', 'CUSTOM') | Default 'CUSTOM' |

### 6.2 Transformações Aplicadas

#### T1: Padronização de Códigos
```python
def standardize_indicator_code(df):
    df['indicator_code'] = df['indicator_code'].str.upper()
    df['indicator_code'] = df['indicator_code'].str.replace(' ', '_')
    df['indicator_code'] = df['indicator_code'].str.strip()
    return df
```

#### T2: Conversão de Tipos
```python
def convert_types(df):
    # Booleanos
    bool_map = {'TRUE': 1, 'FALSE': 0, 'true': 1, 'false': 0, '1': 1, '0': 0}
    df['is_inverted'] = df['is_inverted'].map(bool_map).fillna(0)
    df['is_active'] = df['is_active'].map(bool_map).fillna(1)
    
    # Datas
    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
    
    # Textos
    df['formula'] = df['formula'].fillna('')
    df['notes'] = df['notes'].fillna('')
    
    return df
```

#### T3: Enriquecimento de Metadados
```python
def add_metadata(df):
    df['extraction_timestamp'] = datetime.now()
    df['source_file'] = 'google_sheets:m7_performance_indicators'
    df['row_hash'] = df.apply(lambda x: hashlib.md5(
        ''.join(str(x[col]) for col in df.columns).encode()
    ).hexdigest(), axis=1)
    return df
```

## 7. Processo de Carga

### 7.1 Destino
- **Sistema**: SQL Server - M7Medallion
- **Schema.Tabela**: bronze.performance_indicators
- **Método de carga**: TRUNCATE + INSERT (substitui tudo)

### 7.2 Estrutura da Tabela Bronze
```sql
CREATE TABLE bronze.performance_indicators (
    load_id INT IDENTITY(1,1) PRIMARY KEY,
    load_timestamp DATETIME NOT NULL DEFAULT GETDATE(),
    load_source VARCHAR(200) NOT NULL DEFAULT 'GoogleSheets:m7_performance_indicators',
    
    -- Campos da planilha (todos VARCHAR para aceitar qualquer entrada)
    indicator_code VARCHAR(MAX),
    indicator_name VARCHAR(MAX),
    category VARCHAR(MAX),
    unit VARCHAR(MAX),
    aggregation VARCHAR(MAX),
    formula VARCHAR(MAX),
    is_inverted VARCHAR(MAX),
    is_active VARCHAR(MAX),
    description VARCHAR(MAX),
    created_date VARCHAR(MAX),
    notes VARCHAR(MAX),
    
    -- Metadados de controle
    row_number INT,
    row_hash VARCHAR(32),
    is_processed BIT DEFAULT 0,
    processing_date DATETIME NULL,
    processing_status VARCHAR(50) NULL,
    processing_notes VARCHAR(MAX) NULL
);
```

## 8. Tratamento de Erros

### 8.1 Tipos de Erro e Ações
| Tipo de Erro | Detecção | Ação | Notificação |
|--------------|----------|------|-------------|
| Google Sheets indisponível | API timeout/401/403 | Retry 3x com backoff | Email + Log |
| Planilha alterada | Colunas faltando | Parar execução | Email urgente |
| Código duplicado | indicator_code repetido | Log e continuar | Warning log |
| Fórmula SQL inválida | Parsing básico | Aceitar mas marcar | Flag para revisão |

### 8.2 Processo de Retry
```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    retry=retry_if_exception_type(HttpError)
)
def extract_from_sheets():
    # Código de extração
    pass
```

## 9. Monitoramento e Auditoria

### 9.1 Métricas de Performance
| Métrica | Threshold | Alerta |
|---------|-----------|--------|
| Tempo total execução | < 1 minuto | > 2 minutos |
| Registros processados | > 0 | = 0 |
| Taxa de erro | < 5% | > 10% |
| Latência API Google | < 5 seg | > 10 seg |

### 9.2 Logs
```python
# Formato de log
logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] [ETL-003] %(message)s',
    level=logging.INFO
)

# Exemplo de uso
logger.info(f"Iniciando extração de {SPREADSHEET_ID}")
logger.info(f"Total de indicadores extraídos: {len(df)}")
logger.warning(f"Indicador {code} com categoria inválida: {category}")
```

### 9.3 Auditoria
```sql
-- Tabela de controle de execuções
INSERT INTO audit.etl_executions (
    etl_name,
    execution_start,
    execution_end,
    records_read,
    records_written,
    records_error,
    status,
    details
) VALUES (
    'ETL-003-performance-indicators',
    @start_time,
    @end_time,
    @total_read,
    @total_written,
    @total_error,
    @status,
    @execution_log
);
```

## 10. Qualidade de Dados

### 10.1 Validações Pós-Carga
| Validação | Query/Método | Threshold | Ação se Falha |
|-----------|--------------|-----------|---------------|
| Códigos únicos | COUNT(DISTINCT indicator_code) = COUNT(*) | 100% | Investigar duplicatas |
| Fórmulas não vazias | COUNT(*) WHERE formula IS NOT NULL | > 90% | Alertar gestão |
| Categorias válidas | COUNT(*) WHERE category IN (lista) | 100% | Revisar novos valores |
| Hash único | COUNT(DISTINCT row_hash) = COUNT(*) | 100% | Verificar mudanças |

### 10.2 Script de Validação
```sql
-- Executar após cada carga
EXEC bronze.prc_validate_performance_indicators_load
    @load_timestamp = @current_load_timestamp,
    @expected_count = 6,
    @raise_error = 1;
```

## 11. Agendamento e Triggers

### 11.1 Schedule
- **Ferramenta**: SQL Server Agent / Airflow
- **Frequência**: Sob demanda (triggered)
- **Triggers**: 
  - Manual via procedure
  - Webhook do Google Sheets (se configurado)
  - Antes do processamento mensal de metas

### 11.2 Comando de Execução Manual
```sql
-- Executar ETL manualmente
EXEC bronze.prc_extract_performance_indicators 
    @force_reload = 1,
    @debug_mode = 0;
```

## 12. Manutenção e Operação

### 12.1 Procedimentos Operacionais
- **Re-extração**: Executar procedure com @force_reload = 1
- **Limpeza**: Bronze mantém últimas 10 cargas
- **Arquivamento**: Após 90 dias, mover para bronze_archive

### 12.2 Troubleshooting Comum
| Problema | Sintoma | Diagnóstico | Solução |
|----------|---------|-------------|---------|
| API limite excedido | HTTP 429 | Check quota Google | Aguardar reset |
| Credenciais expiradas | HTTP 401 | Verificar service account | Renovar credenciais |
| Planilha movida/deletada | HTTP 404 | Verificar ID da planilha | Atualizar configuração |
| Novos indicadores não aparecem | Count não aumenta | Check is_active | Verificar filtros |

## 13. Segurança e Compliance

### 13.1 Classificação de Dados
- **Nível de sensibilidade**: Interno
- **PII/PCI**: Não contém

### 13.2 Controles de Segurança
- **Autenticação**: Service Account Google
- **Criptografia em trânsito**: HTTPS/TLS 1.2+
- **Acesso**: Read-only na planilha
- **Auditoria**: Todos os acessos logados

## 14. Versionamento e Mudanças

### 14.1 Controle de Versão
- **Script Python**: `/etl/performance/etl_003_indicators.py`
- **Stored Procedures**: Version tracking em comentários

### 14.2 Processo de Mudança
1. Testar em ambiente dev com planilha de teste
2. Validar com equipe de Performance
3. Deploy em produção com rollback plan
4. Monitorar primeira execução

## 15. Anexos

### 15.1 Script Python Completo
```python
# /etl/performance/etl_003_indicators.py
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import logging
from datetime import datetime
import hashlib
import pyodbc

class PerformanceIndicatorsETL:
    def __init__(self, config):
        self.config = config
        self.setup_logging()
        self.setup_connections()
    
    def extract(self):
        """Extrai dados do Google Sheets"""
        # Implementação da extração
        pass
    
    def transform(self):
        """Aplica transformações necessárias"""
        # Implementação das transformações
        pass
    
    def load(self):
        """Carrega dados no Bronze"""
        # Implementação da carga
        pass
    
    def run(self):
        """Executa o pipeline completo"""
        try:
            self.extract()
            self.transform()
            self.load()
            self.log_success()
        except Exception as e:
            self.log_error(e)
            raise

if __name__ == "__main__":
    etl = PerformanceIndicatorsETL(config)
    etl.run()
```

### 15.2 Procedure de Carga
```sql
CREATE PROCEDURE bronze.prc_process_indicators_to_metadata
AS
BEGIN
    -- Validar dados no bronze
    -- Fazer merge com metadados.performance_indicators
    -- Marcar bronze como processado
    -- Registrar auditoria
END
```

### 15.3 Referências
- [Google Sheets API Documentation](https://developers.google.com/sheets/api)
- [MOD-001 - Modelo Performance Tracking]
- [Python ETL Best Practices]

---

**Documento criado por**: Arquitetura de Dados M7 Investimentos  
**Data**: 2025-01-16  
**Revisão**: Mensal ou sob demanda