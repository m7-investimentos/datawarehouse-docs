#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ETL-IND-001 - Extração de Indicadores de Performance do Google Sheets
================================================================================
Tipo: Script ETL
Versão: 1.0.0
Última atualização: 2025-01-17
Autor: bruno.chiaramonti@multisete.com
Revisor: arquitetura.dados@m7investimentos.com.br
Tags: [etl, performance, indicadores, google-sheets, bronze]
Status: produção
Python: 3.8+
================================================================================

OBJETIVO:
    Extrair dados de configuração de indicadores de performance da planilha 
    Google Sheets m7_performance_indicators para a camada Bronze do Data Warehouse,
    permitindo posterior validação e carga na camada de metadados.

CASOS DE USO:
    1. Carga inicial de indicadores de performance
    2. Atualização de indicadores existentes
    3. Sincronização sob demanda de mudanças na planilha

FREQUÊNCIA DE EXECUÇÃO:
    Sob demanda (mudanças são raras)

EXEMPLOS DE USO:
    # Execução básica
    python etl_001_indicators.py
    
    # Com configuração customizada
    python etl_001_indicators.py --config config/etl_001_production.json
    
    # Modo debug com dry-run
    python etl_001_indicators.py --debug --dry-run
"""

# ==============================================================================
# 1. IMPORTS
# ==============================================================================
# Bibliotecas padrão
import os
import sys
import json
import logging
import argparse
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# Bibliotecas de terceiros
try:
    import pandas as pd
    import numpy as np
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    import pyodbc
    from sqlalchemy import create_engine, text
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Erro ao importar biblioteca: {e}")
    print("Execute: pip install -r requirements.txt")
    sys.exit(1)

# ==============================================================================
# 2. CONFIGURAÇÕES E CONSTANTES
# ==============================================================================

# Diretórios
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / 'config'
DATA_DIR = BASE_DIR / 'data'
LOG_DIR = BASE_DIR / 'logs'
CREDENTIALS_DIR = BASE_DIR / 'credentials'

# Carregar variáveis de ambiente do arquivo .env no diretório credentials
load_dotenv(CREDENTIALS_DIR / '.env')

# Configuração de logging
LOG_FORMAT = '[%(asctime)s] [%(levelname)s] [ETL-IND-001] %(message)s'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Criar diretórios se não existirem
for directory in [CONFIG_DIR, DATA_DIR, LOG_DIR, CREDENTIALS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Configuração do Google Sheets
SPREADSHEET_ID = '1h3jC5EpXOv-O1oyL2tBlt9Q16pLHpsoWCHaeNiRHmeY'
RANGE_NAME = 'Página1!A:K'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Categorias e unidades válidas
VALID_CATEGORIES = ['FINANCEIRO', 'QUALIDADE', 'VOLUME', 'COMPORTAMENTAL', 'PROCESSO', 'GATILHO']
VALID_UNITS = ['R$', '%', 'QTD', 'SCORE', 'HORAS', 'DIAS', 'RATIO']
VALID_AGGREGATIONS = ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN', 'LAST', 'CUSTOM']

# ==============================================================================
# 3. CONFIGURAÇÃO DE LOGGING
# ==============================================================================

def setup_logging(log_file: Optional[str] = None) -> logging.Logger:
    """
    Configura o sistema de logging.
    
    Args:
        log_file: Nome do arquivo de log (opcional)
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger('ETL-IND-001')
    logger.setLevel(LOG_LEVEL)
    
    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(console_handler)
    
    # Handler para arquivo
    if log_file:
        file_handler = logging.FileHandler(LOG_DIR / log_file)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(file_handler)
    
    return logger

# ==============================================================================
# 4. CLASSES
# ==============================================================================

class PerformanceIndicatorsETL:
    """
    ETL para extrair indicadores de performance do Google Sheets para Bronze.
    
    Attributes:
        config: Dicionário de configuração
        logger: Logger para registro de eventos
        credentials: Credenciais do Google Service Account
        db_connection: Conexão com banco de dados
    """
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Inicializa o ETL de indicadores.
        
        Args:
            config: Configurações do ETL
            logger: Logger configurado
        """
        self.config = config
        self.logger = logger
        self.credentials = None
        self.db_engine = None
        self.data = None
        self.processed_data = None
        self.validation_errors = []
        
    def setup_connections(self):
        """Configura conexões com Google Sheets e banco de dados."""
        self.logger.info("Configurando conexões...")
        
        # Google Sheets
        try:
            creds_path = self.config.get('google_credentials_path', 
                                        CREDENTIALS_DIR / 'google_sheets_api.json')
            self.credentials = service_account.Credentials.from_service_account_file(
                creds_path, scopes=SCOPES
            )
            self.logger.info("Credenciais Google carregadas com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao carregar credenciais Google: {e}")
            raise
            
        # Banco de dados
        try:
            db_config = self.config['database']
            # Remover chaves extras do driver se existirem
            driver = db_config['driver'].strip('{}')
            
            # Usar urllib para escapar a senha corretamente
            from urllib.parse import quote_plus
            
            # Criar connection string usando ODBC direto (mais confiável)
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={db_config['server']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['user']};"
                f"PWD={db_config['password']};"
                f"TrustServerCertificate=yes"
            )
            
            # URL para SQLAlchemy usando odbc_connect
            connection_string = f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"
            self.db_engine = create_engine(connection_string)
            self.logger.info("Conexão com banco de dados estabelecida")
        except Exception as e:
            self.logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise
            
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(HttpError)
    )
    def extract(self) -> pd.DataFrame:
        """
        Extrai dados do Google Sheets.
        
        Returns:
            DataFrame com os dados extraídos
            
        Raises:
            HttpError: Se houver erro na API do Google
        """
        self.logger.info(f"Iniciando extração de {SPREADSHEET_ID}")
        
        try:
            service = build('sheets', 'v4', credentials=self.credentials)
            sheet = service.spreadsheets()
            
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                raise ValueError("Planilha vazia ou sem dados")
                
            # Converter para DataFrame
            headers = values[0]
            data = values[1:]
            
            self.data = pd.DataFrame(data, columns=headers)
            self.logger.info(f"Extraídos {len(self.data)} indicadores")
            
            return self.data
            
        except HttpError as e:
            if e.resp.status == 404:
                self.logger.error("Planilha não encontrada - verificar ID")
            elif e.resp.status == 403:
                self.logger.error("Sem permissão para acessar a planilha")
            else:
                self.logger.error(f"Erro HTTP: {e}")
            raise
            
    def validate_data(self) -> bool:
        """
        Valida os dados extraídos.
        
        Returns:
            True se os dados são válidos, False caso contrário
        """
        self.logger.info("Validando dados...")
        self.validation_errors = []
        
        # Validar campos obrigatórios
        required_fields = ['indicator_code', 'indicator_name']
        for field in required_fields:
            if field not in self.data.columns:
                self.validation_errors.append(f"Campo obrigatório ausente: {field}")
                
        # Validar registros
        for idx, row in self.data.iterrows():
            # Campos obrigatórios não nulos
            if pd.isna(row.get('indicator_code')) or pd.isna(row.get('indicator_name')):
                self.validation_errors.append(f"Linha {idx+2}: campos obrigatórios vazios")
                
            # Categoria válida
            if row.get('category') and row['category'] not in VALID_CATEGORIES:
                self.logger.warning(f"Linha {idx+2}: categoria inválida '{row['category']}'")
                
            # Unidade válida
            if row.get('unit') and row['unit'] not in VALID_UNITS:
                self.logger.warning(f"Linha {idx+2}: unidade inválida '{row['unit']}'")
                
        if self.validation_errors:
            self.logger.error(f"Encontrados {len(self.validation_errors)} erros de validação")
            for error in self.validation_errors[:5]:  # Mostrar apenas os 5 primeiros
                self.logger.error(f"  - {error}")
            return False
            
        self.logger.info("Dados validados com sucesso")
        return True
        
    def transform(self) -> pd.DataFrame:
        """
        Aplica transformações aos dados.
        
        Returns:
            DataFrame transformado
        """
        self.logger.info("Aplicando transformações...")
        
        df = self.data.copy()
        
        # T1: Padronização de códigos
        df['indicator_code'] = df['indicator_code'].str.upper().str.replace(' ', '_').str.strip()
        
        # T2: Conversão de tipos
        # Booleanos
        bool_map = {'TRUE': 1, 'FALSE': 0, 'true': 1, 'false': 0, '1': 1, '0': 0, '': 0}
        df['is_inverted'] = df.get('is_inverted', '').map(bool_map).fillna(0).astype(int)
        df['is_active'] = df.get('is_active', '').map(bool_map).fillna(1).astype(int)
        
        # Datas
        if 'created_date' in df.columns:
            df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
        
        # Textos vazios
        text_columns = ['formula', 'notes', 'description']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].fillna('')
                
        # Aggregation default
        if 'aggregation' in df.columns:
            df['aggregation'] = df['aggregation'].fillna('CUSTOM')
            
        # T3: Enriquecimento de metadados
        df['row_number'] = range(2, len(df) + 2)  # Número da linha na planilha
        
        # Calcular hash para cada linha
        hash_columns = ['indicator_code', 'indicator_name', 'category', 'unit', 
                       'aggregation', 'formula', 'is_inverted', 'is_active']
        existing_columns = [col for col in hash_columns if col in df.columns]
        
        df['row_hash'] = df[existing_columns].apply(
            lambda x: hashlib.md5(''.join(str(x[col]) for col in existing_columns).encode()).hexdigest(),
            axis=1
        )
        
        self.processed_data = df
        self.logger.info("Transformações aplicadas com sucesso")
        
        return self.processed_data
        
    def load(self, dry_run: bool = False) -> int:
        """
        Carrega dados no Bronze.
        
        Args:
            dry_run: Se True, não executa a carga real
            
        Returns:
            Número de registros carregados
        """
        self.logger.info("Iniciando carga no Bronze...")
        
        if dry_run:
            self.logger.info("Modo dry-run: dados não serão carregados")
            self.logger.info(f"Seriam carregados {len(self.processed_data)} registros")
            return 0
            
        try:
            # Usar autocommit=False para controlar transação manualmente
            conn = self.db_engine.connect()
            trans = conn.begin()
            
            try:
                # Truncate existing data
                self.logger.info("Limpando dados existentes...")
                conn.execute(text("TRUNCATE TABLE bronze.performance_indicators"))
                
                # Preparar dados para carga
                load_data = self.processed_data.copy()
                
                # Adicionar campos de controle
                load_data['load_timestamp'] = datetime.now()
                load_data['load_source'] = f'GoogleSheets:{SPREADSHEET_ID}'
                load_data['is_processed'] = 0
                load_data['processing_date'] = None
                load_data['processing_status'] = None
                load_data['processing_notes'] = None
                
                # Converter tudo para string para Bronze (exceto campos de controle)
                string_columns = ['indicator_code', 'indicator_name', 'category', 'unit',
                                'aggregation', 'formula', 'is_inverted', 'is_active',
                                'description', 'created_date', 'notes']
                
                for col in string_columns:
                    if col in load_data.columns:
                        load_data[col] = load_data[col].astype(str).replace('nan', '')
                
                # Carregar no banco
                load_data.to_sql(
                    'performance_indicators',
                    conn,
                    schema='bronze',
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                records_loaded = len(load_data)
                self.logger.info(f"Carregados {records_loaded} registros no Bronze")
                
                # Commit explícito
                trans.commit()
                self.logger.info("Transação commitada com sucesso")
                
                # Registrar auditoria (fora da transação principal)
                try:
                    self._log_audit(conn, records_loaded, 'SUCCESS')
                except Exception as audit_error:
                    self.logger.warning(f"Erro ao registrar auditoria: {audit_error}")
                
                return records_loaded
                
            except Exception as e:
                trans.rollback()
                self.logger.error(f"Erro durante carga, rollback executado: {e}")
                raise
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Erro durante carga: {e}")
            self._log_audit(None, 0, 'ERROR', str(e))
            raise
            
    def _log_audit(self, conn, records_count: int, status: str, error_msg: str = None):
        """Registra execução na tabela de auditoria."""
        try:
            audit_data = {
                'etl_name': 'ETL-IND-001-performance-indicators',
                'execution_start': self.config.get('start_time', datetime.now()),
                'execution_end': datetime.now(),
                'records_read': len(self.data) if self.data is not None else 0,
                'records_written': records_count,
                'records_error': len(self.validation_errors),
                'status': status,
                'details': error_msg or json.dumps({
                    'spreadsheet_id': SPREADSHEET_ID,
                    'validation_errors': self.validation_errors[:10] if self.validation_errors else []
                })
            }
            
            if conn:
                pd.DataFrame([audit_data]).to_sql(
                    'etl_executions',
                    conn,
                    schema='audit',
                    if_exists='append',
                    index=False
                )
        except Exception as e:
            self.logger.warning(f"Erro ao registrar auditoria: {e}")
            
    def run_post_load_validation(self):
        """Executa validações pós-carga."""
        self.logger.info("Executando validações pós-carga...")
        
        with self.db_engine.connect() as conn:
            # Verificar códigos únicos
            result = conn.execute(text("""
                SELECT COUNT(*) total, COUNT(DISTINCT indicator_code) unicos
                FROM bronze.performance_indicators
                WHERE load_timestamp = (SELECT MAX(load_timestamp) FROM bronze.performance_indicators)
            """)).fetchone()
            
            if result.total != result.unicos:
                self.logger.warning(f"Códigos duplicados detectados: {result.total - result.unicos}")
                
            # Verificar fórmulas não vazias
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) total,
                    COUNT(CASE WHEN formula != '' AND formula != 'nan' THEN 1 END) com_formula
                FROM bronze.performance_indicators
                WHERE load_timestamp = (SELECT MAX(load_timestamp) FROM bronze.performance_indicators)
            """)).fetchone()
            
            formula_percent = (result.com_formula / result.total * 100) if result.total > 0 else 0
            self.logger.info(f"Indicadores com fórmula: {formula_percent:.1f}%")
            
            if formula_percent < 90:
                self.logger.warning("Menos de 90% dos indicadores têm fórmula definida")
                
    def run(self, dry_run: bool = False):
        """
        Executa o pipeline completo.
        
        Args:
            dry_run: Se True, não executa a carga real
        """
        start_time = datetime.now()
        self.config['start_time'] = start_time
        
        try:
            self.logger.info("="*60)
            self.logger.info("Iniciando ETL-IND-001 - Performance Indicators")
            self.logger.info(f"Timestamp: {start_time}")
            self.logger.info("="*60)
            
            # Setup
            self.setup_connections()
            
            # Extract
            self.extract()
            
            # Validate
            if not self.validate_data():
                raise ValueError("Falha na validação dos dados")
                
            # Transform
            self.transform()
            
            # Load
            records_loaded = self.load(dry_run)
            
            # Post-load validation
            if not dry_run and records_loaded > 0:
                self.run_post_load_validation()
                
            self.logger.info("="*60)
            self.logger.info("ETL-IND-001 concluído com sucesso!")
            self.logger.info(f"Tempo de execução: {datetime.now() - start_time}")
            self.logger.info("="*60)
            
        except Exception as e:
            self.logger.error("="*60)
            self.logger.error(f"ETL-IND-001 falhou: {e}")
            self.logger.error("="*60)
            raise

# ==============================================================================
# 5. FUNÇÕES AUXILIARES
# ==============================================================================

def parse_arguments() -> argparse.Namespace:
    """
    Define e processa argumentos da linha de comando.
    
    Returns:
        Namespace com os argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description='ETL-IND-001 - Extração de Indicadores de Performance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default=str(CONFIG_DIR / 'etl_001_config.json'),
        help='Arquivo de configuração (default: config/etl_001_config.json)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Ativa modo debug com logs detalhados'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Executa sem carregar dados no banco'
    )
    
    parser.add_argument(
        '--force-reload',
        action='store_true',
        help='Força recarga mesmo se dados não mudaram'
    )
    
    return parser.parse_args()

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Carrega configuração do arquivo JSON.
    
    Args:
        config_path: Caminho do arquivo de configuração
        
    Returns:
        Dicionário de configuração
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        # Substituir variáveis de ambiente (usando os nomes corretos do .env)
        config['database']['server'] = os.getenv('DB_SERVER', config['database']['server'])
        config['database']['database'] = os.getenv('DB_DATABASE', config['database']['database'])
        config['database']['user'] = os.getenv('DB_USERNAME', config['database']['user'])
        config['database']['password'] = os.getenv('DB_PASSWORD', config['database']['password'])
        
        # Driver ODBC (caso esteja no .env)
        if os.getenv('DB_DRIVER'):
            config['database']['driver'] = os.getenv('DB_DRIVER')
        
        return config
        
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Erro ao parsear arquivo de configuração: {e}")

# ==============================================================================
# 6. FUNÇÃO PRINCIPAL
# ==============================================================================

def main():
    """Função principal do script."""
    # Parse argumentos
    args = parse_arguments()
    
    # Configurar logging
    log_filename = f"ETL-IND-001_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = setup_logging(log_filename)
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        
    try:
        # Carregar configuração
        config = load_config(args.config)
        config['force_reload'] = args.force_reload
        
        # Criar e executar ETL
        etl = PerformanceIndicatorsETL(config, logger)
        etl.run(args.dry_run)
        
    except KeyboardInterrupt:
        logger.warning("Execução interrompida pelo usuário")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        sys.exit(1)

# ==============================================================================
# 7. EXECUÇÃO
# ==============================================================================

if __name__ == '__main__':
    main()