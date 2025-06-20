#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL-IND-001: ETL de Indicadores de Performance (Google Sheets → Bronze) - V2.0.0
================================================================================
Versão: 2.0.0
Última atualização: 2025-01-20
Autor: bruno.chiaramonti@multisete.com

Descrição: 
-----------
ETL responsável por extrair indicadores de performance do Google Sheets
e carregar na camada Bronze do Data Warehouse.

NOVA FUNCIONALIDADE V2.0.0:
- Detecta mudanças nos dados e força reprocessamento quando necessário
- Compara com dados existentes no Silver antes de carregar

Fonte: Google Sheets (m7_performance_indicators)
Destino: bronze.performance_indicators
"""

import os
import sys
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pyodbc
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import warnings
warnings.filterwarnings('ignore')

# Google Sheets API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================

# IDs e Ranges do Google Sheets
SPREADSHEET_ID = '18AJfFeOOKNvCEdz9qS6xAJNUKoAzJAE1QCa7KK88vQE'
RANGE_NAME = 'Indicators!A:Z'

# Caminhos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
CREDENTIALS_DIR = os.path.join(BASE_DIR, 'credentials')
LOG_DIR = os.path.join(BASE_DIR, 'logs')

# Arquivos
CONFIG_FILE = os.path.join(CONFIG_DIR, 'etl_001_config.json')
CREDENTIALS_FILE = os.path.join(CREDENTIALS_DIR, 'google_sheets_api.json')

# Criar diretórios se não existirem
os.makedirs(LOG_DIR, exist_ok=True)

# Configuração de Logging
log_filename = os.path.join(LOG_DIR, f"ETL-IND-001_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Valores válidos para validação
VALID_CATEGORIES = ['FINANCEIRO', 'QUALIDADE', 'VOLUME', 'COMPORTAMENTAL', 'PROCESSO', 'GATILHO']
VALID_UNITS = ['R$', '%', 'QTD', 'SCORE', 'HORAS', 'DIAS', 'RATIO']
VALID_AGGREGATIONS = ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN', 'LAST', 'CUSTOM']

# ==============================================================================
# CLASSE ETL
# ==============================================================================

class IndicatorsETL:
    """ETL para carregar indicadores de performance do Google Sheets para Bronze."""
    
    def __init__(self, config_file: str = CONFIG_FILE):
        """
        Inicializa o ETL.
        
        Args:
            config_file: Caminho para arquivo de configuração
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = self._load_config(config_file)
        self.credentials = None
        self.db_engine = None
        self.data = None
        self.processed_data = None
        self.validation_errors = []
        self.existing_silver_data = None  # V2.0.0: Para comparação
        
    def _load_config(self, config_file: str) -> Dict:
        """Carrega configurações do arquivo JSON."""
        if not os.path.exists(config_file):
            self.logger.warning(f"Arquivo de configuração não encontrado: {config_file}")
            return {}
            
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Erro ao carregar configuração: {e}")
            return {}
            
    def setup_credentials(self):
        """Configura credenciais do Google Sheets."""
        self.logger.info("Configurando credenciais do Google Sheets...")
        
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {CREDENTIALS_FILE}")
            
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                CREDENTIALS_FILE,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            self.logger.info("Credenciais configuradas com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao configurar credenciais: {e}")
            raise
            
    def setup_database(self):
        """Configura conexão com banco de dados."""
        self.logger.info("Configurando conexão com banco de dados...")
        
        # Buscar configurações do ambiente ou config
        db_config = {
            'server': os.getenv('DB_SERVER', self.config.get('database', {}).get('server', '172.17.0.10')),
            'database': os.getenv('DB_DATABASE', self.config.get('database', {}).get('database', 'M7Medallion')),
            'username': os.getenv('DB_USERNAME', self.config.get('database', {}).get('username', 'm7invest')),
            'password': os.getenv('DB_PASSWORD', self.config.get('database', {}).get('password', '!@Multi19732846')),
            'driver': os.getenv('DB_DRIVER', self.config.get('database', {}).get('driver', 'ODBC Driver 17 for SQL Server'))
        }
        
        try:
            conn_str = (
                f"DRIVER={{{db_config['driver']}}};"
                f"SERVER={db_config['server']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['username']};"
                f"PWD={db_config['password']};"
                f"TrustServerCertificate=yes"
            )
            
            connection_string = f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"
            self.db_engine = create_engine(connection_string)
            self.logger.info("Conexão com banco de dados estabelecida")
        except Exception as e:
            self.logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise
            
    def load_existing_silver_data(self):
        """
        V2.0.0: Carrega dados existentes do Silver para comparação.
        """
        self.logger.info("Carregando dados existentes do Silver para comparação...")
        
        try:
            query = """
            SELECT 
                indicator_code,
                indicator_name,
                category,
                unit,
                aggregation_method,
                calculation_formula,
                is_inverted,
                is_active,
                version
            FROM silver.performance_indicators
            WHERE valid_to IS NULL  -- Apenas registros atuais
            """
            
            self.existing_silver_data = pd.read_sql(query, self.db_engine)
            self.logger.info(f"Carregados {len(self.existing_silver_data)} indicadores do Silver")
            
        except Exception as e:
            self.logger.warning(f"Erro ao carregar dados do Silver (pode não existir ainda): {e}")
            self.existing_silver_data = pd.DataFrame()
            
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
            
    def detect_changes(self) -> List[str]:
        """
        V2.0.0: Detecta quais indicadores tiveram mudanças.
        
        Returns:
            Lista de indicator_codes que mudaram
        """
        if self.existing_silver_data.empty:
            self.logger.info("Sem dados no Silver - todos os indicadores serão processados")
            return self.data['indicator_code'].tolist()
            
        changed_indicators = []
        
        for _, row in self.data.iterrows():
            indicator_code = row.get('indicator_code', '')
            if not indicator_code:
                continue
                
            # Buscar no Silver
            existing = self.existing_silver_data[
                self.existing_silver_data['indicator_code'] == indicator_code
            ]
            
            if existing.empty:
                # Novo indicador
                self.logger.info(f"Novo indicador detectado: {indicator_code}")
                changed_indicators.append(indicator_code)
            else:
                # Comparar campos importantes
                existing_row = existing.iloc[0]
                
                # Lista de campos para comparar
                fields_to_compare = [
                    ('indicator_name', 'indicator_name'),
                    ('category', 'category'),
                    ('unit', 'unit'),
                    ('aggregation', 'aggregation_method'),
                    ('formula', 'calculation_formula'),
                    ('is_inverted', 'is_inverted'),
                    ('is_active', 'is_active')
                ]
                
                for sheet_field, db_field in fields_to_compare:
                    sheet_value = str(row.get(sheet_field, '')).strip()
                    db_value = str(existing_row.get(db_field, '')).strip()
                    
                    # Normalizar valores booleanos
                    if sheet_field in ['is_inverted', 'is_active']:
                        sheet_value = '1' if sheet_value.upper() in ['TRUE', '1', 'SIM', 'YES'] else '0'
                        db_value = '1' if str(db_value) == '1' or str(db_value).upper() == 'TRUE' else '0'
                    
                    if sheet_value != db_value:
                        self.logger.info(
                            f"Mudança detectada em {indicator_code}.{sheet_field}: "
                            f"'{db_value}' → '{sheet_value}'"
                        )
                        changed_indicators.append(indicator_code)
                        break
                        
        # Remover duplicatas
        changed_indicators = list(set(changed_indicators))
        
        if changed_indicators:
            self.logger.info(f"Total de {len(changed_indicators)} indicadores com mudanças")
        else:
            self.logger.info("Nenhuma mudança detectada nos indicadores")
            
        return changed_indicators
        
    def force_reprocess_changed_indicators(self, changed_indicators: List[str]):
        """
        V2.0.0: Força reprocessamento dos indicadores que mudaram.
        
        Args:
            changed_indicators: Lista de indicator_codes que mudaram
        """
        if not changed_indicators:
            return
            
        self.logger.info(f"Forçando reprocessamento de {len(changed_indicators)} indicadores...")
        
        try:
            # Criar lista formatada para SQL IN clause
            indicators_list = "','".join(changed_indicators)
            
            query = f"""
            UPDATE bronze.performance_indicators
            SET is_processed = 0
            WHERE indicator_code IN ('{indicators_list}')
              AND load_id = (
                  SELECT MAX(load_id) 
                  FROM bronze.performance_indicators
              )
            """
            
            with self.db_engine.connect() as conn:
                result = conn.execute(text(query))
                rows_updated = result.rowcount
                conn.commit()
                
            self.logger.info(f"Marcados {rows_updated} registros para reprocessamento")
            
        except Exception as e:
            self.logger.error(f"Erro ao marcar indicadores para reprocessamento: {e}")
            # Não falhar o ETL por isso
            
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
        
        # Copiar dados
        df = self.data.copy()
        
        # Remover linhas vazias
        df = df.dropna(subset=['indicator_code', 'indicator_name'])
        
        # Padronizar valores booleanos
        bool_columns = ['is_inverted', 'is_active']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: 
                    '1' if str(x).upper() in ['TRUE', '1', 'SIM', 'YES'] else '0'
                )
        
        # Gerar hash do registro para detecção de mudanças
        df['record_hash'] = df.apply(
            lambda row: pd.util.hash_pandas_object(
                row[['indicator_code', 'indicator_name', 'category', 'unit']], 
                index=False
            )[0], 
            axis=1
        ).astype(str)
        
        # Adicionar ID único
        df['load_id'] = int(datetime.now().timestamp())
        
        self.processed_data = df
        self.logger.info(f"Transformações aplicadas. {len(df)} registros prontos para carga")
        
        return df
        
    def load(self, dry_run: bool = False) -> int:
        """
        Carrega dados no Bronze.
        
        Args:
            dry_run: Se True, não executa a carga
            
        Returns:
            Número de registros carregados
        """
        if self.processed_data is None or self.processed_data.empty:
            self.logger.warning("Nenhum dado para carregar")
            return 0
            
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
                                'description', 'notes']
                
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
                
                # V2.0.0: Detectar e forçar reprocessamento de mudanças
                changed_indicators = self.detect_changes()
                if changed_indicators:
                    self.force_reprocess_changed_indicators(changed_indicators)
                
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
                    'validation_errors': self.validation_errors[:10] if self.validation_errors else [],
                    'version': '2.0.0'
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
        
        try:
            # Verificar se os dados foram carregados
            query = "SELECT COUNT(*) as total FROM bronze.performance_indicators WHERE load_id = (SELECT MAX(load_id) FROM bronze.performance_indicators)"
            result = pd.read_sql(query, self.db_engine)
            
            total_loaded = result.iloc[0]['total']
            self.logger.info(f"Validação pós-carga: {total_loaded} registros encontrados no Bronze")
            
            if total_loaded != len(self.processed_data):
                self.logger.warning(f"Divergência: esperados {len(self.processed_data)}, encontrados {total_loaded}")
                
        except Exception as e:
            self.logger.error(f"Erro na validação pós-carga: {e}")
            
    def execute_silver_procedure(self):
        """
        V2.0.0: Executa a procedure Bronze to Silver após a carga.
        """
        self.logger.info("Executando procedure Bronze to Silver...")
        
        try:
            with self.db_engine.connect() as conn:
                # Executar procedure
                conn.execute(text("""
                    EXEC bronze.prc_process_indicators_to_silver 
                        @load_id = NULL,
                        @validate_only = 0,
                        @debug_mode = 1
                """))
                conn.commit()
                
            self.logger.info("Procedure Bronze to Silver executada com sucesso")
            
        except Exception as e:
            self.logger.error(f"Erro ao executar procedure Bronze to Silver: {e}")
            # Não falhar o ETL por isso
    
    def run(self, dry_run: bool = False) -> bool:
        """
        Executa o pipeline ETL completo.
        
        Args:
            dry_run: Se True, não executa a carga
            
        Returns:
            True se executado com sucesso, False caso contrário
        """
        self.config['start_time'] = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info("INICIANDO ETL-IND-001: Performance Indicators (V2.0.0)")
        self.logger.info("=" * 80)
        
        try:
            # 1. Setup
            self.setup_credentials()
            self.setup_database()
            
            # 2. V2.0.0: Carregar dados existentes do Silver
            self.load_existing_silver_data()
            
            # 3. Extract
            self.extract()
            
            # 4. Validate
            if not self.validate_data():
                if not self.config.get('force_load_on_validation_error', False):
                    self.logger.error("Validação falhou. ETL abortado.")
                    return False
                self.logger.warning("Validação falhou mas force_load está ativo. Continuando...")
            
            # 5. Transform
            self.transform()
            
            # 6. Load
            records_loaded = self.load(dry_run)
            
            if not dry_run and records_loaded > 0:
                # 7. Post-load validation
                self.run_post_load_validation()
                
                # 8. V2.0.0: Executar procedure Bronze to Silver
                self.execute_silver_procedure()
            
            self.logger.info("=" * 80)
            self.logger.info("ETL CONCLUÍDO COM SUCESSO")
            self.logger.info("=" * 80)
            return True
            
        except Exception as e:
            self.logger.error(f"Erro durante execução do ETL: {e}")
            self.logger.error("=" * 80)
            self.logger.error("ETL FALHOU")
            self.logger.error("=" * 80)
            return False

# ==============================================================================
# MAIN
# ==============================================================================

def main():
    """Função principal."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='ETL de Indicadores de Performance (Google Sheets → Bronze) V2.0.0'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Executa sem carregar dados (validação apenas)'
    )
    parser.add_argument(
        '--config',
        default=CONFIG_FILE,
        help='Arquivo de configuração customizado'
    )
    
    args = parser.parse_args()
    
    # Executar ETL
    etl = IndicatorsETL(config_file=args.config)
    success = etl.run(dry_run=args.dry_run)
    
    # Retornar código de saída apropriado
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()