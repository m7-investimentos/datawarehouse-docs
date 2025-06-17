#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ETL-IND-002 - Extração de Atribuições de Performance do Google Sheets
================================================================================
Tipo: Script ETL
Versão: 1.0.0
Última atualização: 2025-01-17
Autor: bruno.chiaramonti@multisete.com
Revisor: arquitetura.dados@m7investimentos.com.br
Tags: [etl, performance, assignments, google-sheets, bronze]
Status: produção
Python: 3.8+
================================================================================

OBJETIVO:
    Extrair dados de atribuições de indicadores de performance por assessor 
    da planilha Google Sheets m7_performance_assignments para a camada Bronze 
    do Data Warehouse, incluindo validações de integridade de pesos e 
    relacionamentos.

CASOS DE USO:
    1. Carga inicial de atribuições de indicadores
    2. Atualização de pesos e vigências de indicadores
    3. Sincronização sob demanda de mudanças trimestrais

FREQUÊNCIA DE EXECUÇÃO:
    Diária ou sob demanda (mudanças são trimestrais)

EXEMPLOS DE USO:
    # Execução básica
    python etl_002_assignments.py
    
    # Com configuração customizada
    python etl_002_assignments.py --config config/etl_002_production.json
    
    # Modo debug com dry-run
    python etl_002_assignments.py --debug --dry-run
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
from decimal import Decimal

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

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração de logging
LOG_FORMAT = '[%(asctime)s] [%(levelname)s] [ETL-IND-002] %(message)s'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Diretórios
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / 'config'
DATA_DIR = BASE_DIR / 'data'
LOG_DIR = BASE_DIR / 'logs'
CREDENTIALS_DIR = BASE_DIR / 'credentials'

# Criar diretórios se não existirem
for directory in [CONFIG_DIR, DATA_DIR, LOG_DIR, CREDENTIALS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Configuração do Google Sheets
SPREADSHEET_ID = '1nm-z2Fbp7pasHx5gmVbm7JPNBRWp4iRElYCbVfEFpOE'
RANGE_NAME = 'Página1!A:J'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Tipos e validações válidas
VALID_INDICATOR_TYPES = ['CARD', 'GATILHO', 'KPI', 'PPI', 'METRICA']
MIN_EXPECTED_RECORDS = 100
MAX_WEIGHT_DEVIATION = 0.01  # 0.01% de tolerância

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
    logger = logging.getLogger('ETL-IND-002')
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

class PerformanceAssignmentsETL:
    """
    ETL para extrair atribuições de performance do Google Sheets para Bronze.
    
    Attributes:
        config: Dicionário de configuração
        logger: Logger para registro de eventos
        credentials: Credenciais do Google Service Account
        db_engine: Engine de conexão com banco de dados
    """
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Inicializa o ETL de atribuições.
        
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
        self.indicators_df = None
        
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
            
            # Criar connection string usando ODBC direto
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
            
    def load_indicators(self):
        """Carrega indicadores existentes para validação."""
        try:
            with self.db_engine.connect() as conn:
                query = text("""
                    SELECT DISTINCT indicator_code 
                    FROM bronze.performance_indicators
                    WHERE is_active = '1'
                """)
                result = conn.execute(query)
                self.indicators_df = pd.DataFrame(result.fetchall(), columns=['indicator_code'])
                self.logger.info(f"Carregados {len(self.indicators_df)} indicadores para validação")
        except Exception as e:
            self.logger.warning(f"Erro ao carregar indicadores: {e}")
            self.indicators_df = pd.DataFrame(columns=['indicator_code'])
            
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
            ValueError: Se os dados estiverem abaixo do mínimo esperado
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
            
            if len(values) < 10:
                raise ValueError(f"Poucos dados na planilha: {len(values)} linhas (mínimo: 10)")
                
            # Converter para DataFrame
            headers = values[0]
            data = values[1:]
            
            self.data = pd.DataFrame(data, columns=headers)
            self.logger.info(f"Extraídos {len(self.data)} registros de atribuições")
            
            return self.data
            
        except HttpError as e:
            if e.resp.status == 404:
                self.logger.error("Planilha não encontrada - verificar ID")
            elif e.resp.status == 403:
                self.logger.error("Sem permissão para acessar a planilha")
            else:
                self.logger.error(f"Erro HTTP: {e}")
            raise
            
    def standardize_assignments(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Padroniza códigos e tipos de dados.
        
        Args:
            df: DataFrame com dados brutos
            
        Returns:
            DataFrame padronizado
        """
        # Padronizar cod_assessor
        df['cod_assessor'] = df['cod_assessor'].str.upper().str.strip()
        
        # Padronizar indicator_code
        df['indicator_code'] = df['indicator_code'].str.upper().str.replace(' ', '_').str.strip()
        
        # Padronizar indicator_type
        df['indicator_type'] = df['indicator_type'].str.upper().str.strip()
        
        # Converter weight para numérico
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(0.0)
        
        # Para não-CARD, garantir weight = 0
        df.loc[df['indicator_type'] != 'CARD', 'weight'] = 0.0
        
        # Preencher campos vazios
        text_columns = ['nome_assessor', 'created_by', 'approved_by', 'comments']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].fillna('')
        
        return df
        
    def validate_weights(self, df: pd.DataFrame) -> List[Dict]:
        """
        Valida que soma dos pesos CARD = 100% por assessor/período.
        
        Args:
            df: DataFrame com dados padronizados
            
        Returns:
            Lista de erros de validação
        """
        validation_errors = []
        
        # Filtrar apenas registros CARD ativos (sem valid_to)
        card_df = df[(df['indicator_type'] == 'CARD') & 
                     (df['valid_to'].isna() | (df['valid_to'] == ''))]
        
        if len(card_df) == 0:
            self.logger.warning("Nenhum indicador CARD ativo encontrado")
            return validation_errors
        
        # Agrupar por assessor e valid_from
        weight_sums = card_df.groupby(['cod_assessor', 'valid_from'])['weight'].sum()
        
        # Verificar somas diferentes de 100
        invalid_weights = weight_sums[abs(weight_sums - 100.0) > MAX_WEIGHT_DEVIATION]
        
        for (assessor, valid_from), total_weight in invalid_weights.items():
            validation_errors.append({
                'error_type': 'INVALID_WEIGHT_SUM',
                'cod_assessor': assessor,
                'valid_from': valid_from,
                'total_weight': float(total_weight),
                'expected': 100.0,
                'deviation': abs(float(total_weight) - 100.0)
            })
            
        return validation_errors
        
    def validate_relationships(self, df: pd.DataFrame, indicators_df: pd.DataFrame) -> List[Dict]:
        """
        Valida que todos indicator_codes existem em indicators.
        
        Args:
            df: DataFrame com dados padronizados
            indicators_df: DataFrame com indicadores válidos
            
        Returns:
            Lista de erros de validação
        """
        validation_errors = []
        
        if indicators_df.empty:
            self.logger.warning("Sem indicadores para validar relacionamentos")
            return validation_errors
        
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
        
    def validate_data(self) -> bool:
        """
        Valida os dados extraídos.
        
        Returns:
            True se os dados são válidos, False caso contrário
        """
        self.logger.info("Validando dados...")
        self.validation_errors = []
        
        # Validar campos obrigatórios
        required_fields = ['cod_assessor', 'indicator_code', 'indicator_type']
        for field in required_fields:
            if field not in self.data.columns:
                self.validation_errors.append({
                    'error_type': 'MISSING_REQUIRED_FIELD',
                    'field': field,
                    'message': f"Campo obrigatório ausente: {field}"
                })
                
        # Validar registros
        for idx, row in self.data.iterrows():
            # Campos obrigatórios não nulos
            for field in required_fields:
                if pd.isna(row.get(field)) or row.get(field) == '':
                    self.validation_errors.append({
                        'error_type': 'EMPTY_REQUIRED_FIELD',
                        'row': idx + 2,  # +2 para considerar header + 0-index
                        'field': field,
                        'message': f"Linha {idx+2}: campo {field} vazio"
                    })
                    
            # Tipo de indicador válido
            if row.get('indicator_type') and row['indicator_type'] not in VALID_INDICATOR_TYPES:
                self.validation_errors.append({
                    'error_type': 'INVALID_INDICATOR_TYPE',
                    'row': idx + 2,
                    'indicator_type': row['indicator_type'],
                    'message': f"Linha {idx+2}: tipo inválido '{row['indicator_type']}'"
                })
                
            # Validar peso para CARD
            if row.get('indicator_type') == 'CARD':
                weight = pd.to_numeric(row.get('weight'), errors='coerce')
                if pd.isna(weight) or weight <= 0 or weight > 100:
                    self.validation_errors.append({
                        'error_type': 'INVALID_CARD_WEIGHT',
                        'row': idx + 2,
                        'weight': row.get('weight'),
                        'message': f"Linha {idx+2}: peso CARD inválido '{row.get('weight')}'"
                    })
                    
        # Log de erros críticos
        critical_errors = [e for e in self.validation_errors if e['error_type'] in [
            'MISSING_REQUIRED_FIELD', 'EMPTY_REQUIRED_FIELD'
        ]]
        
        if critical_errors:
            self.logger.error(f"Encontrados {len(critical_errors)} erros críticos de validação")
            for error in critical_errors[:5]:  # Mostrar apenas os 5 primeiros
                self.logger.error(f"  - {error}")
            return False
            
        self.logger.info("Dados validados com sucesso (erros não críticos serão registrados)")
        return True
        
    def transform(self) -> pd.DataFrame:
        """
        Aplica transformações aos dados.
        
        Returns:
            DataFrame transformado
        """
        self.logger.info("Aplicando transformações...")
        
        df = self.data.copy()
        
        # T1: Padronização
        df = self.standardize_assignments(df)
        
        # T2: Validação de pesos
        weight_errors = self.validate_weights(df)
        if weight_errors:
            self.validation_errors.extend(weight_errors)
            self.logger.warning(f"{len(weight_errors)} erros de validação de peso encontrados")
            
        # T3: Validação de relacionamentos
        if self.indicators_df is not None and not self.indicators_df.empty:
            rel_errors = self.validate_relationships(df, self.indicators_df)
            if rel_errors:
                self.validation_errors.extend(rel_errors)
                self.logger.warning(f"{len(rel_errors)} códigos de indicador inválidos")
                
        # T4: Enriquecimento de metadados
        df['row_number'] = range(2, len(df) + 2)  # Número da linha na planilha
        
        # Calcular hash para cada linha
        hash_columns = ['cod_assessor', 'indicator_code', 'valid_from']
        df['row_hash'] = df[hash_columns].apply(
            lambda x: hashlib.md5('_'.join(str(x[col]) for col in hash_columns).encode()).hexdigest(),
            axis=1
        )
        
        # Adicionar flag de vigência
        df['is_current'] = (df['valid_to'].isna() | (df['valid_to'] == '')).astype(int)
        
        # Validação de soma de pesos por assessor/período
        df['weight_sum_valid'] = 1  # Default: válido
        df['indicator_exists'] = 1  # Default: existe
        
        # Marcar registros com erro de peso
        for error in self.validation_errors:
            if error['error_type'] == 'INVALID_WEIGHT_SUM':
                mask = (df['cod_assessor'] == error['cod_assessor']) & \
                       (df['valid_from'] == error['valid_from']) & \
                       (df['indicator_type'] == 'CARD')
                df.loc[mask, 'weight_sum_valid'] = 0
                
            elif error['error_type'] == 'INVALID_INDICATOR_CODE':
                mask = (df['cod_assessor'] == error['cod_assessor']) & \
                       (df['indicator_code'] == error['indicator_code'])
                df.loc[mask, 'indicator_exists'] = 0
        
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
            # Usar transação explícita
            conn = self.db_engine.connect()
            trans = conn.begin()
            
            try:
                # Limpar dados existentes do mesmo dia
                self.logger.info("Limpando dados existentes do dia...")
                conn.execute(text("""
                    DELETE FROM bronze.performance_assignments 
                    WHERE CAST(load_timestamp AS DATE) = CAST(GETDATE() AS DATE)
                """))
                
                # Preparar dados para carga
                load_data = self.processed_data.copy()
                
                # Adicionar campos de controle
                load_data['load_timestamp'] = datetime.now()
                load_data['load_source'] = f'GoogleSheets:{SPREADSHEET_ID}'
                load_data['is_processed'] = 0
                load_data['processing_date'] = None
                load_data['processing_status'] = None
                load_data['processing_notes'] = None
                
                # Adicionar erros de validação como JSON
                if self.validation_errors:
                    # Criar dicionário de erros por chave única
                    error_dict = {}
                    for error in self.validation_errors:
                        key = f"{error.get('cod_assessor', '')}_{error.get('indicator_code', '')}_{error.get('valid_from', '')}"
                        if key not in error_dict:
                            error_dict[key] = []
                        error_dict[key].append(error)
                    
                    # Aplicar erros às linhas correspondentes
                    load_data['validation_errors'] = load_data.apply(
                        lambda x: json.dumps(
                            error_dict.get(f"{x['cod_assessor']}_{x['indicator_code']}_{x['valid_from']}", [])
                        ) if f"{x['cod_assessor']}_{x['indicator_code']}_{x['valid_from']}" in error_dict else None,
                        axis=1
                    )
                else:
                    load_data['validation_errors'] = None
                
                # Converter tudo para string para Bronze
                string_columns = ['cod_assessor', 'nome_assessor', 'indicator_code', 
                                'indicator_type', 'weight', 'valid_from', 'valid_to',
                                'created_by', 'approved_by', 'comments']
                
                for col in string_columns:
                    if col in load_data.columns:
                        load_data[col] = load_data[col].astype(str).replace('nan', '')
                
                # Carregar no banco
                load_data.to_sql(
                    'performance_assignments',
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
                
                # Registrar auditoria
                try:
                    self._log_audit(conn, records_loaded, 'SUCCESS')
                except Exception as audit_error:
                    self.logger.warning(f"Erro ao registrar auditoria: {audit_error}")
                
                # Executar validações pós-carga
                self.post_load_validations()
                
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
                'etl_name': 'ETL-IND-002-performance-assignments',
                'execution_start': self.config.get('start_time', datetime.now()),
                'execution_end': datetime.now(),
                'records_read': len(self.data) if self.data is not None else 0,
                'records_written': records_count,
                'records_error': len(self.validation_errors),
                'status': status,
                'details': error_msg or json.dumps({
                    'spreadsheet_id': SPREADSHEET_ID,
                    'validation_errors': self.validation_errors[:10] if self.validation_errors else [],
                    'weight_errors': len([e for e in self.validation_errors if e['error_type'] == 'INVALID_WEIGHT_SUM']),
                    'relationship_errors': len([e for e in self.validation_errors if e['error_type'] == 'INVALID_INDICATOR_CODE'])
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
            
    def post_load_validations(self):
        """Executa validações pós-carga."""
        self.logger.info("Executando validações pós-carga...")
        
        try:
            with self.db_engine.connect() as conn:
                # Verificar assessores únicos
                result = conn.execute(text("""
                    SELECT COUNT(DISTINCT cod_assessor) as assessores_unicos
                    FROM bronze.performance_assignments
                    WHERE load_timestamp = (SELECT MAX(load_timestamp) FROM bronze.performance_assignments)
                """)).fetchone()
                
                self.logger.info(f"Assessores únicos carregados: {result.assessores_unicos}")
                
                # Verificar soma de pesos inválidos
                result = conn.execute(text("""
                    WITH weight_check AS (
                        SELECT 
                            cod_assessor,
                            valid_from,
                            SUM(CAST(weight AS DECIMAL(5,2))) as total_weight,
                            COUNT(*) as indicator_count
                        FROM bronze.performance_assignments
                        WHERE indicator_type = 'CARD'
                          AND (valid_to IS NULL OR valid_to = '')
                          AND load_timestamp = (SELECT MAX(load_timestamp) FROM bronze.performance_assignments)
                        GROUP BY cod_assessor, valid_from
                    )
                    SELECT 
                        COUNT(*) as assessores_com_erro,
                        COUNT(DISTINCT cod_assessor) as assessores_afetados
                    FROM weight_check
                    WHERE ABS(total_weight - 100.0) >= 0.01
                """)).fetchone()
                
                if result.assessores_com_erro > 0:
                    self.logger.warning(f"Assessores com soma de pesos inválida: {result.assessores_afetados}")
                    
                # Notificar se houver muitos erros
                if len(self.validation_errors) > 50:
                    self.logger.warning("Alto número de erros de validação - verificar dados de origem")
                    
        except Exception as e:
            self.logger.warning(f"Erro durante validações pós-carga: {e}")
            
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
            self.logger.info("Iniciando ETL-IND-002 - Performance Assignments")
            self.logger.info(f"Timestamp: {start_time}")
            self.logger.info("="*60)
            
            # Setup
            self.setup_connections()
            
            # Carregar indicadores para validação
            self.load_indicators()
            
            # Extract
            self.extract()
            
            # Validate
            if not self.validate_data():
                raise ValueError("Falha na validação crítica dos dados")
                
            # Transform
            self.transform()
            
            # Load
            records_loaded = self.load(dry_run)
            
            # Notificar sobre erros de validação
            if self.validation_errors:
                weight_errors = len([e for e in self.validation_errors if e['error_type'] == 'INVALID_WEIGHT_SUM'])
                rel_errors = len([e for e in self.validation_errors if e['error_type'] == 'INVALID_INDICATOR_CODE'])
                
                self.logger.warning(f"Total de erros de validação: {len(self.validation_errors)}")
                self.logger.warning(f"  - Erros de soma de peso: {weight_errors}")
                self.logger.warning(f"  - Indicadores inválidos: {rel_errors}")
                
            self.logger.info("="*60)
            self.logger.info("ETL-IND-002 concluído com sucesso!")
            self.logger.info(f"Tempo de execução: {datetime.now() - start_time}")
            self.logger.info("="*60)
            
        except Exception as e:
            self.logger.error("="*60)
            self.logger.error(f"ETL-IND-002 falhou: {e}")
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
        description='ETL-IND-002 - Extração de Atribuições de Performance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default=str(CONFIG_DIR / 'etl_002_config.json'),
        help='Arquivo de configuração (default: config/etl_002_config.json)'
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
        '--validate-only',
        action='store_true',
        help='Apenas valida dados sem carregar'
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
            
        # Substituir variáveis de ambiente
        config['database']['server'] = os.getenv('DB_SERVER', config['database']['server'])
        config['database']['database'] = os.getenv('DB_DATABASE', config['database']['database'])
        config['database']['user'] = os.getenv('DB_USERNAME', config['database']['user'])
        config['database']['password'] = os.getenv('DB_PASSWORD', config['database']['password'])
        
        # Driver ODBC
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
    log_filename = f"ETL-IND-002_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = setup_logging(log_filename)
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        
    try:
        # Carregar configuração
        config = load_config(args.config)
        
        # Criar e executar ETL
        etl = PerformanceAssignmentsETL(config, logger)
        
        if args.validate_only:
            etl.setup_connections()
            etl.load_indicators()
            etl.extract()
            if etl.validate_data():
                etl.transform()
                logger.info("Validação concluída - dados prontos para carga")
            else:
                logger.error("Validação falhou - corrigir erros antes de carregar")
        else:
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