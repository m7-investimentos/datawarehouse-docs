#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ETL-IND-003 - Extração de Metas de Performance do Google Sheets
================================================================================
Tipo: Script ETL
Versão: 1.0.0
Última atualização: 2025-01-17
Autor: bruno.chiaramonti@multisete.com
Revisor: arquitetura.dados@m7investimentos.com.br
Tags: [etl, performance, targets, metas, google-sheets, bronze]
Status: produção
Python: 3.8+
================================================================================

OBJETIVO:
    Extrair dados de metas mensais de performance por assessor e indicador 
    da planilha Google Sheets m7_performance_targets para a camada Bronze 
    do Data Warehouse, incluindo validações de integridade de valores e 
    relacionamentos temporais.

CASOS DE USO:
    1. Carga anual de metas (dezembro para ano seguinte)
    2. Ajustes mensais de metas
    3. Recarga completa sob demanda

FREQUÊNCIA DE EXECUÇÃO:
    Mensal ou sob demanda (planejamento anual com ajustes)

EXEMPLOS DE USO:
    # Execução básica
    python etl_003_targets.py
    
    # Com configuração customizada
    python etl_003_targets.py --config config/etl_003_production.json
    
    # Modo debug com dry-run
    python etl_003_targets.py --debug --dry-run
    
    # Carga anual completa
    python etl_003_targets.py --mode annual --target-year 2025
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
import calendar

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

# Criar diretórios se não existirem
for directory in [CONFIG_DIR, DATA_DIR, LOG_DIR, CREDENTIALS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Carregar variáveis de ambiente do arquivo .env
load_dotenv(CREDENTIALS_DIR / '.env')

# Configuração de logging
LOG_FORMAT = '[%(asctime)s] [%(levelname)s] [ETL-IND-003] %(message)s'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Configuração do Google Sheets
SPREADSHEET_ID = '1nm-z2Fbp7pasHx5gmVbm7JPNBRWp4iRElYCbVfEFpOE'
RANGE_NAME = 'Página1!A:I'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Constantes de validação
MIN_EXPECTED_RECORDS = 1000
MAX_EXPECTED_RECORDS = 5000
REQUIRED_MONTHS = 12
MAX_WEIGHT_DEVIATION = 0.01  # 1% de tolerância para arredondamentos
DEFAULT_BATCH_SIZE = 10  # Reduced for SQL Server compatibility

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
    logger = logging.getLogger('ETL-IND-003')
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

class PerformanceTargetsETL:
    """
    ETL para extrair metas de performance do Google Sheets para Bronze.
    
    Attributes:
        config: Dicionário de configuração
        logger: Logger para registro de eventos
        credentials: Credenciais do Google Service Account
        db_engine: Engine de conexão com banco de dados
    """
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Inicializa o ETL de metas.
        
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
        self.batch_size = config.get('batch_size', DEFAULT_BATCH_SIZE)
        self.inverted_indicators = []
        
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
            
    def load_inverted_indicators(self):
        """Carrega lista de indicadores invertidos da tabela de indicadores."""
        try:
            with self.db_engine.connect() as conn:
                query = text("""
                    SELECT DISTINCT indicator_code 
                    FROM bronze.performance_indicators
                    WHERE is_inverted = '1'
                """)
                result = conn.execute(query)
                self.inverted_indicators = [row[0] for row in result]
                self.logger.info(f"Carregados {len(self.inverted_indicators)} indicadores invertidos")
        except Exception as e:
            self.logger.warning(f"Erro ao carregar indicadores invertidos: {e}")
            self.inverted_indicators = []
            
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(HttpError)
    )
    def extract_in_batches(self) -> pd.DataFrame:
        """
        Extrai dados do Google Sheets em lotes para melhor performance.
        
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
            
            # Obter metadados da planilha para saber o tamanho
            sheet_metadata = sheet.get(
                spreadsheetId=SPREADSHEET_ID
            ).execute()
            
            total_rows = sheet_metadata['sheets'][0]['properties']['gridProperties']['rowCount']
            self.logger.info(f"Total de linhas na planilha: {total_rows}")
            
            # Extrair em batches
            all_data = []
            headers = None
            
            for start_row in range(1, total_rows, self.batch_size):
                end_row = min(start_row + self.batch_size - 1, total_rows)
                range_name = f'Página1!A{start_row}:I{end_row}'
                
                try:
                    result = sheet.values().get(
                        spreadsheetId=SPREADSHEET_ID,
                        range=range_name
                    ).execute()
                    
                    values = result.get('values', [])
                    
                    if start_row == 1 and values:
                        headers = values[0]
                        values = values[1:]
                    
                    # Filtrar linhas vazias
                    filtered_values = []
                    for row in values:
                        if row and any(cell.strip() for cell in row if cell):
                            filtered_values.append(row)
                    
                    if filtered_values:
                        all_data.extend(filtered_values)
                        self.logger.debug(f"Extraído batch: linhas {start_row} a {end_row} ({len(filtered_values)} registros válidos)")
                    
                    # Se encontrar muitas linhas vazias consecutivas, parar
                    if len(filtered_values) < 10 and len(values) > 50:
                        self.logger.info("Muitas linhas vazias encontradas, finalizando extração")
                        break
                        
                except Exception as e:
                    self.logger.error(f"Erro ao extrair batch {start_row}-{end_row}: {e}")
                    raise
            
            if not all_data:
                raise ValueError("Planilha vazia ou sem dados")
            
            if len(all_data) < MIN_EXPECTED_RECORDS:
                self.logger.warning(f"Poucos dados na planilha: {len(all_data)} linhas (esperado: >{MIN_EXPECTED_RECORDS})")
                
            # Normalizar dados
            max_cols = len(headers)
            normalized_data = []
            for row in all_data:
                normalized_row = row + [''] * (max_cols - len(row))
                normalized_data.append(normalized_row[:max_cols])
            
            # Converter para DataFrame
            self.data = pd.DataFrame(normalized_data, columns=headers)
            self.logger.info(f"Extraídos {len(self.data)} registros de metas")
            
            return self.data
            
        except HttpError as e:
            if e.resp.status == 404:
                self.logger.error("Planilha não encontrada - verificar ID")
            elif e.resp.status == 403:
                self.logger.error("Sem permissão para acessar a planilha")
            else:
                self.logger.error(f"Erro HTTP: {e}")
            raise
            
    def standardize_periods(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Padroniza períodos garantindo primeiro e último dia do mês.
        
        Args:
            df: DataFrame com dados brutos
            
        Returns:
            DataFrame com períodos padronizados
        """
        # Converter para datetime
        df['period_start'] = pd.to_datetime(df['period_start'], errors='coerce')
        
        # Forçar primeiro dia do mês
        df['period_start'] = df['period_start'].apply(
            lambda x: x.replace(day=1) if pd.notna(x) else x
        )
        
        # Calcular último dia do mês
        df['period_end'] = df['period_start'].apply(
            lambda x: x.replace(day=calendar.monthrange(x.year, x.month)[1]) if pd.notna(x) else x
        )
        
        # Period type sempre MENSAL
        df['period_type'] = 'MENSAL'
        
        return df
        
    def convert_and_validate_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte valores e valida lógica de metas.
        
        Args:
            df: DataFrame com dados padronizados
            
        Returns:
            DataFrame com valores convertidos e validados
        """
        # Converter para numérico
        value_cols = ['target_value', 'stretch_value', 'minimum_value']
        for col in value_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        # Identificar indicadores invertidos
        df['is_inverted'] = df['indicator_code'].isin(self.inverted_indicators).astype(int)
        
        # Validar lógica stretch > target > minimum (exceto invertidos)
        def validate_target_logic(row):
            if pd.isna(row['stretch_value']) or pd.isna(row['minimum_value']):
                return 1  # OK se valores opcionais não existem
            
            if row['stretch_value'] == 0 or row['minimum_value'] == 0:
                return 1  # OK se valores são zero
            
            if row['is_inverted']:
                # Para invertidos: stretch < target < minimum
                return int(row['stretch_value'] <= row['target_value'] <= row['minimum_value'])
            else:
                # Normal: stretch >= target >= minimum
                return int(row['stretch_value'] >= row['target_value'] >= row['minimum_value'])
        
        df['target_logic_valid'] = df.apply(validate_target_logic, axis=1)
        
        # Log de registros com lógica inválida
        invalid_logic = df[df['target_logic_valid'] == 0]
        if len(invalid_logic) > 0:
            self.logger.warning(f"{len(invalid_logic)} registros com lógica stretch/target/minimum inválida")
        
        return df
        
    def validate_annual_completeness(self, df: pd.DataFrame) -> List[Dict]:
        """
        Valida que cada assessor/indicador tem 12 meses.
        
        Args:
            df: DataFrame com dados padronizados
            
        Returns:
            Lista de erros de validação
        """
        validation_errors = []
        
        # Agrupar por assessor e indicador
        grouped = df.groupby(['cod_assessor', 'indicator_code'])
        
        for (assessor, indicator), group in grouped:
            # Extrair meses únicos
            months = group['period_start'].dt.month.unique()
            missing_months = sorted(set(range(1, 13)) - set(months))
            
            if missing_months:
                validation_errors.append({
                    'error_type': 'INCOMPLETE_YEAR',
                    'cod_assessor': assessor,
                    'indicator_code': indicator,
                    'missing_months': missing_months,
                    'months_found': len(months)
                })
        
        if validation_errors:
            self.logger.warning(f"{len(validation_errors)} combinações assessor/indicador com ano incompleto")
            # Log de alguns exemplos
            for error in validation_errors[:5]:
                self.logger.debug(f"  - {error['cod_assessor']}/{error['indicator_code']}: faltam meses {error['missing_months']}")
        
        return validation_errors
        
    def add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona metadados de controle ao DataFrame.
        
        Args:
            df: DataFrame com dados transformados
            
        Returns:
            DataFrame com metadados adicionados
        """
        # Timestamp de extração
        df['extraction_timestamp'] = datetime.now()
        df['source_file'] = f'GoogleSheets:{SPREADSHEET_ID}'
        
        # Número da linha original
        df['row_number'] = range(2, len(df) + 2)
        
        # Hash por registro único
        df['row_hash'] = df.apply(
            lambda x: hashlib.md5(
                f"{x['cod_assessor']}_{x['indicator_code']}_{x['period_start'].strftime('%Y-%m') if pd.notna(x['period_start']) else ''}".encode()
            ).hexdigest(), 
            axis=1
        )
        
        # Adicionar ano e trimestre de referência
        df['target_year'] = df['period_start'].dt.year
        df['target_quarter'] = df['period_start'].dt.quarter
        
        return df
        
    def validate_batch(self, df: pd.DataFrame) -> Dict[str, List]:
        """
        Validações em lote para performance.
        
        Args:
            df: DataFrame com dados processados
            
        Returns:
            Dicionário com erros categorizados
        """
        errors = {
            'critical': [],
            'warning': [],
            'info': []
        }
        
        # Validação 1: Volume total
        if len(df) < MIN_EXPECTED_RECORDS:
            errors['warning'].append(f"Volume baixo: apenas {len(df)} registros (esperado: >{MIN_EXPECTED_RECORDS})")
        elif len(df) > MAX_EXPECTED_RECORDS:
            errors['warning'].append(f"Volume alto: {len(df)} registros (esperado: <{MAX_EXPECTED_RECORDS})")
        
        # Validação 2: Assessores únicos
        unique_assessors = df['cod_assessor'].nunique()
        if unique_assessors < 20:
            errors['warning'].append(f"Poucos assessores: {unique_assessors}")
        
        # Validação 3: Metas zeradas
        zero_targets = df[df['target_value'] == 0]
        if len(zero_targets) > 0:
            errors['info'].append(f"{len(zero_targets)} metas com valor zero")
        
        # Validação 4: Lógica de valores
        invalid_logic = df[df['target_logic_valid'] == 0]
        if len(invalid_logic) > 0:
            errors['critical'].append(
                f"{len(invalid_logic)} registros com lógica stretch/target/minimum inválida"
            )
        
        # Validação 5: Datas inválidas
        invalid_dates = df[df['period_start'].isna()]
        if len(invalid_dates) > 0:
            errors['critical'].append(
                f"{len(invalid_dates)} registros com data inválida"
            )
        
        return errors
        
    def validate_data(self) -> bool:
        """
        Valida os dados extraídos.
        
        Returns:
            True se os dados são válidos, False caso contrário
        """
        self.logger.info("Validando dados...")
        self.validation_errors = []
        
        # Validar campos obrigatórios
        required_fields = ['cod_assessor', 'indicator_code', 'period_start', 'target_value']
        for field in required_fields:
            if field not in self.data.columns:
                self.validation_errors.append({
                    'error_type': 'MISSING_REQUIRED_FIELD',
                    'field': field,
                    'message': f"Campo obrigatório ausente: {field}"
                })
                
        # Se faltar campos críticos, retornar False
        if self.validation_errors:
            self.logger.error(f"Campos obrigatórios ausentes: {[e['field'] for e in self.validation_errors]}")
            return False
        
        # Validar registros
        invalid_rows = []
        for idx, row in self.data.iterrows():
            row_errors = []
            
            # Campos obrigatórios não nulos
            for field in required_fields:
                if pd.isna(row.get(field)) or str(row.get(field)).strip() == '':
                    row_errors.append(f"campo {field} vazio")
            
            if row_errors:
                invalid_rows.append({
                    'row': idx + 2,
                    'errors': row_errors
                })
        
        if invalid_rows:
            self.logger.error(f"Encontradas {len(invalid_rows)} linhas inválidas")
            for row_info in invalid_rows[:5]:  # Mostrar apenas as 5 primeiras
                self.logger.error(f"  - Linha {row_info['row']}: {', '.join(row_info['errors'])}")
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
        
        # T1: Padronização de períodos
        df = self.standardize_periods(df)
        
        # T2: Conversão e validação de valores
        df = self.convert_and_validate_values(df)
        
        # T3: Validação de completude anual
        annual_errors = self.validate_annual_completeness(df)
        if annual_errors:
            self.validation_errors.extend(annual_errors)
        
        # T4: Adicionar metadados
        df = self.add_metadata(df)
        
        # Validação em lote
        batch_errors = self.validate_batch(df)
        
        # Log de erros de validação
        for level, errors in batch_errors.items():
            for error in errors:
                if level == 'critical':
                    self.logger.error(f"CRÍTICO: {error}")
                elif level == 'warning':
                    self.logger.warning(f"AVISO: {error}")
                else:
                    self.logger.info(f"INFO: {error}")
        
        self.processed_data = df
        self.logger.info("Transformações aplicadas com sucesso")
        
        return self.processed_data
        
    def load_optimized(self, dry_run: bool = False) -> int:
        """
        Carrega dados no Bronze com otimização para grande volume.
        
        Args:
            dry_run: Se True, não executa a carga real
            
        Returns:
            Número de registros carregados
        """
        self.logger.info(f"Iniciando carga otimizada de {len(self.processed_data)} registros")
        
        if dry_run:
            self.logger.info("Modo dry-run: dados não serão carregados")
            self.logger.info(f"Seriam carregados {len(self.processed_data)} registros")
            return 0
            
        start_time = datetime.now()
        records_loaded = 0
        
        try:
            # Usar transação explícita
            conn = self.db_engine.connect()
            trans = conn.begin()
            
            try:
                # Determinar se é carga completa ou incremental
                target_year = self.processed_data['target_year'].iloc[0]
                
                # Se for janeiro ou modo anual, fazer carga completa
                if datetime.now().month == 1 or self.config.get('execution_mode') == 'annual':
                    self.logger.info(f"Executando carga completa para o ano {target_year}")
                    conn.execute(text(f"""
                        DELETE FROM bronze.performance_targets 
                        WHERE target_year = {target_year}
                    """))
                else:
                    # Carga incremental - deletar apenas o mês atual
                    current_month = datetime.now().month
                    self.logger.info(f"Executando carga incremental para {target_year}-{current_month:02d}")
                    conn.execute(text(f"""
                        DELETE FROM bronze.performance_targets 
                        WHERE target_year = {target_year}
                        AND MONTH(CAST(period_start AS DATE)) = {current_month}
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
                        if error['error_type'] == 'INCOMPLETE_YEAR':
                            key = f"{error['cod_assessor']}_{error['indicator_code']}"
                            if key not in error_dict:
                                error_dict[key] = []
                            error_dict[key].append(error)
                    
                    # Aplicar erros às linhas correspondentes
                    load_data['validation_errors'] = load_data.apply(
                        lambda x: json.dumps(
                            error_dict.get(f"{x['cod_assessor']}_{x['indicator_code']}", [])
                        ) if f"{x['cod_assessor']}_{x['indicator_code']}" in error_dict else None,
                        axis=1
                    )
                else:
                    load_data['validation_errors'] = None
                
                # Converter datas para string no formato apropriado
                load_data['period_start'] = load_data['period_start'].dt.strftime('%Y-%m-%d')
                load_data['period_end'] = load_data['period_end'].dt.strftime('%Y-%m-%d')
                
                # Converter tudo para string para Bronze (exceto campos numéricos específicos)
                string_columns = ['cod_assessor', 'nome_assessor', 'indicator_code', 
                                'period_type', 'period_start', 'period_end']
                
                for col in string_columns:
                    if col in load_data.columns:
                        load_data[col] = load_data[col].astype(str).replace('nan', '').replace('NaT', '')
                
                # Manter valores numéricos como string mas formatados
                numeric_columns = ['target_value', 'stretch_value', 'minimum_value']
                for col in numeric_columns:
                    if col in load_data.columns:
                        load_data[col] = load_data[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) and x != 0 else '')
                
                # Reorganizar colunas na ordem correta (excluindo load_id que é IDENTITY)
                columns_order = [
                    'load_timestamp', 'load_source',
                    'cod_assessor', 'nome_assessor', 'indicator_code',
                    'period_type', 'period_start', 'period_end',
                    'target_value', 'stretch_value', 'minimum_value',
                    'row_number', 'row_hash', 'target_year', 'target_quarter',
                    'is_processed', 'processing_date', 'processing_status', 'processing_notes',
                    'target_logic_valid', 'is_inverted', 'validation_errors'
                ]
                
                # Remover colunas extras que não existem na tabela
                extra_columns = ['extraction_timestamp', 'source_file']
                for col in extra_columns:
                    if col in load_data.columns:
                        load_data = load_data.drop(columns=[col])
                
                # Garantir que todas as colunas existem
                for col in columns_order:
                    if col not in load_data.columns:
                        self.logger.warning(f"Coluna {col} não encontrada no DataFrame")
                
                # Reorganizar DataFrame
                available_columns = [col for col in columns_order if col in load_data.columns]
                load_data = load_data[available_columns]
                
                # Carregar em batches
                total_batches = (len(load_data) - 1) // self.batch_size + 1
                
                for i in range(0, len(load_data), self.batch_size):
                    batch_start = datetime.now()
                    batch = load_data.iloc[i:i+self.batch_size]
                    
                    try:
                        batch.to_sql(
                            'performance_targets',
                            conn,
                            schema='bronze',
                            if_exists='append',
                            index=False,
                            method=None  # Usar método padrão, não multi
                        )
                        
                        records_loaded += len(batch)
                        batch_time = (datetime.now() - batch_start).total_seconds()
                        batch_num = i // self.batch_size + 1
                        
                        self.logger.debug(
                            f"Batch {batch_num}/{total_batches} carregado em {batch_time:.2f}s "
                            f"({len(batch)/batch_time:.0f} records/s)"
                        )
                        
                    except Exception as batch_error:
                        current_batch_num = i // self.batch_size + 1
                        self.logger.error(f"Erro ao inserir batch {current_batch_num}: {batch_error}")
                        # Tentar inserir linha por linha neste batch
                        for idx, row in batch.iterrows():
                            try:
                                row_df = pd.DataFrame([row])
                                row_df.to_sql(
                                    'performance_targets',
                                    conn,
                                    schema='bronze',
                                    if_exists='append',
                                    index=False
                                )
                                records_loaded += 1
                            except Exception as row_error:
                                self.logger.error(f"Erro ao inserir linha {idx}: {row_error}")
                
                # Commit explícito
                trans.commit()
                self.logger.info("Transação commitada com sucesso")
                
                total_time = (datetime.now() - start_time).total_seconds()
                self.logger.info(
                    f"Carga completa: {records_loaded} registros em {total_time:.2f}s "
                    f"({records_loaded/total_time:.0f} records/s)"
                )
                
                # Registrar auditoria
                try:
                    self._log_audit(conn, records_loaded, 'SUCCESS')
                except Exception as audit_error:
                    self.logger.warning(f"Erro ao registrar auditoria: {audit_error}")
                
                # Executar validações pós-carga
                self.post_load_validations(target_year)
                
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
                'etl_name': 'ETL-IND-003-performance-targets',
                'execution_start': self.config.get('start_time', datetime.now()),
                'execution_end': datetime.now(),
                'records_read': len(self.data) if self.data is not None else 0,
                'records_written': records_count,
                'records_error': len(self.validation_errors),
                'status': status,
                'details': error_msg or json.dumps({
                    'spreadsheet_id': SPREADSHEET_ID,
                    'validation_errors': len(self.validation_errors),
                    'incomplete_years': len([e for e in self.validation_errors if e['error_type'] == 'INCOMPLETE_YEAR']),
                    'invalid_logic': len(self.processed_data[self.processed_data['target_logic_valid'] == 0]) if self.processed_data is not None else 0
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
            
    def post_load_validations(self, target_year: int):
        """Executa validações pós-carga."""
        self.logger.info("Executando validações pós-carga...")
        
        try:
            with self.db_engine.connect() as conn:
                # Verificar volume carregado
                result = conn.execute(text(f"""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT cod_assessor) as unique_assessors,
                        COUNT(DISTINCT indicator_code) as unique_indicators,
                        COUNT(DISTINCT CONCAT(cod_assessor, '_', indicator_code)) as unique_combinations
                    FROM bronze.performance_targets
                    WHERE target_year = {target_year}
                    AND load_timestamp = (
                        SELECT MAX(load_timestamp) 
                        FROM bronze.performance_targets 
                        WHERE target_year = {target_year}
                    )
                """)).fetchone()
                
                self.logger.info(f"Carregados: {result.total_records} registros, "
                               f"{result.unique_assessors} assessores, "
                               f"{result.unique_indicators} indicadores, "
                               f"{result.unique_combinations} combinações únicas")
                
                # Verificar completude anual
                result = conn.execute(text(f"""
                    WITH monthly_coverage AS (
                        SELECT 
                            cod_assessor,
                            indicator_code,
                            COUNT(DISTINCT MONTH(CAST(period_start AS DATE))) as months_count
                        FROM bronze.performance_targets
                        WHERE target_year = {target_year}
                        AND is_processed = 0
                        GROUP BY cod_assessor, indicator_code
                    )
                    SELECT 
                        COUNT(*) as total_combinations,
                        SUM(CASE WHEN months_count = 12 THEN 1 ELSE 0 END) as complete_years,
                        SUM(CASE WHEN months_count < 12 THEN 1 ELSE 0 END) as incomplete_years,
                        AVG(months_count) as avg_months
                    FROM monthly_coverage
                """)).fetchone()
                
                if result.incomplete_years > 0:
                    self.logger.warning(f"Combinações com ano incompleto: {result.incomplete_years} "
                                      f"({result.incomplete_years/result.total_combinations*100:.1f}%)")
                
                # Verificar lógica de metas
                result = conn.execute(text(f"""
                    SELECT 
                        COUNT(*) as total_with_stretch,
                        SUM(CASE WHEN target_logic_valid = 0 THEN 1 ELSE 0 END) as invalid_logic
                    FROM bronze.performance_targets
                    WHERE target_year = {target_year}
                    AND stretch_value != ''
                    AND minimum_value != ''
                    AND is_processed = 0
                """)).fetchone()
                
                if result.invalid_logic > 0:
                    self.logger.warning(f"Registros com lógica inválida: {result.invalid_logic} "
                                      f"de {result.total_with_stretch}")
                    
        except Exception as e:
            self.logger.warning(f"Erro durante validações pós-carga: {e}")
            
    def generate_quality_report(self) -> Dict:
        """
        Gera relatório de qualidade dos dados.
        
        Returns:
            Dicionário com estatísticas de qualidade
        """
        if self.processed_data is None:
            return {}
            
        df = self.processed_data
        
        report = {
            'summary': {
                'total_records': len(df),
                'unique_assessors': df['cod_assessor'].nunique(),
                'unique_indicators': df['indicator_code'].nunique(),
                'date_range': f"{df['period_start'].min()} to {df['period_start'].max()}",
                'extraction_time': datetime.now().isoformat()
            },
            'validation': {
                'records_with_invalid_logic': len(df[df['target_logic_valid'] == 0]),
                'records_missing_stretch': df['stretch_value'].eq(0).sum(),
                'records_missing_minimum': df['minimum_value'].eq(0).sum(),
                'zero_targets': len(df[df['target_value'] == 0]),
                'incomplete_years': len([e for e in self.validation_errors if e['error_type'] == 'INCOMPLETE_YEAR'])
            },
            'statistics': {
                'avg_target_value': float(df['target_value'].mean()),
                'total_annual_target': float(df['target_value'].sum()),
                'targets_by_month': df.groupby(df['period_start'].dt.month)['target_value'].agg(['count', 'mean', 'sum']).to_dict()
            }
        }
        
        return report
        
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
            self.logger.info("Iniciando ETL-IND-003 - Performance Targets")
            self.logger.info(f"Timestamp: {start_time}")
            self.logger.info(f"Modo: {'DRY-RUN' if dry_run else 'PRODUÇÃO'}")
            self.logger.info("="*60)
            
            # Setup
            self.setup_connections()
            
            # Carregar indicadores invertidos
            self.load_inverted_indicators()
            
            # Extract
            self.extract_in_batches()
            
            # Validate
            if not self.validate_data():
                raise ValueError("Falha na validação crítica dos dados")
                
            # Transform
            self.transform()
            
            # Generate quality report
            quality_report = self.generate_quality_report()
            self.logger.info(f"Relatório de Qualidade: {json.dumps(quality_report['summary'], indent=2)}")
            
            # Load
            records_loaded = self.load_optimized(dry_run)
            
            # Log final
            self.logger.info("="*60)
            self.logger.info("ETL-IND-003 concluído com sucesso!")
            self.logger.info(f"Tempo de execução: {datetime.now() - start_time}")
            self.logger.info(f"Registros processados: {records_loaded}")
            if self.validation_errors:
                self.logger.info(f"Erros de validação: {len(self.validation_errors)}")
            self.logger.info("="*60)
            
        except Exception as e:
            self.logger.error("="*60)
            self.logger.error(f"ETL-IND-003 falhou: {e}")
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
        description='ETL-IND-003 - Extração de Metas de Performance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default=str(CONFIG_DIR / 'etl_003_config.json'),
        help='Arquivo de configuração (default: config/etl_003_config.json)'
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=['monthly', 'annual', 'custom'],
        default='monthly',
        help='Modo de execução (default: monthly)'
    )
    
    parser.add_argument(
        '--target-year',
        type=int,
        default=datetime.now().year,
        help='Ano alvo para carga (default: ano atual)'
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
    log_filename = f"ETL-IND-003_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = setup_logging(log_filename)
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        
    try:
        # Carregar configuração
        config = load_config(args.config)
        
        # Adicionar parâmetros de execução à configuração
        config['execution_mode'] = args.mode
        config['target_year'] = args.target_year
        
        # Criar e executar ETL
        etl = PerformanceTargetsETL(config, logger)
        
        if args.validate_only:
            etl.setup_connections()
            etl.load_inverted_indicators()
            etl.extract_in_batches()
            if etl.validate_data():
                etl.transform()
                quality_report = etl.generate_quality_report()
                logger.info("Validação concluída - dados prontos para carga")
                logger.info(f"Relatório: {json.dumps(quality_report, indent=2)}")
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