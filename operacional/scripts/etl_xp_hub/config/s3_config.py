"""
Configuração de conexão e operações com AWS S3

Este módulo centraliza todas as configurações relacionadas ao S3,
incluindo credenciais, estrutura de buckets e padrões de nomenclatura.
"""

import os
import re
from typing import Optional, Tuple, List
from dataclasses import dataclass
from dotenv import load_dotenv

from utils.exceptions import ConfigurationError, S3ExtractionError

# Carrega variáveis de ambiente
load_dotenv()


@dataclass
class S3FileInfo:
    """
    Informações extraídas de um arquivo S3.
    
    Representa metadados de um arquivo seguindo o padrão de nomenclatura.
    """
    filename: str           # Nome completo do arquivo
    folder_name: str        # Nome da pasta S3 (extraído do filename)
    date: str              # Data no formato aaaa-mm-dd
    s3_key: str            # Chave completa no S3 (folder/filename)
    local_path: Optional[str] = None  # Caminho local (quando baixado)


class S3Config:
    """
    Configuração centralizada para operações com AWS S3.
    
    Gerencia credenciais, padrões de nomenclatura e estrutura do bucket.
    """
    
    # Padrões de nomenclatura:
    # 1. nome_da_pasta_aaaa-mm-dd.xlsx
    # 2. nome_da_pasta_acumulado.xlsx
    FILENAME_PATTERN_DATE = r"^(.+)_(\d{4}-\d{2}-\d{2})\.xlsx$"
    FILENAME_PATTERN_ACUMULADO = r"^(.+)_acumulado\.xlsx$"
    
    def __init__(self):
        self._load_environment_variables()
        self._validate_configuration()
    
    def _load_environment_variables(self):
        """Carrega e valida variáveis de ambiente do S3"""
        self.access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region_name = os.getenv('AWS_REGION', 'sa-east-1')  # Default baseado no script
        self.bucket_name = os.getenv('S3_BUCKET_NAME', 'm7investimentos')  # Default baseado no script
        
        # Configurações opcionais
        self.session_token = os.getenv('AWS_SESSION_TOKEN')  # Para credenciais temporárias
        self.endpoint_url = os.getenv('S3_ENDPOINT_URL')     # Para S3 compatível (LocalStack, etc.)
        
        # Configurações de operação
        self.download_timeout = int(os.getenv('S3_DOWNLOAD_TIMEOUT', '300'))
        self.upload_timeout = int(os.getenv('S3_UPLOAD_TIMEOUT', '300'))
        self.max_retries = int(os.getenv('S3_MAX_RETRIES', '3'))
    
    def _validate_configuration(self):
        """Valida se todas as configurações obrigatórias estão presentes"""
        required_fields = ['access_key_id', 'secret_access_key', 'bucket_name']
        missing_fields = []
        
        for field in required_fields:
            if not getattr(self, field):
                missing_fields.append(field.upper())
        
        if missing_fields:
            raise ConfigurationError(
                f"Missing required S3 configuration: {', '.join(missing_fields)}",
                config_type="s3",
                context={"missing_fields": missing_fields}
            )
    
    def get_boto3_config(self) -> dict:
        """
        Retorna configuração para inicializar cliente boto3.
        
        Returns:
            Dicionário com configurações para boto3.client('s3')
        """
        config = {
            'aws_access_key_id': self.access_key_id,
            'aws_secret_access_key': self.secret_access_key,
            'region_name': self.region_name
        }
        
        # Adiciona campos opcionais se presentes
        if self.session_token:
            config['aws_session_token'] = self.session_token
        if self.endpoint_url:
            config['endpoint_url'] = self.endpoint_url
            
        return config
    
    def parse_filename(self, filename: str) -> Optional[S3FileInfo]:
        """
        Extrai informações do nome do arquivo seguindo os padrões estabelecidos.
        
        Padrões aceitos:
        1. nome_da_pasta_aaaa-mm-dd.xlsx -> pasta: nome_da_pasta, data: aaaa-mm-dd
        2. nome_da_pasta_acumulado.xlsx -> pasta: nome_da_pasta, data: "acumulado"
        
        Args:
            filename: Nome do arquivo
            
        Returns:
            S3FileInfo se válido, None caso contrário
        """
        try:
            # Primeiro tenta padrão com data
            match_date = re.match(self.FILENAME_PATTERN_DATE, filename)
            
            if match_date:
                folder_name = match_date.group(1)
                date = match_date.group(2)
                s3_key = f"{folder_name}/{filename}"
                
                return S3FileInfo(
                    filename=filename,
                    folder_name=folder_name,
                    date=date,
                    s3_key=s3_key
                )
            
            # Depois tenta padrão acumulado
            match_acumulado = re.match(self.FILENAME_PATTERN_ACUMULADO, filename)
            
            if match_acumulado:
                folder_name = match_acumulado.group(1)
                date = "acumulado"  # Usa "acumulado" como indicador especial
                s3_key = f"{folder_name}/{filename}"
                
                return S3FileInfo(
                    filename=filename,
                    folder_name=folder_name,
                    date=date,
                    s3_key=s3_key
                )
            
            # Se não corresponde a nenhum padrão
            return None
                
        except Exception as e:
            raise S3ExtractionError(
                f"Error parsing filename: {filename}",
                context={"filename": filename, "error": str(e)},
                original_exception=e
            )
    
    def validate_filename(self, filename: str) -> bool:
        """
        Valida se o nome do arquivo segue o padrão estabelecido.
        
        Args:
            filename: Nome do arquivo
            
        Returns:
            True se válido, False caso contrário
        """
        return self.parse_filename(filename) is not None
    
    def get_s3_key(self, folder_name: str, filename: str) -> str:
        """
        Gera chave S3 baseada na pasta e nome do arquivo.
        
        Args:
            folder_name: Nome da pasta no S3
            filename: Nome do arquivo
            
        Returns:
            Chave completa para o S3
        """
        return f"{folder_name}/{filename}"
    
    def get_processed_key(self, original_key: str) -> str:
        """
        Gera chave para arquivo processado.
        
        Move o arquivo para subpasta 'processado' dentro da pasta original.
        Exemplo: vendas/arquivo.xlsx -> vendas/processado/arquivo.xlsx
        
        Args:
            original_key: Chave original do arquivo
            
        Returns:
            Nova chave para arquivo processado
        """
        parts = original_key.split('/')
        if len(parts) >= 2:
            folder = parts[0]
            filename = parts[-1]
            return f"{folder}/processado/{filename}"
        else:
            return f"processado/{original_key}"
    
    def list_expected_folders(self) -> List[str]:
        """
        Lista todas as pastas existentes no bucket S3 dinamicamente.
        
        Returns:
            Lista de nomes de pastas encontradas no bucket
        """
        try:
            import boto3
            s3_client = boto3.client('s3', **self.get_boto3_config())
            
            response = s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Delimiter='/'
            )
            
            folders = []
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    folder_name = prefix['Prefix'].rstrip('/')
                    # Ignora pastas que começam com ponto ou são temporárias
                    if not folder_name.startswith('.') and folder_name != 'temp':
                        folders.append(folder_name)
            
            return sorted(folders)
            
        except Exception as e:
            print(f"⚠️ Warning: Não foi possível listar pastas do S3: {e}")
            # Fallback para pastas conhecidas se falhar
            return ['diversificacao', 'vendas', 'relatorio_financeiro', 'dados_cliente']
    
    def get_folder_files_pattern(self, folder_name: str) -> str:
        """
        Retorna padrão de arquivos esperados para uma pasta específica.
        Inclui tanto arquivos com data quanto arquivos acumulados.
        
        Args:
            folder_name: Nome da pasta
            
        Returns:
            Padrão regex para arquivos da pasta
        """
        # Padrão que aceita tanto data quanto "acumulado"
        return rf"^{folder_name}_(\d{{4}}-\d{{2}}-\d{{2}}|acumulado)\.xlsx$"
    
    def extract_date_from_filename(self, filename: str) -> Optional[str]:
        """
        Extrai apenas a data do nome do arquivo.
        
        Args:
            filename: Nome do arquivo
            
        Returns:
            Data no formato aaaa-mm-dd ou None se inválido
        """
        file_info = self.parse_filename(filename)
        return file_info.date if file_info else None


# Instância global de configuração (singleton pattern)
_s3_config_instance = None

def get_s3_config() -> S3Config:
    """
    Retorna instância singleton da configuração do S3.
    
    Returns:
        Instância de S3Config
    """
    global _s3_config_instance
    if _s3_config_instance is None:
        _s3_config_instance = S3Config()
    return _s3_config_instance
