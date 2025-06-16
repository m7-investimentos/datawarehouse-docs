"""
Extrator S3 - Download de arquivos do bucket S3

Esta classe gerencia o download de arquivos Excel do S3 para processamento local.
"""

import boto3
import os
from typing import List, Optional
from botocore.exceptions import ClientError

from config.s3_config import get_s3_config, S3FileInfo
from utils.exceptions import S3ExtractionError


class S3Extractor:
    """
    Extrator para baixar arquivos do S3.
    
    Gerencia download de arquivos Excel seguindo o padrão de nomenclatura
    e organização em pastas temporárias.
    """
    
    def __init__(self, temp_folder: str = "./temp"):
        """
        Inicializa extrator S3.
        
        Args:
            temp_folder: Pasta para arquivos temporários
        """
        self.config = get_s3_config()
        self.temp_folder = temp_folder
        self.s3_client = None
        
        # Cria pasta temporária
        os.makedirs(temp_folder, exist_ok=True)
    
    def _get_s3_client(self):
        """Cria cliente S3 se não existe."""
        if self.s3_client is None:
            try:
                boto3_config = self.config.get_boto3_config()
                self.s3_client = boto3.client('s3', **boto3_config)
            except Exception as e:
                raise S3ExtractionError(
                    "Failed to create S3 client",
                    bucket=self.config.bucket_name,
                    original_exception=e
                )
        return self.s3_client
    
    def list_files_in_folder(self, folder_name: str) -> List[S3FileInfo]:
        """
        Lista arquivos .xlsx em uma pasta específica do S3.
        
        Args:
            folder_name: Nome da pasta no S3
            
        Returns:
            Lista de informações dos arquivos (vazia se não houver arquivos)
        """
        try:
            s3_client = self._get_s3_client()
            
            response = s3_client.list_objects_v2(
                Bucket=self.config.bucket_name,
                Prefix=f"{folder_name}/",
                Delimiter="/"  # Não busca em subpastas
            )
            
            files_info = []
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    filename = os.path.basename(key)
                    
                    # Só processa arquivos .xlsx que não estão na pasta processado
                    # e que estão diretamente na pasta (não em subpastas)
                    if (filename.endswith('.xlsx') and 
                        '/processado/' not in key and
                        key.count('/') == 1):  # Apenas um '/' = arquivo direto na pasta
                        
                        file_info = self.config.parse_filename(filename)
                        if file_info:
                            file_info.s3_key = key
                            files_info.append(file_info)
            
            return files_info
            
        except Exception as e:
            # Log warning mas não quebra o processo
            print(f"⚠️ Warning: Could not list files in folder '{folder_name}': {e}")
            return []  # Retorna lista vazia em vez de quebrar
    
    def download_file(self, s3_key: str, local_path: str) -> str:
        """
        Baixa um arquivo específico do S3.
        
        Args:
            s3_key: Chave do arquivo no S3
            local_path: Caminho local para salvar
            
        Returns:
            Caminho do arquivo baixado
        """
        try:
            s3_client = self._get_s3_client()
            
            # Cria diretório se não existe
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            s3_client.download_file(
                self.config.bucket_name,
                s3_key,
                local_path
            )
            
            return local_path
            
        except Exception as e:
            raise S3ExtractionError(
                f"Failed to download file: {s3_key}",
                bucket=self.config.bucket_name,
                key=s3_key,
                context={"local_path": local_path},
                original_exception=e
            )
    
    def download_all_files(self) -> List[S3FileInfo]:
        """
        Baixa todos os arquivos .xlsx de todas as pastas.
        
        Returns:
            Lista de informações dos arquivos baixados
        """
        downloaded_files = []
        expected_folders = self.config.list_expected_folders()
        
        print(f"📁 Verificando {len(expected_folders)} pastas: {expected_folders}")
        
        for folder in expected_folders:
            print(f"📂 Verificando pasta: {folder}")
            
            folder_files = self.list_files_in_folder(folder)
            
            if not folder_files:
                print(f"ℹ️ Pasta '{folder}' está vazia ou não possui arquivos .xlsx")
                continue
            
            print(f"📄 Encontrados {len(folder_files)} arquivos em '{folder}'")
            
            for file_info in folder_files:
                try:
                    # Define caminho local
                    local_filename = f"{folder}_{file_info.filename}"
                    local_path = os.path.join(self.temp_folder, local_filename)
                    
                    # Baixa arquivo
                    self.download_file(file_info.s3_key, local_path)
                    
                    # Atualiza informações do arquivo
                    file_info.local_path = local_path
                    downloaded_files.append(file_info)
                    
                    print(f"✅ Baixado: {file_info.filename}")
                    
                except Exception as e:
                    print(f"❌ Erro baixando {file_info.filename}: {e}")
                    # Continua com os próximos arquivos mesmo se um falhar
                    continue
        
        print(f"📊 Total de arquivos baixados: {len(downloaded_files)}")
        return downloaded_files
    
    def move_to_processed(self, s3_key: str) -> str:
        """
        Move arquivo para pasta 'processado' no S3.
        
        Args:
            s3_key: Chave atual do arquivo
            
        Returns:
            Nova chave do arquivo processado
        """
        try:
            s3_client = self._get_s3_client()
            
            # Gera nova chave para pasta processado
            new_key = self.config.get_processed_key(s3_key)
            
            # Copia arquivo para nova localização
            s3_client.copy_object(
                Bucket=self.config.bucket_name,
                CopySource={'Bucket': self.config.bucket_name, 'Key': s3_key},
                Key=new_key
            )
            
            # Remove arquivo original
            s3_client.delete_object(
                Bucket=self.config.bucket_name,
                Key=s3_key
            )
            
            return new_key
            
        except Exception as e:
            raise S3ExtractionError(
                f"Failed to move file to processed: {s3_key}",
                bucket=self.config.bucket_name,
                key=s3_key,
                original_exception=e
            )
    
    def cleanup_temp_files(self):
        """Remove todos os arquivos temporários baixados."""
        try:
            for file in os.listdir(self.temp_folder):
                if file.endswith('.xlsx'):
                    file_path = os.path.join(self.temp_folder, file)
                    os.remove(file_path)
        except Exception as e:
            # Log mas não falha o processo
            print(f"Warning: Failed to cleanup temp files: {e}")
