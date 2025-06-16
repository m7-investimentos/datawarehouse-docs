"""
Exceções customizadas para o sistema ETL S3 -> SQL Server

Este módulo define todas as exceções específicas do sistema ETL,
permitindo tratamento de erros mais granular e debugging eficiente.
"""

from typing import Optional, Dict, Any


class ETLException(Exception):
    """
    Exceção base para todos os erros do sistema ETL.
    
    Todas as outras exceções do sistema devem herdar desta classe.
    Fornece funcionalidades comuns como logging e contexto de erro.
    """
    
    def __init__(
        self, 
        message: str, 
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        self.message = message
        self.error_code = error_code
        self.context = context or {}
        self.original_exception = original_exception
        
        # Mensagem completa para o Exception
        full_message = f"[{error_code}] {message}" if error_code else message
        super().__init__(full_message)
    
    def get_context_info(self) -> str:
        """Retorna informações de contexto formatadas"""
        if not self.context:
            return ""
        
        context_items = [f"{k}: {v}" for k, v in self.context.items()]
        return f" | Context: {', '.join(context_items)}"


class ConfigurationError(ETLException):
    """
    Erro de configuração do sistema.
    
    Levantado quando há problemas com:
    - Variáveis de ambiente faltando
    - Configurações inválidas
    - Credenciais incorretas
    """
    
    def __init__(self, message: str, config_type: str = "general", **kwargs):
        super().__init__(
            message, 
            error_code=f"CONFIG_{config_type.upper()}_ERROR",
            **kwargs
        )


class S3ExtractionError(ETLException):
    """
    Erro durante extração de arquivos do S3.
    
    Levantado quando há problemas com:
    - Conexão com S3
    - Arquivos não encontrados
    - Permissões insuficientes
    - Download de arquivos
    """
    
    def __init__(self, message: str, bucket: str = None, key: str = None, **kwargs):
        context = kwargs.pop('context', {})
        if bucket:
            context['bucket'] = bucket
        if key:
            context['key'] = key
            
        super().__init__(
            message,
            error_code="S3_EXTRACTION_ERROR",
            context=context,
            **kwargs
        )


class TransformationError(ETLException):
    """
    Erro durante transformação de dados.
    
    Levantado quando há problemas com:
    - Leitura de arquivos Excel
    - Validação de dados
    - Transformações de negócio
    - Limpeza de dados
    """
    
    def __init__(
        self, 
        message: str, 
        file_path: str = None, 
        transformer_type: str = None,
        **kwargs
    ):
        context = kwargs.pop('context', {})
        if file_path:
            context['file_path'] = file_path
        if transformer_type:
            context['transformer_type'] = transformer_type
            
        super().__init__(
            message,
            error_code="TRANSFORMATION_ERROR",
            context=context,
            **kwargs
        )


class ValidationError(ETLException):
    """
    Erro de validação de dados.
    
    Levantado quando dados não atendem critérios de validação:
    - Schema incorreto
    - Tipos de dados inválidos
    - Regras de negócio violadas
    - Dados obrigatórios faltando
    """
    
    def __init__(
        self, 
        message: str, 
        validation_type: str = None,
        failed_rules: list = None,
        **kwargs
    ):
        context = kwargs.pop('context', {})
        if validation_type:
            context['validation_type'] = validation_type
        if failed_rules:
            context['failed_rules'] = failed_rules
            
        super().__init__(
            message,
            error_code="VALIDATION_ERROR",
            context=context,
            **kwargs
        )


class DatabaseLoadError(ETLException):
    """
    Erro durante carregamento no banco de dados.
    
    Levantado quando há problemas com:
    - Conexão com SQL Server
    - Execução de queries
    - Constraints violadas
    - Deadlocks ou timeouts
    """
    
    def __init__(
        self, 
        message: str, 
        table_name: str = None,
        operation: str = None,
        **kwargs
    ):
        context = kwargs.pop('context', {})
        if table_name:
            context['table_name'] = table_name
        if operation:
            context['operation'] = operation
            
        super().__init__(
            message,
            error_code="DATABASE_LOAD_ERROR",
            context=context,
            **kwargs
        )


class FileManagementError(ETLException):
    """
    Erro durante gerenciamento de arquivos.
    
    Levantado quando há problemas com:
    - Criação/exclusão de diretórios
    - Movimentação de arquivos
    - Permissões de arquivo
    - Limpeza de arquivos temporários
    """
    
    def __init__(self, message: str, file_path: str = None, operation: str = None, **kwargs):
        context = kwargs.pop('context', {})
        if file_path:
            context['file_path'] = file_path
        if operation:
            context['operation'] = operation
            
        super().__init__(
            message,
            error_code="FILE_MANAGEMENT_ERROR",
            context=context,
            **kwargs
        )


class ETLTimeoutError(ETLException):
    """
    Erro de timeout durante operações ETL.
    
    Levantado quando operações excedem tempo limite:
    - Downloads do S3 muito lentos
    - Queries de banco muito demoradas
    - Processamento de arquivos grandes
    """
    
    def __init__(self, message: str, timeout_seconds: int = None, operation: str = None, **kwargs):
        context = kwargs.pop('context', {})
        if timeout_seconds:
            context['timeout_seconds'] = timeout_seconds
        if operation:
            context['operation'] = operation
            
        super().__init__(
            message,
            error_code="ETL_TIMEOUT_ERROR",
            context=context,
            **kwargs
        )
