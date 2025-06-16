"""
Configuração de conexão e operações com SQL Server

Este módulo centraliza todas as configurações relacionadas ao banco de dados,
incluindo string de conexão, configurações de tabelas e estratégias de carregamento.
"""

import os
from typing import Dict, Optional, Any
from dataclasses import dataclass
from enum import Enum
from dotenv import load_dotenv

from utils.exceptions import ConfigurationError

# Carrega variáveis de ambiente
load_dotenv()


class LoadStrategy(Enum):
    """Estratégias de carregamento de dados"""

    TRUNCATE_LOAD = "truncate_load"  # Apaga tudo e insere
    INCREMENTAL = "incremental"  # Adiciona novos registros
    UPSERT = "upsert"  # Insert ou Update baseado em chave
    APPEND = "append"  # Apenas insere novos dados (sem truncate)


@dataclass
class TableConfig:
    """
    Configuração específica de uma tabela.

    Define como cada tabela deve ser tratada durante o processo ETL.
    """

    name: str  # Nome da tabela no banco
    load_strategy: LoadStrategy  # Estratégia de carregamento
    primary_key: Optional[str] = None  # Chave primária (para upsert/incremental)
    batch_size: int = 1000  # Tamanho do lote para inserção
    timeout_seconds: int = 300  # Timeout para operações
    truncate_before_load: bool = False  # Se deve truncar antes (para TRUNCATE_LOAD)

    def __post_init__(self):
        """Validações após inicialização"""
        if self.load_strategy in [LoadStrategy.UPSERT, LoadStrategy.INCREMENTAL]:
            if not self.primary_key:
                raise ConfigurationError(
                    f"Primary key is required for {self.load_strategy.value} strategy",
                    config_type="table",
                    context={"table_name": self.name},
                )


class SQLServerConfig:
    """
    Configuração centralizada para conexão com SQL Server.

    Gerencia connection strings, configurações de tabelas e validações.
    """

    def __init__(self):
        self._load_environment_variables()
        self._setup_table_configurations()
        self._validate_configuration()

    def _load_environment_variables(self):
        """Carrega e valida variáveis de ambiente"""
        self.server = os.getenv("SQL_SERVER")
        self.database = os.getenv("SQL_DATABASE")
        self.username = os.getenv("SQL_USERNAME")
        self.password = os.getenv("SQL_PASSWORD")
        self.schema = os.getenv("SQL_SCHEMA", "bronze")  # Default: bronze

        # Configurações opcionais com defaults
        self.port = os.getenv("SQL_PORT", "1433")
        self.driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
        self.connection_timeout = int(os.getenv("SQL_CONNECTION_TIMEOUT", "30"))
        self.command_timeout = int(os.getenv("SQL_COMMAND_TIMEOUT", "300"))

    def _setup_table_configurations(self):
        """
        Inicializa configurações de tabelas com exemplo de diversificação.

        As configurações específicas serão adicionadas posteriormente
        conforme as especificações de cada tabela.
        """
        self.table_configs = {
            # Configuração para xp_diversificacao
            "xp_diversificacao": TableConfig(
                name="xp_diversificacao",
                load_strategy=LoadStrategy.APPEND,
                batch_size=5000,
                timeout_seconds=600,
            )
        }

    def _validate_configuration(self):
        """Valida se todas as configurações obrigatórias estão presentes"""
        required_fields = ["server", "database", "username", "password"]
        missing_fields = []

        for field in required_fields:
            if not getattr(self, field):
                missing_fields.append(field.upper())

        if missing_fields:
            raise ConfigurationError(
                f"Missing required database configuration: {', '.join(missing_fields)}",
                config_type="database",
                context={"missing_fields": missing_fields},
            )

    def get_connection_string(self, use_trusted_connection: bool = False) -> str:
        """
        Gera string de conexão para SQL Server.

        Args:
            use_trusted_connection: Se deve usar autenticação Windows

        Returns:
            String de conexão formatada
        """
        if use_trusted_connection:
            conn_str = (
                f"Driver={{{self.driver}}};"
                f"Server={self.server},{self.port};"
                f"Database={self.database};"
                f"Trusted_Connection=yes;"
                f"Connection Timeout={self.connection_timeout};"
            )
        else:
            conn_str = (
                f"Driver={{{self.driver}}};"
                f"Server={self.server},{self.port};"
                f"Database={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"Connection Timeout={self.connection_timeout};"
            )

        return conn_str

    def get_sqlalchemy_url(self, use_trusted_connection: bool = False) -> str:
        """
        Gera URL de conexão para SQLAlchemy.

        Args:
            use_trusted_connection: Se deve usar autenticação Windows

        Returns:
            URL de conexão para SQLAlchemy
        """
        from urllib.parse import quote_plus

        conn_str = self.get_connection_string(use_trusted_connection)
        return f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"

    def get_table_config(self, table_name: str) -> TableConfig:
        """
        Retorna configuração específica de uma tabela.

        Args:
            table_name: Nome da tabela

        Returns:
            Configuração da tabela

        Raises:
            ConfigurationError: Se tabela não configurada
        """
        if table_name not in self.table_configs:
            raise ConfigurationError(
                f"No configuration found for table: {table_name}",
                config_type="table",
                context={
                    "table_name": table_name,
                    "available_tables": list(self.table_configs.keys()),
                },
            )

        return self.table_configs[table_name]

    def get_full_table_name(self, table_name: str) -> str:
        """
        Retorna nome completo da tabela com schema.

        Args:
            table_name: Nome da tabela

        Returns:
            Nome completo: schema.table_name
        """
        return f"{self.schema}.{table_name}"

    def add_table_config(self, table_name: str, config: TableConfig):
        """
        Adiciona nova configuração de tabela.

        Args:
            table_name: Nome da tabela
            config: Configuração da tabela
        """
        self.table_configs[table_name] = config

    def list_configured_tables(self) -> Dict[str, Dict[str, Any]]:
        """
        Lista todas as tabelas configuradas com suas estratégias.

        Returns:
            Dicionário com informações das tabelas
        """
        return {
            table_name: {
                "full_name": self.get_full_table_name(table_name),
                "load_strategy": config.load_strategy.value,
                "primary_key": config.primary_key,
                "batch_size": config.batch_size,
            }
            for table_name, config in self.table_configs.items()
        }


# Instância global de configuração (singleton pattern)
_db_config_instance = None


def get_database_config() -> SQLServerConfig:
    """
    Retorna instância singleton da configuração do banco.

    Returns:
        Instância de SQLServerConfig
    """
    global _db_config_instance
    if _db_config_instance is None:
        _db_config_instance = SQLServerConfig()
    return _db_config_instance
