import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine, text
from typing import Optional, Dict, Any
import pyodbc
from config.database_config import get_database_config, LoadStrategy, TableConfig
from utils.exceptions import DatabaseLoadError


class SQLServerLoader:
    def __init__(self):
        self.config = get_database_config()
        self.engine = None

    def _get_engine(self):
        if self.engine is None:
            try:
                connection_url = self.config.get_sqlalchemy_url()
                self.engine = create_engine(
                    connection_url,
                    fast_executemany=True,
                    pool_pre_ping=True,
                    pool_recycle=300,
                    pool_size=10,
                    max_overflow=20,
                    echo=False,
                )
            except Exception as e:
                raise DatabaseLoadError(
                    "Failed to create optimized database engine",
                    context={"connection_url": "***hidden***"},
                    original_exception=e,
                )
        return self.engine

    def test_connection(self) -> bool:
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.fetchone()[0] == 1
        except Exception as e:
            raise DatabaseLoadError(
                "Database connection test failed", original_exception=e
            )

    def load_data(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        try:
            table_config = self.config.get_table_config(table_name)
            full_table_name = self.config.get_full_table_name(table_name)

            if table_config.load_strategy == LoadStrategy.TRUNCATE_LOAD:
                return self._truncate_and_load(df, table_name, table_config)
            elif table_config.load_strategy == LoadStrategy.INCREMENTAL:
                return self._incremental_load(df, table_name, table_config)
            elif table_config.load_strategy == LoadStrategy.UPSERT:
                return self._upsert_load(df, table_name, table_config)
            elif table_config.load_strategy == LoadStrategy.APPEND:
                return self._append_load(df, table_name, table_config)
            else:
                raise DatabaseLoadError(
                    f"Unsupported load strategy: {table_config.load_strategy}",
                    table_name=table_name,
                    operation="load_data",
                )
        except Exception as e:
            if isinstance(e, DatabaseLoadError):
                raise
            raise DatabaseLoadError(
                f"Error loading data to table: {table_name}",
                table_name=table_name,
                operation="load_data",
                context={"dataframe_shape": df.shape},
                original_exception=e,
            )

    def _truncate_and_load(
        self, df: pd.DataFrame, table_name: str, config: TableConfig
    ) -> Dict[str, Any]:
        try:
            engine = self._get_engine()
            with engine.connect() as connection:
                with connection.begin():
                    connection.execute(
                        text(
                            f"TRUNCATE TABLE {self.config.get_full_table_name(table_name)}"
                        )
                    )
                    rows_inserted = self._insert_dataframe(df, table_name)

            return {
                "strategy": "truncate_load",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "rows_deleted": "all",
                "status": "success",
            }
        except Exception as e:
            raise DatabaseLoadError(
                f"Truncate and load failed for table: {table_name}",
                table_name=table_name,
                operation="truncate_load",
                original_exception=e,
            )

    def _incremental_load(
        self, df: pd.DataFrame, table_name: str, config: TableConfig
    ) -> Dict[str, Any]:
        try:
            # _insert_dataframe agora já cria sua própria conexão
            rows_inserted = self._insert_dataframe(df, table_name)

            return {
                "strategy": "incremental",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "status": "success",
            }
        except Exception as e:
            raise DatabaseLoadError(
                f"Incremental load failed for table: {table_name}",
                table_name=table_name,
                operation="incremental_load",
                original_exception=e,
            )

    def _append_load(
        self, df: pd.DataFrame, table_name: str, config: TableConfig
    ) -> Dict[str, Any]:
        try:
            print(f"\n📊 DEBUG - DataFrame info:")
            print(f"   Shape: {df.shape}")
            print(f"   Columns: {list(df.columns)}")
            print(f"   Types: {df.dtypes.to_dict()}")
            print(f"\n📋 Primeiras 3 linhas:")
            print(df.head(3).to_string())

            # _insert_dataframe agora já cria sua própria conexão
            rows_inserted = self._insert_dataframe(df, table_name)

            return {
                "strategy": "append",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "status": "success",
            }
        except Exception as e:
            import traceback

            print("💥 DEBUG - Erro detalhado:", str(e))
            traceback.print_exc()
            raise DatabaseLoadError(
                f"Append load failed for table: {table_name}",
                table_name=table_name,
                operation="append_load",
                original_exception=e,
            )

    def _upsert_load(
        self, df: pd.DataFrame, table_name: str, config: TableConfig
    ) -> Dict[str, Any]:
        raise DatabaseLoadError(
            f"Upsert strategy not yet implemented for table: {table_name}",
            table_name=table_name,
            operation="upsert_load",
        )

    def _insert_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        """Insere DataFrame usando pyodbc diretamente"""
        try:
            full_table_name = self.config.get_full_table_name(table_name)
            conn_str = self.config.get_connection_string()
            
            # Conectar usando pyodbc diretamente
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                
                # Obter colunas
                columns = df.columns.tolist()
                
                # Criar placeholders para valores
                placeholders = ', '.join(['?' for _ in columns])
                columns_str = ', '.join([f'[{col}]' for col in columns])
                
                # Query de inserção
                insert_query = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders})"
                
                # Converter DataFrame para lista de tuplas
                # Converter <NA> para None para compatibilidade com pyodbc
                data_tuples = []
                for row in df.itertuples(index=False, name=None):
                    # Converter pd.NA para None
                    converted_row = tuple(None if pd.isna(val) else val for val in row)
                    data_tuples.append(converted_row)
                
                # Inserir em lotes
                batch_size = self.config.get_table_config(table_name).batch_size
                total_inserted = 0
                total_rows = len(data_tuples)
                
                print(f"\n📊 Iniciando inserção em {full_table_name}")
                print(f"   Total de linhas: {total_rows}")
                print(f"   Tamanho do lote: {batch_size}")
                
                for i in range(0, len(data_tuples), batch_size):
                    batch = data_tuples[i:i + batch_size]
                    cursor.executemany(insert_query, batch)
                    total_inserted += len(batch)
                    
                    # Commit a cada lote
                    conn.commit()
                    
                    # Progress mais frequente
                    remaining = total_rows - total_inserted
                    progress = (total_inserted / total_rows) * 100
                    print(f"   💾 Inseridas {total_inserted}/{total_rows} linhas ({progress:.1f}%) - Faltam: {remaining}")
                
                cursor.close()
                
                print(f"   ✅ Inserção concluída! Total: {total_inserted} linhas\n")
            
            return total_inserted
            
        except Exception as e:
            raise DatabaseLoadError(
                f"[DATABASE_LOAD_ERROR] Failed to bulk insert DataFrame to table: {full_table_name}",
                context={"error": str(e), "columns": list(df.columns)},
            )

    def get_table_row_count(self, table_name: str) -> int:
        try:
            full_table_name = self.config.get_full_table_name(table_name)
            engine = self._get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {full_table_name}"))
                return result.fetchone()[0]
        except Exception as e:
            raise DatabaseLoadError(
                f"Failed to get row count for table: {table_name}",
                table_name=table_name,
                operation="get_row_count",
                original_exception=e,
            )

    def validate_data_loaded(self, df: pd.DataFrame, table_name: str) -> bool:
        try:
            current_count = self.get_table_row_count(table_name)
            expected_min = len(df)
            return current_count >= expected_min
        except Exception as e:
            raise DatabaseLoadError(
                f"Data validation failed for table: {table_name}",
                table_name=table_name,
                operation="validate_data",
                original_exception=e,
            )
