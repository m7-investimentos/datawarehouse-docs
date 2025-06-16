"""
Classe Tabela - Processador genérico para arquivos Excel

Esta classe fornece métodos flexíveis para transformar qualquer arquivo Excel,
permitindo aplicar diferentes transformações conforme necessário.
"""

import pandas as pd
import os
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, date
import unicodedata

from utils.exceptions import TransformationError, ValidationError


class Tabela:
    """
    Classe genérica para processamento de arquivos Excel.

    Fornece métodos flexíveis que podem ser aplicados conforme necessário
    para transformar dados de diferentes tipos de arquivo.
    """

    def __init__(self, file_path: str):
        """
        Inicializa com carregamento do arquivo Excel.

        Args:
            file_path: Caminho para o arquivo Excel
        """
        self.file_path = file_path
        self.original_columns = None
        self.df = self.load_excel()

    def load_excel(self) -> pd.DataFrame:
        """
        Carrega arquivo Excel como DataFrame.

        Returns:
            DataFrame com dados brutos

        Raises:
            TransformationError: Se não conseguir carregar o arquivo
        """
        try:
            if not os.path.exists(self.file_path):
                raise TransformationError(
                    f"File not found: {self.file_path}", file_path=self.file_path
                )

            # Carrega como string para preservar formatação
            df = pd.read_excel(self.file_path, dtype=str, engine="openpyxl")

            if df.empty:
                raise TransformationError(
                    f"Excel file is empty: {self.file_path}", file_path=self.file_path
                )

            self.original_columns = list(df.columns)
            return df

        except Exception as e:
            if isinstance(e, TransformationError):
                raise
            else:
                raise TransformationError(
                    f"Error loading Excel file: {self.file_path}",
                    file_path=self.file_path,
                    original_exception=e,
                )

    def rename_columns(self, mapping: Dict[str, str]) -> "Tabela":
        """
        Renomeia colunas conforme mapeamento fornecido.

        Args:
            mapping: Dicionário {nome_original: nome_novo}

        Returns:
            Self para permitir method chaining
        """
        # Só renomeia colunas que existem
        valid_mapping = {
            old: new for old, new in mapping.items() if old in self.df.columns
        }

        if valid_mapping:
            self.df = self.df.rename(columns=valid_mapping)

        return self

    def normalize_text_columns(self, columns: List[str]) -> "Tabela":
        """
        Normaliza texto em colunas especificadas (remove acentos, lowercase).

        Args:
            columns: Lista de nomes de colunas

        Returns:
            Self para permitir method chaining
        """
        for column in columns:
            if column in self.df.columns:
                self.df[column] = self.df[column].apply(self._normalize_text)

        return self

    def format_date_columns(self, columns: List[str]) -> "Tabela":
        """
        Formata colunas de data considerando formato brasileiro.

        Args:
            columns: Lista de nomes de colunas

        Returns:
            Self para permitir method chaining
        """
        for column in columns:
            if column in self.df.columns:
                self.df[column] = self.df[column].apply(self._format_date_br)

        return self

    def format_numeric_columns(self, columns: List[str]) -> "Tabela":
        """
        Formata colunas numéricas (substitui vírgula por ponto).

        Args:
            columns: Lista de nomes de colunas

        Returns:
            Self para permitir method chaining
        """
        for column in columns:
            if column in self.df.columns:
                self.df[column] = self.df[column].apply(self._format_numeric)

        return self

    def format_boolean_columns(self, columns: List[str]) -> "Tabela":
        """
        Converte colunas para booleano numérico (0/1).

        Args:
            columns: Lista de nomes de colunas

        Returns:
            Self para permitir method chaining
        """
        for column in columns:
            if column in self.df.columns:
                self.df[column] = self.df[column].apply(self._convert_boolean)

        return self

    def convert_boolean_columns(self, columns: List[str]) -> "Tabela":
        """
        Alias para format_boolean_columns para manter compatibilidade.

        Args:
            columns: Lista de nomes de colunas

        Returns:
            Self para permitir method chaining
        """
        return self.format_boolean_columns(columns)
    
    def clean_monetary_columns(self, columns: List[str]) -> "Tabela":
        """
        Limpa e converte colunas monetárias brasileiras (R$ 1.234,56) para float.
        
        Args:
            columns: Lista de nomes de colunas monetárias
            
        Returns:
            Self para permitir method chaining
        """
        for column in columns:
            if column in self.df.columns:
                self.df[column] = self.df[column].apply(self._clean_monetary_value)
        
        return self

    def clean_column_code(self, column: str, chars_to_remove: str = "A") -> "Tabela":
        """
        Remove caracteres específicos de uma coluna de códigos.

        Args:
            column: Nome da coluna
            chars_to_remove: Caracteres a serem removidos

        Returns:
            Self para permitir method chaining
        """
        if column in self.df.columns:
            self.df[column] = (
                self.df[column]
                .astype(str)
                .str.replace(chars_to_remove, "", regex=False)
            )

        return self

    def clean_cnpj_column(self, column: str) -> "Tabela":
        """
        Limpa coluna de CNPJ removendo caracteres não numéricos.

        Args:
            column: Nome da coluna de CNPJ

        Returns:
            Self para permitir method chaining
        """
        if column in self.df.columns:
            self.df[column] = (
                self.df[column].astype(str).str.replace(r"[^\d]", "", regex=True)
            )
            self.df[column] = self.df[column].replace("", pd.NA)

        return self

    def remove_last_rows(self, num_rows: int) -> "Tabela":
        """
        Remove as últimas N linhas do DataFrame.

        Args:
            num_rows: Número de linhas a remover

        Returns:
            Self para permitir method chaining
        """
        if num_rows > 0 and len(self.df) > num_rows:
            self.df = self.df.iloc[:-num_rows]

        return self

    def remove_empty_rows(self) -> "Tabela":
        """
        Remove linhas completamente vazias.

        Returns:
            Self para permitir method chaining
        """
        self.df = self.df.dropna(how="all")
        return self

    def remove_empty_columns(self) -> "Tabela":
        """
        Remove colunas completamente vazias.

        Returns:
            Self para permitir method chaining
        """
        self.df = self.df.dropna(axis=1, how="all")
        return self

    def trim_text_columns(self) -> "Tabela":
        """
        Remove espaços em branco no início/fim de colunas de texto.

        Returns:
            Self para permitir method chaining
        """
        string_columns = self.df.select_dtypes(include=["object"]).columns
        for col in string_columns:
            self.df[col] = self.df[col].astype(str).str.strip()
            # Substitui strings vazias por NaN
            self.df[col] = self.df[col].replace(["", "nan", "None"], pd.NA)

        return self

    def add_processing_date(self, column_name: str = "data_carga") -> "Tabela":
        """
        Adiciona coluna com data atual de processamento.

        Args:
            column_name: Nome da coluna a ser criada

        Returns:
            Self para permitir method chaining
        """
        self.df[column_name] = datetime.today().strftime("%Y-%m-%d")
        return self

    def add_reference_date(self, column_name: str = "data_ref") -> "Tabela":
        """
        Adiciona coluna com data de referência baseada no nome do arquivo.
        Extrai a data do padrão nome_arquivo_YYYY-MM-DD.xlsx

        Args:
            column_name: Nome da coluna a ser criada

        Returns:
            Self para permitir method chaining
        """
        try:
            # Extrai apenas o nome do arquivo sem caminho
            filename = os.path.basename(self.file_path)
            # Remove extensão
            filename_no_ext = os.path.splitext(filename)[0]
            # Tenta extrair data do final do nome (formato YYYY-MM-DD)
            date_str = filename_no_ext[-10:]  # Últimos 10 caracteres
            # Valida se é uma data válida
            pd.to_datetime(date_str, format="%Y-%m-%d")
            self.df[column_name] = date_str
        except:
            # Se falhar, usa data de hoje
            self.df[column_name] = datetime.today().strftime("%Y-%m-%d")
        return self

    def reorder_columns(self, column_order: List[str]) -> "Tabela":
        """
        Reordena colunas conforme lista especificada.

        Args:
            column_order: Lista com ordem desejada das colunas

        Returns:
            Self para permitir method chaining
        """
        # Só reordena colunas que existem
        existing_columns = [col for col in column_order if col in self.df.columns]
        if existing_columns:
            self.df = self.df[existing_columns]

        return self

    def filter_rows_by_column(
        self, column: str, values: List[Any], exclude: bool = False
    ) -> "Tabela":
        """
        Filtra linhas baseado em valores de uma coluna.

        Args:
            column: Nome da coluna
            values: Lista de valores para filtrar
            exclude: Se True, exclui as linhas com esses valores

        Returns:
            Self para permitir method chaining
        """
        if column in self.df.columns:
            if exclude:
                self.df = self.df[~self.df[column].isin(values)]
            else:
                self.df = self.df[self.df[column].isin(values)]

        return self

    def validate_required_columns(self, required_columns: List[str]) -> "Tabela":
        """
        Valida se colunas obrigatórias estão presentes.

        Args:
            required_columns: Lista de colunas obrigatórias

        Returns:
            Self para permitir method chaining

        Raises:
            ValidationError: Se alguma coluna obrigatória estiver ausente
        """
        missing_columns = [
            col for col in required_columns if col not in self.df.columns
        ]

        if missing_columns:
            raise ValidationError(
                f"Missing required columns: {missing_columns}",
                validation_type="required_columns",
                failed_rules=missing_columns,
                context={
                    "missing_columns": missing_columns,
                    "available_columns": list(self.df.columns),
                    "required_columns": required_columns,
                },
            )

        return self

    def get_data(self) -> pd.DataFrame:
        """
        Retorna o DataFrame atual.

        Returns:
            DataFrame processado
        """
        return self.df.copy()

    def save_csv(self, file_path: str, **kwargs) -> "Tabela":
        """
        Salva DataFrame como CSV.

        Args:
            file_path: Caminho do arquivo a salvar
            **kwargs: Argumentos adicionais para to_csv

        Returns:
            Self para permitir method chaining
        """
        # Configurações padrão
        csv_config = {
            "index": False,
            "encoding": "iso-8859-1",
            "sep": ",",
            "quotechar": '"',
        }
        csv_config.update(kwargs)

        # Cria diretório se não existir
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        self.df.to_csv(file_path, **csv_config)
        return self

    def info(self) -> Dict[str, Any]:
        """
        Retorna informações sobre o DataFrame atual.

        Returns:
            Dicionário com informações
        """
        return {
            "file_path": self.file_path,
            "shape": self.df.shape,
            "columns": list(self.df.columns),
            "original_columns": self.original_columns,
            "null_counts": self.df.isnull().sum().to_dict(),
            "dtypes": self.df.dtypes.to_dict(),
        }

    # Métodos auxiliares privados
    def _normalize_text(self, texto: Any) -> Optional[str]:
        """Normaliza texto removendo acentos e convertendo para lowercase."""
        if pd.isna(texto):
            return None

        try:
            return (
                unicodedata.normalize("NFKD", str(texto).lower())
                .encode("ASCII", "ignore")
                .decode("utf-8")
                .strip()
            )
        except:
            return None

    def _format_date_br(self, data: Any) -> Optional[date]:
        """Converte data para formato date, considerando formato brasileiro."""
        if pd.isna(data) or str(data).strip() == "":
            return None

        try:
            dt = pd.to_datetime(data, dayfirst=True, errors="coerce")
            if pd.isna(dt):
                return None
            return dt.date()
        except:
            return None

    def _format_numeric(self, valor: Any) -> Optional[float]:
        """Converte valor para float, tratando separadores decimais brasileiros."""
        if pd.isna(valor) or str(valor).strip() == "":
            return None

        try:
            valor_str = str(valor).replace(",", ".")
            return float(valor_str)
        except:
            return None

    def _convert_boolean(self, valor: Any) -> int:
        """Converte valor para booleano numérico (0 ou 1)."""
        if isinstance(valor, str):
            valor = valor.strip().lower()
            return 1 if valor in ["sim", "ativo"] else 0
        return 0
    
    def _clean_monetary_value(self, valor: Any) -> float:
        """Limpa e converte valores monetários brasileiros para float."""
        if pd.isna(valor) or valor is None:
            return 0.0
        
        valor_str = str(valor).strip()
        
        # Verifica se é um valor negativo
        negativo = "-" in valor_str
        
        # Remove R$, espaços e o próprio símbolo de negativo
        import re
        valor_str = re.sub(r"R\$|\s|-", "", valor_str)
        
        # Substituição de separadores brasileiros (1.234,56 → 1234.56)
        valor_str = valor_str.replace(".", "").replace(",", ".")
        
        try:
            resultado = float(valor_str)
            # Aplica o sinal negativo se necessário
            return -resultado if negativo else resultado
        except ValueError:
            return 0.0

    # Métodos de pós-processamento
    @staticmethod
    def filter_new_records_by_key(df: pd.DataFrame, table_name: str, engine, key_column: str) -> pd.DataFrame:
        """
        Filtra DataFrame para conter apenas registros com chaves que não existem no banco.
        Útil para inserções incrementais baseadas em chave única.
        
        Args:
            df: DataFrame a ser filtrado
            table_name: Nome completo da tabela no banco
            engine: Engine do SQLAlchemy
            key_column: Nome da coluna chave para verificar duplicatas
            
        Returns:
            DataFrame filtrado com apenas registros novos
        """
        try:
            from sqlalchemy import text
            
            if key_column not in df.columns:
                print(f"⚠️ Coluna {key_column} não encontrada. Retornando todos os registros.")
                return df
            
            # Remove possíveis duplicatas no próprio DataFrame
            df = df.drop_duplicates(subset=[key_column], keep='last')
            
            # Busca todas as chaves existentes no banco
            with engine.connect() as conn:
                query = text(f"SELECT DISTINCT {key_column} FROM {table_name}")
                result = conn.execute(query)
                existing_keys = {str(row[0]) for row in result if row[0] is not None}
            
            print(f"\n🔍 Verificando registros existentes por {key_column}...")
            print(f"   Registros no banco: {len(existing_keys)}")
            print(f"   Registros no arquivo (sem duplicatas): {len(df)}")
            
            # Converte chave para string para comparação
            df[key_column] = df[key_column].astype(str)
            
            # Filtra apenas os novos
            df_filtered = df[~df[key_column].isin(existing_keys)]
            
            print(f"   Novos registros a inserir: {len(df_filtered)}")
            
            if len(df_filtered) == 0:
                print("   ℹ️ Nenhum registro novo encontrado.")
            
            return df_filtered
            
        except Exception as e:
            print(f"⚠️ Erro ao filtrar registros: {str(e)}")
            print("   Continuando com todos os registros...")
            return df
    
    @staticmethod
    def post_load_cleanup_by_period(
        df: pd.DataFrame, table_name: str, engine, date_column: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Remove registros antigos do mesmo ano/mês que foi inserido.
        Útil para tabelas acumuladas que precisam substituir dados do mesmo período.

        Args:
            df: DataFrame que foi inserido
            table_name: Nome completo da tabela
            engine: Engine do SQLAlchemy
            date_column: Nome da coluna de data (se None, detecta automaticamente)

        Returns:
            Dict com informações sobre a limpeza
        """
        try:
            from sqlalchemy import text

            # Se não especificou coluna, tenta detectar automaticamente
            if date_column is None:
                # Lista de possíveis colunas de data em ordem de prioridade
                possible_date_columns = ['data_ref', 'ano_mes', 'data_referencia', 'periodo', 'mes_ano']
                
                for col in possible_date_columns:
                    if col in df.columns:
                        date_column = col
                        print(f"🔍 Coluna de data detectada automaticamente: {date_column}")
                        break
                
                if date_column is None:
                    print("⚠️ Nenhuma coluna de data encontrada. Pulando limpeza de registros antigos.")
                    return {"status": "skipped", "reason": "no date column found"}
            
            # Verificar se coluna de data existe
            if date_column not in df.columns:
                print(
                    f"⚠️ Coluna {date_column} não encontrada. Pulando limpeza de registros antigos."
                )
                return {"status": "skipped", "reason": f"no {date_column} column"}

            # Converter para datetime se necessário
            try:
                # Primeiro tenta converter diretamente
                df[date_column] = pd.to_datetime(df[date_column])
            except:
                # Se falhar, verifica se é formato YYYYMM (ano_mes)
                try:
                    # Converte para string e verifica se tem 6 dígitos (YYYYMM)
                    sample_value = str(df[date_column].iloc[0])
                    if len(sample_value) == 6 and sample_value.isdigit():
                        df[date_column] = pd.to_datetime(df[date_column].astype(str), format='%Y%m')
                    else:
                        raise ValueError(f"Formato de data não reconhecido: {sample_value}")
                except Exception as e:
                    print(f"⚠️ Erro ao converter coluna {date_column}: {str(e)}")
                    return {"status": "error", "reason": f"date conversion failed: {str(e)}"}

            # Extrair ano/mês únicos
            periodos = df[date_column].dt.to_period("M").unique()

            if len(periodos) == 0:
                return {"status": "skipped", "reason": "no periods found"}

            print(f"\n🧹 Iniciando limpeza de registros antigos...")
            print(f"   Períodos a limpar: {[str(p) for p in periodos]}")

            total_deleted = 0

            with engine.connect() as conn:
                with conn.begin():
                    for periodo in periodos:
                        # Extrair ano e mês
                        ano = periodo.year
                        mes = periodo.month

                        # Query para contar registros antes de deletar
                        count_query = text(
                            f"""
                            SELECT COUNT(*) 
                            FROM {table_name}
                            WHERE YEAR({date_column}) = :ano 
                            AND MONTH({date_column}) = :mes
                            AND data_carga < CAST(GETDATE() AS DATE)
                        """
                        )

                        result = conn.execute(count_query, {"ano": ano, "mes": mes})
                        count_before = result.scalar()

                        if count_before > 0:
                            # Query para deletar registros antigos do mesmo período
                            delete_query = text(
                                f"""
                                DELETE FROM {table_name}
                                WHERE YEAR({date_column}) = :ano 
                                AND MONTH({date_column}) = :mes
                                AND data_carga < CAST(GETDATE() AS DATE)
                            """
                            )

                            result = conn.execute(
                                delete_query, {"ano": ano, "mes": mes}
                            )
                            deleted = result.rowcount
                            total_deleted += deleted

                            print(
                                f"   🗑️ Período {ano}-{mes:02d}: {deleted} registros antigos removidos"
                            )

            print(
                f"   ✅ Limpeza concluída! Total removido: {total_deleted} registros\n"
            )

            return {
                "status": "success",
                "periods_cleaned": [str(p) for p in periodos],
                "total_deleted": total_deleted,
            }

        except Exception as e:
            print(f"❌ Erro na limpeza pós-carga: {str(e)}")
            # Não propagar erro - a inserção já foi feita com sucesso
            return {"status": "error", "error": str(e)}
