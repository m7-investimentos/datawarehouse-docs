"""
ETL Principal - Genérico e Escalável

Sistema que descobre automaticamente processadores por convenção:
- processors/diversificacao.py → processar_diversificacao()
- processors/vendas.py → processar_vendas()
- etc.
"""

import os
import sys
import importlib
from datetime import datetime

# Adiciona diretório base e processors ao sys.path
base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(base_dir)
sys.path.append(os.path.join(base_dir, "processors"))

from extractors.s3_extractor import S3Extractor
from processors.tabela import Tabela
from utils.helpers import inserir_tabela_no_banco
from config.database_config import LoadStrategy


def descobrir_processadores():
    """
    Descobre automaticamente processadores disponíveis por convenção de nome.
    Returns:
        Dict com {folder_name: função_processadora}
    """
    processadores = {}
    processors_dir = os.path.join(base_dir, "processors")

    if os.path.exists(processors_dir):
        for filename in os.listdir(processors_dir):
            if (
                filename.endswith(".py")
                and not filename.startswith("__")
                and filename != "tabela.py"
            ):
                module_name = filename[:-3]

                try:
                    module = importlib.import_module(f"processors.{module_name}")
                    function_name = f"processar_{module_name}"

                    if hasattr(module, function_name):
                        processadores[module_name.lower()] = getattr(
                            module, function_name
                        )
                        print(f"📦 Processador descoberto: {module_name}")
                    else:
                        print(
                            f"⚠️ Função {function_name} não encontrada em {module_name}"
                        )

                except Exception as e:
                    print(f"⚠️ Erro importando {module_name}: {e}")

    return processadores


def obter_configuracoes_processador(folder_name):
    """
    Obtém configurações específicas do processador.
    """
    try:
        module = importlib.import_module(f"processors.{folder_name}")
        return {
            "nome_tabela": getattr(module, "NOME_TABELA", f"xp_{folder_name}"),
            "load_strategy": getattr(module, "LOAD_STRATEGY", LoadStrategy.APPEND),
            "batch_size": getattr(module, "BATCH_SIZE", 3000),
            "pre_load_func": getattr(module, "PRE_LOAD_FUNCTION", None),
        }
    except Exception:
        return {
            "nome_tabela": f"xp_{folder_name}",
            "load_strategy": LoadStrategy.APPEND,
            "batch_size": 3000,
            "pre_load_func": None,
        }


def processar_arquivo(file_info, processadores):
    """
    Processa arquivo usando processador específico ou genérico.
    """
    try:
        folder_name = file_info.folder_name.lower().strip()
        
        # Tratamento especial para arquivos de contas (contas_h, contas_e, contas_a)
        if folder_name.startswith('contas_'):
            folder_name = 'contas'
        
        file_path = file_info.local_path
        filename = file_info.filename

        print(f"📄 Processando: {filename} (pasta: {folder_name})")

        if folder_name in processadores:
            print(f"🎯 Usando processador específico: {folder_name}")
            processador_func = processadores[folder_name]
            tabela_processada = processador_func(file_path)
            config = obter_configuracoes_processador(folder_name)
        else:
            print(f"🔧 Usando processamento genérico: {folder_name}")
            tabela = Tabela(file_path)

            print(f"\n📊 DEBUG - DataFrame ORIGINAL:")
            df_original = tabela.get_data()
            print(f"   Shape: {df_original.shape}")
            print(f"   Columns: {list(df_original.columns)}")
            print(f"   Types: {df_original.dtypes.to_dict()}")
            print(f"\n📋 Primeiras 3 linhas ORIGINAIS:")
            print(df_original.head(3).to_string())

            tabela_processada = (
                tabela.remove_empty_rows().trim_text_columns().add_processing_date()
            )

            print(f"\n📊 DEBUG - DataFrame PROCESSADO:")
            df_processado = tabela_processada.get_data()
            print(f"   Shape: {df_processado.shape}")
            print(f"   Columns: {list(df_processado.columns)}")
            print(f"   Types: {df_processado.dtypes.to_dict()}")
            print(f"\n📋 Primeiras 3 linhas PROCESSADAS:")
            print(df_processado.head(3).to_string())

            config = obter_configuracoes_processador(folder_name)

        resultado = inserir_tabela_no_banco(
            tabela_processada,
            config["nome_tabela"],
            config["load_strategy"],
            config["batch_size"],
            config["pre_load_func"],  # Passa a função de pré-processamento
        )

        print(f"✅ {filename}: {resultado['rows_inserted']} linhas inseridas")
        
        # Executar função de pós-processamento se existir
        try:
            module = importlib.import_module(f"processors.{folder_name}")
            if hasattr(module, "POST_LOAD_FUNCTION"):
                print(f"🔄 Executando pós-processamento para {folder_name}...")
                from loaders.sql_server_loader import SQLServerLoader
                loader = SQLServerLoader()
                engine = loader._get_engine()
                
                post_func = getattr(module, "POST_LOAD_FUNCTION")
                full_table_name = loader.config.get_full_table_name(config["nome_tabela"])
                post_result = post_func(tabela_processada.get_data(), full_table_name, engine)
                
                if post_result.get("status") == "success":
                    print(f"✅ Pós-processamento concluído: {post_result}")
                else:
                    print(f"⚠️ Pós-processamento com aviso: {post_result}")
        except Exception as e:
            print(f"⚠️ Erro no pós-processamento (dados já inseridos): {e}")
        
        return True

    except Exception as e:
        print(f"❌ Erro processando {filename}: {e}")
        return False


def main():
    """Função principal - genérica e escalável."""
    inicio = datetime.now()

    print("🚀 ETL Pipeline Genérico")
    print("=" * 40)

    try:
        print("🔍 Descobrindo processadores...")
        processadores = descobrir_processadores()

        if not processadores:
            print("ℹ️ Nenhum processador específico encontrado (usará genérico)")

        print("\n📥 Extraindo arquivos do S3...")
        extractor = S3Extractor("./temp")
        arquivos = extractor.download_all_files()

        if not arquivos:
            print("ℹ️ Nenhum arquivo encontrado")
            return 0

        print(f"📁 {len(arquivos)} arquivos baixados")

        sucessos, falhas = 0, 0

        print(f"\n⚙️ Processando arquivos...")
        for arquivo in arquivos:
            if processar_arquivo(arquivo, processadores):
                sucessos += 1
            else:
                falhas += 1

        print(f"\n📦 Movendo arquivos processados...")
        for arquivo in arquivos:
            try:
                extractor.move_to_processed(arquivo.s3_key)
                print(f"✅ {arquivo.filename} movido para processado")
            except Exception as e:
                print(f"⚠️ {arquivo.filename} não foi movido: {e}")

        print(f"\n🧹 Limpando temporários...")
        extractor.cleanup_temp_files()

        fim = datetime.now()
        duracao = (fim - inicio).total_seconds()

        print("\n" + "=" * 40)
        print("📊 RELATÓRIO FINAL")
        print("=" * 40)
        print(f"🔧 Processadores: {', '.join(processadores.keys()) or 'Genérico'}")
        print(f"⏱️ Duração: {duracao:.2f}s")
        print(f"📁 Sucessos: {sucessos}")
        print(f"❌ Falhas: {falhas}")
        print("✅ Pipeline concluído!")

        return 0 if falhas == 0 else 1

    except Exception as e:
        print(f"\n💥 ERRO FATAL: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
