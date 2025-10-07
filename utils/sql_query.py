"""
Extrator de Dados Simplificado
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import time
from typing import Optional, List
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from pathlib import Path
import yaml

load_dotenv()

class SimpleDataExtractor:
    """Extrator simples para consultar banco de dados e salvar CSVs."""

    def __init__(self, config_file: str = "config/config_databases.yaml"):
        """Inicializa conexão com banco de dados."""
        # Controla verbosidade de logs internos (queries, conexão, etc.)
        self.verbose = False
        self.config = self._load_config(config_file)
        self.engine = self._create_engine()
        self._ensure_dataset_dir()

    def _load_sql_queries(self) -> dict:
        """Carrega queries dos arquivos SQL da pasta sql/."""
        queries = {}
        sql_dir = Path("sql")

        if not sql_dir.exists():
            print("⚠️ Pasta sql/ não encontrada, usando queries padrão")
            return {}

        # Mapear apenas os arquivos SQL essenciais
        sql_files = {
            'vendas.sql': 'vendas',
            'volume.sql': 'volume',
            'vendas_empresa.sql': 'vendas_empresa'
        }

        for filename, query_name in sql_files.items():
            sql_file = sql_dir / filename
            if sql_file.exists():
                try:
                    with open(sql_file, "r", encoding="utf-8") as f:
                        queries[query_name] = f.read().strip()
                    if self.verbose:
                        print(f"✅ Query {query_name} carregada de {filename}")
                except Exception as e:
                    if self.verbose:
                        print(f"❌ Erro ao carregar {filename}: {e}")
            else:
                if self.verbose:
                    print(f"⚠️ Arquivo {filename} não encontrado")

        return queries

    def _load_config(self, config_file: str) -> dict:
        """Carrega configurações do arquivo YAML."""
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            sql_queries = self._load_sql_queries()
            config["queries"] = sql_queries if sql_queries else {}
            return config
        except Exception as e:
            print(f"❌ Erro ao carregar configurações: {e}")
            raise

    def _create_engine(self):
        """Cria engine SQLAlchemy com configurações do .env."""
        try:
            start = time.perf_counter()
            # Configurações do banco
            driver = os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server')
            server = os.getenv('DB_SERVER', 'localhost')
            port = os.getenv('DB_PORT', '1433')
            username = os.getenv('DB_UID')
            password = os.getenv('DB_PWD')
            
            # String de conexão ODBC
            odbc_conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server},{port};"
                f"UID={username};"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
            )
            
            # Criar engine SQLAlchemy
            quoted_conn_str = quote_plus(odbc_conn_str)
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}")
            
            elapsed = time.perf_counter() - start
            if self.verbose:
                print(f"✅ Conexão com banco de dados estabelecida (⏱️ {elapsed:.2f}s)")
            return engine
            
        except Exception as e:
            print(f"❌ Erro na conexão: {e}")
            return None

    def _ensure_dataset_dir(self):
        """Garante que a pasta dataset existe."""
        os.makedirs("dataset", exist_ok=True)

    def _execute_query(self, database: str, query: str, params: dict = None) -> pd.DataFrame:
        """Executa query em um database específico."""
        if not self.engine:
            print("❌ Engine não disponível")
            return pd.DataFrame()
        
        try:
            start_total = time.perf_counter()
            # Modificar conexão para usar database específico
            connection_str = str(self.engine.url)
            if "DATABASE=" not in connection_str:
                # Adicionar database à string de conexão
                odbc_part = connection_str.split("odbc_connect=")[1]
                new_odbc = f"{odbc_part.rstrip('%3B')};DATABASE={database}"
                connection_str = connection_str.split("odbc_connect=")[0] + f"odbc_connect={quote_plus(new_odbc)}"
                
                temp_engine = create_engine(connection_str)
            else:
                temp_engine = self.engine
            
            # Executar query
            with temp_engine.connect() as conn:
                start_query = time.perf_counter()
                df = pd.read_sql(text(query), conn, params=params)
                query_elapsed = time.perf_counter() - start_query
            
            total_elapsed = time.perf_counter() - start_total
            if self.verbose:
                print(f"🗄️ Query no DB '{database}' retornou {len(df):,} linhas (execução: {query_elapsed:.2f}s, total: {total_elapsed:.2f}s)")
            return df
            
        except Exception as e:
            print(f"❌ Erro executando query no {database}: {e}")
            return pd.DataFrame()
    
    def extract_sales_data(self, database: str, 
                          date_start: str = None, 
                          date_end: str = None,
                          custom_query: str = None) -> str:
        
        """
        Extrai dados de vendas e salva CSV.
        
        Args:
            database: Nome do database (ex: '007BE_ERP_BI')
            date_start: Data início (YYYY-MM-DD)
            date_end: Data fim (YYYY-MM-DD) 
            custom_query: Query customizada (opcional)
        
        Returns:
            Status da operação
        """
        
        # Datas padrão usando configuração
        days_back = self.config.get('extraction', {}).get('default_days_back', 730)
        if not date_start:
            date_start = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        if not date_end:
            date_end = datetime.now().strftime('%Y-%m-%d')
        
        custom_query = self.config.get('queries', {}).get('vendas')
                
        if self.verbose:
            print(f"💰 Extraindo VENDAS de {database} ({date_start} a {date_end})")
        
        # Executar query
        t0 = time.perf_counter()
        params = {"date_start": date_start, "date_end": date_end}
        df = self._execute_query(database, custom_query, params)
        
        if df.empty:
            return f"⚠️ Nenhum dado de vendas encontrado para {database}"
        
        # Configurações de saída
        separator = self.config.get('extraction', {}).get('separator', ';')
        encoding = self.config.get('extraction', {}).get('encoding', 'utf-8')
        
        # Salvar CSV de vendas
        output_file = f"dataset/{database}_vendas.csv"
        df.to_csv(output_file, index=False, sep=separator, encoding=encoding)
        elapsed_sec = time.perf_counter() - t0
        elapsed_min = elapsed_sec / 60.0
        print(f"⏱️ Tempo total extração (vendas/{database}): {elapsed_min:.2f} min")
        return f"✅ {database} VENDAS: {len(df):,} registros salvos em {output_file} (⏱️ {elapsed_min:.2f} min)"
    
    def extract_vendas_data(self, database: str, 
                          date_start: str = None, 
                          date_end: str = None,
                          custom_query: str = None) -> dict:
        """
        Extrai dados de vendas e retorna em memória (para uso com InMemoryForecastPipeline).
        
        Args:
            database: Nome do database (ex: '007BE_ERP_BI')
            date_start: Data início (YYYY-MM-DD)
            date_end: Data fim (YYYY-MM-DD) 
            custom_query: Query customizada (opcional)
        
        Returns:
            Dict com success, csv_content, error, etc.
        """
        
        # Datas padrão usando configuração
        days_back = self.config.get('extraction', {}).get('default_days_back', 730)
        if not date_start:
            date_start = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        if not date_end:
            date_end = datetime.now().strftime('%Y-%m-%d')
        
        custom_query = self.config.get('queries', {}).get('vendas')
                
        if self.verbose:
            print(f"💰 Extraindo VENDAS de {database} ({date_start} a {date_end})")
        
        try:
            # Executar query
            t0 = time.perf_counter()
            params = {"date_start": date_start, "date_end": date_end}
            df = self._execute_query(database, custom_query, params)
            
            if df.empty:
                return {
                    'success': False,
                    'error': f"Nenhum dado de vendas encontrado para {database}",
                    'csv_content': None
                }
            
            # Configurações de saída
            separator = self.config.get('extraction', {}).get('separator', ';')
            encoding = self.config.get('extraction', {}).get('encoding', 'utf-8')
            
            # Converter para CSV em memória
            csv_content = df.to_csv(index=False, sep=separator, encoding=encoding)
            elapsed_sec = time.perf_counter() - t0
            elapsed_min = elapsed_sec / 60.0
            
            if self.verbose:
                print(f"⏱️ Tempo total extração (vendas/{database}): {elapsed_min:.2f} min")
            
            return {
                'success': True,
                # 'csv_content': csv_content,
                'records_count': len(df),
                'elapsed_seconds': elapsed_sec,
                'message': f"✅ {database} VENDAS: {len(df):,} registros extraídos (⏱️ {elapsed_min:.2f} min)"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'csv_content': None
            }

    def extract_volume_data(self, database: str, 
                                date_start: str = None, 
                                date_end: str = None,
                                custom_query: str = None) -> str:
        
        """
        Extrai dados de volume consolidado (sem agrupamento por produto) e salva CSV.
        
        Args:
            database: Nome do database (ex: '007BE_ERP_BI')
            date_start: Data início (YYYY-MM-DD)
            date_end: Data fim (YYYY-MM-DD) 
            custom_query: Query customizada (opcional)
        
        Returns:
            Status da operação
        """
        
        # Datas padrão usando configuração
        days_back = self.config.get('extraction', {}).get('default_days_back', 730)
        if not date_start:
            date_start = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        if not date_end:
            date_end = datetime.now().strftime('%Y-%m-%d')
        
        # Query padrão ou customizada
        if not custom_query:
            custom_query = self.config.get('queries', {}).get('volume')
        
        if not custom_query:
            return f"❌ Query 'volume' não encontrada na configuração"
        
        if self.verbose:
            print(f"📦📊 Extraindo VOLUME CONSOLIDADO de {database} ({date_start} a {date_end})")
        
        # Executar query
        t0 = time.perf_counter()
        params = {"date_start": date_start, "date_end": date_end}
        df = self._execute_query(database, custom_query, params)
        
        if df.empty:
            return f"⚠️ Nenhum dado de volume consolidado encontrado para {database}"
        
        # Configurações de saída
        separator = self.config.get('extraction', {}).get('separator', ';')
        encoding = self.config.get('extraction', {}).get('encoding', 'utf-8')
        
        # Salvar CSV de volume consolidado
        output_file = f"dataset/{database}_volume.csv"
        df.to_csv(output_file, index=False, sep=separator, encoding=encoding)
        elapsed_sec = time.perf_counter() - t0
        elapsed_min = elapsed_sec / 60.0
        print(f"⏱️ Tempo total extração (volume/{database}): {elapsed_min:.2f} min")
        return f"✅ {database} VOLUME: {len(df):,} registros salvos em {output_file} (⏱️ {elapsed_min:.2f} min)"

    def extract_vendas_empresa_data(self, database: str, 
                                 date_start: str = None, 
                                 date_end: str = None,
                                 custom_query: str = None) -> str:
        
        """Extrai dados de vendas por empresa e salva em CSV."""
        # Datas padrão usando configuração
        days_back = self.config.get('extraction', {}).get('default_days_back', 730)
        if not date_start:
            date_start = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        if not date_end:
            date_end = datetime.now().strftime('%Y-%m-%d')
        
        # Query padrão ou customizada
        if not custom_query:
            custom_query = self.config.get('queries', {}).get('vendas_empresa')
        
        if not custom_query:
            return f"❌ Query 'vendas_empresa' não encontrada na configuração"
        
        if self.verbose:
            print(f"💰📊 Extraindo VENDAS POR EMPRESA de {database} ({date_start} a {date_end})")
        
        # Executar query
        t0 = time.perf_counter()
        params = {"date_start": date_start, "date_end": date_end}
        df = self._execute_query(database, custom_query, params)
        
        if df.empty:
            return f"⚠️ Nenhum dado de vendas por empresa encontrado para {database}"
        
        # Configurações de saída
        separator = self.config.get('extraction', {}).get('separator', ';')
        encoding = self.config.get('extraction', {}).get('encoding', 'utf-8')
        
        # Salvar CSV de vendas por empresa
        output_file = f"dataset/{database}_vendas_empresa.csv"
        df.to_csv(output_file, index=False, sep=separator, encoding=encoding)
        elapsed_sec = time.perf_counter() - t0
        elapsed_min = elapsed_sec / 60.0
        print(f"⏱️ Tempo total extração (vendas_empresa/{database}): {elapsed_min:.2f} min")
        return f"✅ {database} VENDAS GRUPO: {len(df):,} registros salvos em {output_file} (⏱️ {elapsed_min:.2f} min)"
    
    def extract_vendas_empresa_data(self, database: str, 
                                 date_start: str = None, 
                                 date_end: str = None,
                                 custom_query: str = None) -> dict:
        """
        Extrai dados de vendas por empresa e retorna em memória (para uso com InMemoryForecastPipeline).
        
        Args:
            database: Nome do database (ex: '007BE_ERP_BI')
            date_start: Data início (YYYY-MM-DD)
            date_end: Data fim (YYYY-MM-DD) 
            custom_query: Query customizada (opcional)
        
        Returns:
            Dict com success, csv_content, error, etc.
        """
        
        # Datas padrão usando configuração
        days_back = self.config.get('extraction', {}).get('default_days_back', 730)
        if not date_start:
            date_start = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        if not date_end:
            date_end = datetime.now().strftime('%Y-%m-%d')
        
        # Query padrão ou customizada
        if not custom_query:
            custom_query = self.config.get('queries', {}).get('vendas_empresa')
        
        if not custom_query:
            return {
                'success': False,
                'error': "Query 'vendas_empresa' não encontrada na configuração",
                'csv_content': None
            }
        
        if self.verbose:
            print(f"💰📊 Extraindo VENDAS POR EMPRESA de {database} ({date_start} a {date_end})")
        
        try:
            # Executar query
            t0 = time.perf_counter()
            params = {"date_start": date_start, "date_end": date_end}
            df = self._execute_query(database, custom_query, params)
            
            if df.empty:
                return {
                    'success': False,
                    'error': f"Nenhum dado de vendas por empresa encontrado para {database}",
                    'csv_content': None
                }
            
            # Configurações de saída
            separator = self.config.get('extraction', {}).get('separator', ';')
            encoding = self.config.get('extraction', {}).get('encoding', 'utf-8')
            
            # Converter para CSV em memória
            csv_content = df.to_csv(index=False, sep=separator, encoding=encoding)
            elapsed_sec = time.perf_counter() - t0
            elapsed_min = elapsed_sec / 60.0
            
            if self.verbose:
                print(f"⏱️ Tempo total extração (vendas_empresa/{database}): {elapsed_min:.2f} min")
            
            return {
                'success': True,
                # 'csv_content': csv_content,
                'records_count': len(df),
                'elapsed_seconds': elapsed_sec,
                'message': f"✅ {database} VENDAS GRUPO: {len(df):,} registros extraídos (⏱️ {elapsed_min:.2f} min)"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'csv_content': None
            }

def extract_all_clients(data_type: str = "all"):
    """Função auxiliar para extrair dados de todos os clientes."""
    extractor = SimpleDataExtractor()
    databases = extractor.config.get("databases", [])
    results = {}
    for database_name in databases:
        if data_type == 'vendas':
            results[database_name] = extractor.extract_sales_data(database_name)
        # elif data_type == 'volume':
        #     results[database_name] = extractor.extract_volume_data(database_name)
        elif data_type == 'vendas_empresa':
            results[database_name] = extractor.extract_vendas_empresa_data(database_name)
        elif data_type == 'all':
            results[database_name] = {
                'vendas': extractor.extract_sales_data(database_name),
                # 'volume': extractor.extract_volume_data(database_name),
                'vendas_empresa': extractor.extract_vendas_empresa_data(database_name)
            }
    return results

def extract_all_sales():
    """Função de conveniência para extrair apenas vendas."""
    return extract_all_clients(data_type="vendas")

def extract_all_volume():
    """Função de conveniência para extrair apenas volume."""
    return extract_all_clients(data_type="volume")

def extract_all_vendas_empresa():
    """Função de conveniência para extrair apenas vendas por empresa."""
    return extract_all_clients(data_type="vendas_empresa")

if __name__ == '__main__':
    print("🚀 Iniciando extração de dados...")
    extractor = SimpleDataExtractor()
    database = "007BE_ERP_BI"
    # sales_result = extractor.extract_sales_data(database)
    # print(f"\n{sales_result}")
    # volume_result = extractor.extract_volume_data(database)
    # print(f"\n{volume_result}")
    # vendas_empresa_result = extractor.extract_vendas_empresa_data(database)
    # print(f"\n{vendas_empresa_result}")
    
    results = extract_all_clients('all')
    # results = extract_all_vendas_empresa()
    print("\\n✅ Resultados:")
    print(results)