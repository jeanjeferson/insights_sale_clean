#!/usr/bin/env python3
"""
Script de teste para o InMemoryForecastPipeline
Demonstra o uso da nova classe em memória.
"""

import sys
from pathlib import Path

# Adicionar o diretório raiz ao PYTHONPATH
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pipelines.in_memory_forecast_pipeline import InMemoryForecastPipeline

def test_single_database():
    """Testa pipeline para um database específico."""
    print("🧪 TESTE - DATABASE ESPECÍFICO")
    print("=" * 50)
    
    # Inicializar pipeline
    pipeline = InMemoryForecastPipeline()
    
    # Testar com um database específico (ajustar conforme necessário)
    test_database = "005NO_ERP_BI"  # Exemplo
    
    print(f"Testando com database: {test_database}")
    
    try:
        # Executar pipeline
        result = pipeline.run_complete_pipeline(test_database)
        
        # Verificar resultado
        if result['success']:
            print("✅ Pipeline executado com sucesso!")
            print(f"   Database: {result['database']}")
            print(f"   Tempo: {result['elapsed_seconds']:.2f}s")
            print(f"   Arquivos enviados: {result['files_uploaded']}")
            
            # Detalhes dos resultados
            if 'results' in result:
                vendas = result['results'].get('vendas', {})
                vendas_empresa = result['results'].get('vendas_empresa', {})
                
                if vendas.get('success'):
                    print("   💰 Vendas: ✅ Processado")
                    metrics = vendas.get('metrics', {})
                    if 30 in metrics:
                        mape = metrics[30].get('MAPE', 0)
                        print(f"      MAPE: {mape:.2f}%")
                else:
                    print("   💰 Vendas: ❌ Falhou")
                
                if vendas_empresa.get('success'):
                    summary = vendas_empresa.get('summary_metrics', {})
                    companies = summary.get('companies_processed', 0)
                    avg_mape = summary.get('average_mape', 0)
                    print(f"   🏢 Vendas por Empresa: ✅ Processado")
                    print(f"      Empresas: {companies}")
                    print(f"      MAPE médio: {avg_mape:.2f}%")
                else:
                    print("   🏢 Vendas por Empresa: ❌ Falhou")
        else:
            print("❌ Pipeline falhou")
            if 'error' in result:
                print(f"   Erro: {result['error']}")
                
    except Exception as e:
        print(f"❌ Erro no teste: {e}")

def test_all_databases():
    """Testa pipeline para todos os databases configurados."""
    print("🧪 TESTE - TODOS OS DATABASES")
    print("=" * 50)
    
    # Inicializar pipeline
    pipeline = InMemoryForecastPipeline()
    
    try:
        # Executar pipeline
        results = pipeline.run_all_databases()
        
        # Verificar resultado
        if results['success']:
            print("✅ Pipeline executado com sucesso!")
            print(f"   Sucessos: {results['successful_count']}/{results['total_count']}")
            print(f"   Empresas processadas: {results['metadata']['total_companies']}")
            print(f"   Qualidade geral: {results['metadata']['overall_quality']}")
            
            # Detalhes por database
            print("\n📊 DETALHES POR DATABASE:")
            for result in results['results']:
                db_name = result['database']
                status = "✅" if result['success'] else "❌"
                elapsed = result.get('elapsed_seconds', 0)
                files = result.get('files_uploaded', 0)
                print(f"   {status} {db_name}: {elapsed:.2f}s, {files} arquivos")
        else:
            print("❌ Pipeline falhou")
            if 'error' in results:
                print(f"   Erro: {results['error']}")
                
    except Exception as e:
        print(f"❌ Erro no teste: {e}")

def test_data_loading():
    """Testa apenas o carregamento de dados."""
    print("🧪 TESTE - CARREGAMENTO DE DADOS")
    print("=" * 50)
    
    # Inicializar pipeline
    pipeline = InMemoryForecastPipeline()
    
    # Testar com um database específico
    test_database = "005NO_ERP_BI"  # Exemplo
    
    try:
        # Carregar dados
        data = pipeline.load_all_data(test_database)
        
        print(f"Database: {data['database']}")
        print(f"Carregado em: {data['loaded_at']}")
        
        # Verificar clientes
        if data['clientes']['success']:
            print("✅ Clientes: Carregado com sucesso")
        else:
            print("❌ Clientes: Falhou")
        
        # Verificar vendas
        if data['vendas']['success']:
            df = data['vendas']['data']
            print(f"✅ Vendas: {len(df)} registros")
            print(f"   Colunas: {list(df.columns)}")
            print(f"   Período: {df.iloc[0, 0]} a {df.iloc[-1, 0]}")
        else:
            print(f"❌ Vendas: {data['vendas'].get('error', 'Erro desconhecido')}")
        
        # Verificar vendas por empresa
        if data['vendas_empresa']['success']:
            df = data['vendas_empresa']['data']
            empresas = df['Empresa'].nunique()
            print(f"✅ Vendas por Empresa: {len(df)} registros, {empresas} empresas")
        else:
            print(f"❌ Vendas por Empresa: {data['vendas_empresa'].get('error', 'Erro desconhecido')}")
            
    except Exception as e:
        print(f"❌ Erro no teste de carregamento: {e}")

def test_single_database_extraction():
    """Testa apenas a extração de dados de um database específico."""
    print("🧪 TESTE - EXTRAÇÃO DE DADOS DE UM DATABASE")
    print("=" * 50)
    
    # Inicializar pipeline
    pipeline = InMemoryForecastPipeline()
    
    # Testar com um database específico
    test_database = "005NO_ERP_BI"  # Exemplo
    
    print(f"Extraindo dados para: {test_database}")
    
    try:
        # Carregar dados
        data = pipeline.load_all_data(test_database)
        
        print(f"Database: {data['database']}")
        print(f"Carregado em: {data['loaded_at']}")
        
        # Verificar clientes
        if data['clientes']['success']:
            print("✅ Clientes: Extraído com sucesso")
            print(f"   Resultado: {data['clientes']['result']}")
        else:
            print("❌ Clientes: Falhou")
        
        # Verificar vendas
        if data['vendas']['success']:
            df = data['vendas']['data']
            print(f"✅ Vendas: {len(df)} registros extraídos")
            print(f"   Colunas: {list(df.columns)}")
            if len(df) > 0:
                print(f"   Período: {df.iloc[0, 0]} a {df.iloc[-1, 0]}")
                print(f"   Valores: R$ {df.iloc[:, 1].sum():,.2f} total")
        else:
            print(f"❌ Vendas: {data['vendas'].get('error', 'Erro desconhecido')}")
        
        # Verificar vendas por empresa
        if data['vendas_empresa']['success']:
            df = data['vendas_empresa']['data']
            empresas = df['Empresa'].nunique()
            print(f"✅ Vendas por Empresa: {len(df)} registros, {empresas} empresas")
            if len(df) > 0:
                print(f"   Período: {df['data'].min()} a {df['data'].max()}")
                print(f"   Valores: R$ {df['vendas'].sum():,.2f} total")
        else:
            print(f"❌ Vendas por Empresa: {data['vendas_empresa'].get('error', 'Erro desconhecido')}")
            
    except Exception as e:
        print(f"❌ Erro no teste de extração: {e}")

def main():
    """Menu principal de testes."""
    print("🧪 TESTE DO INMEMORYFORECASTPIPELINE")
    print("=" * 50)
    print("Escolha uma opção:")
    print("1. Teste de carregamento de dados")
    print("2. Teste de extração de dados (1 database)")
    print("3. Teste de database específico (pipeline completo)")
    print("4. Teste de todos os databases")
    print("5. Executar todos os testes")
    
    try:
        choice = input("\nOpção (1-5): ").strip()
        
        if choice == "1":
            test_data_loading()
        elif choice == "2":
            test_single_database_extraction()
        elif choice == "3":
            test_single_database()
        elif choice == "4":
            test_all_databases()
        elif choice == "5":
            print("\n🔄 Executando todos os testes...")
            test_data_loading()
            print("\n" + "="*50)
            test_single_database_extraction()
            print("\n" + "="*50)
            test_single_database()
            print("\n" + "="*50)
            test_all_databases()
        else:
            print("❌ Opção inválida")
            
    except KeyboardInterrupt:
        print("\n👋 Saindo...")
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    main()
