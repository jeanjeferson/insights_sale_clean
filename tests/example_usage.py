#!/usr/bin/env python3
"""
Exemplo de uso do InMemoryForecastPipeline
Demonstra diferentes formas de usar o novo pipeline em memória.
"""

import sys
from pathlib import Path

# Adicionar o diretório raiz ao PYTHONPATH
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pipelines.in_memory_forecast_pipeline import InMemoryForecastPipeline

def example_single_database():
    """Exemplo: Processar um database específico."""
    print("📝 EXEMPLO: Database Específico")
    print("=" * 50)
    
    # Inicializar pipeline
    pipeline = InMemoryForecastPipeline()
    
    # Processar um database específico
    database = "005NO_ERP_BI"  # Substitua pelo seu database
    
    print(f"Processando database: {database}")
    
    # Executar pipeline completo
    result = pipeline.run_complete_pipeline(database)
    
    # Verificar resultado
    if result['success']:
        print("✅ Sucesso!")
        print(f"   Tempo: {result['elapsed_seconds']:.2f}s")
        print(f"   Arquivos enviados: {result['files_uploaded']}")
        
        # Detalhes dos resultados
        results = result['results']
        
        # Vendas
        if results['vendas']['success']:
            print("   💰 Vendas: Processado com sucesso")
            metrics = results['vendas']['metrics']
            for horizon, metric in metrics.items():
                mape = metric.get('MAPE', 0)
                print(f"      {horizon} dias - MAPE: {mape:.2f}%")
        else:
            print("   💰 Vendas: Falhou")
        
        # Vendas por Empresa
        if results['vendas_empresa']['success']:
            summary = results['vendas_empresa']['summary_metrics']
            print(f"   🏢 Vendas por Empresa: {summary['companies_processed']} empresas")
            print(f"      MAPE médio: {summary['average_mape']:.2f}%")
        else:
            print("   🏢 Vendas por Empresa: Falhou")
    else:
        print("❌ Falhou")
        print(f"   Erro: {result.get('error', 'Desconhecido')}")

def example_all_databases():
    """Exemplo: Processar todos os databases configurados."""
    print("📝 EXEMPLO: Todos os Databases")
    print("=" * 50)
    
    # Inicializar pipeline
    pipeline = InMemoryForecastPipeline()
    
    # Executar para todos os databases
    results = pipeline.run_all_databases()
    
    # Verificar resultado
    if results['success']:
        print("✅ Sucesso!")
        print(f"   Databases processados: {results['successful_count']}/{results['total_count']}")
        print(f"   Empresas totais: {results['metadata']['total_companies']}")
        print(f"   Qualidade geral: {results['metadata']['overall_quality']}")
        
        # Detalhes por database
        print("\n📊 Detalhes por Database:")
        for result in results['results']:
            db_name = result['database']
            status = "✅" if result['success'] else "❌"
            elapsed = result.get('elapsed_seconds', 0)
            files = result.get('files_uploaded', 0)
            print(f"   {status} {db_name}: {elapsed:.2f}s, {files} arquivos")
    else:
        print("❌ Falhou")
        print(f"   Erro: {results.get('error', 'Desconhecido')}")

def example_custom_config():
    """Exemplo: Usar configuração customizada."""
    print("📝 EXEMPLO: Configuração Customizada")
    print("=" * 50)
    
    # Inicializar com arquivo de configuração específico
    config_file = "config/config_databases.yaml"  # Substitua pelo seu arquivo
    
    try:
        pipeline = InMemoryForecastPipeline(config_file)
        print(f"✅ Pipeline inicializado com configuração: {config_file}")
        
        # Listar databases configurados
        databases = pipeline.config.get('databases', [])
        print(f"   Databases configurados: {len(databases)}")
        for db in databases:
            print(f"      - {db}")
        
    except Exception as e:
        print(f"❌ Erro ao carregar configuração: {e}")

def example_data_loading_only():
    """Exemplo: Apenas carregar dados sem processar."""
    print("📝 EXEMPLO: Apenas Carregamento de Dados")
    print("=" * 50)
    
    # Inicializar pipeline
    pipeline = InMemoryForecastPipeline()
    
    # Carregar dados para um database
    database = "005NO_ERP_BI"  # Substitua pelo seu database
    
    print(f"Carregando dados para: {database}")
    
    data = pipeline.load_all_data(database)
    
    # Verificar dados carregados
    print(f"Database: {data['database']}")
    print(f"Carregado em: {data['loaded_at']}")
    
    # Clientes
    if data['clientes']['success']:
        print("✅ Clientes: Carregado")
    else:
        print("❌ Clientes: Falhou")
    
    # Vendas
    if data['vendas']['success']:
        df = data['vendas']['data']
        print(f"✅ Vendas: {len(df)} registros")
        print(f"   Colunas: {list(df.columns)}")
    else:
        print(f"❌ Vendas: {data['vendas'].get('error', 'Erro desconhecido')}")
    
    # Vendas por Empresa
    if data['vendas_empresa']['success']:
        df = data['vendas_empresa']['data']
        empresas = df['Empresa'].nunique()
        print(f"✅ Vendas por Empresa: {len(df)} registros, {empresas} empresas")
    else:
        print(f"❌ Vendas por Empresa: {data['vendas_empresa'].get('error', 'Erro desconhecido')}")

def example_error_handling():
    """Exemplo: Tratamento de erros."""
    print("📝 EXEMPLO: Tratamento de Erros")
    print("=" * 50)
    
    # Inicializar pipeline
    pipeline = InMemoryForecastPipeline()
    
    # Tentar processar um database que pode não existir
    database = "DATABASE_INEXISTENTE"
    
    print(f"Tentando processar database: {database}")
    
    try:
        result = pipeline.run_complete_pipeline(database)
        
        if result['success']:
            print("✅ Sucesso inesperado!")
        else:
            print("❌ Falha esperada")
            print(f"   Erro: {result.get('error', 'Desconhecido')}")
            
    except Exception as e:
        print(f"❌ Exceção capturada: {e}")

def main():
    """Menu principal de exemplos."""
    print("📚 EXEMPLOS DE USO - INMEMORYFORECASTPIPELINE")
    print("=" * 60)
    print("Escolha um exemplo:")
    print("1. Database específico")
    print("2. Todos os databases")
    print("3. Configuração customizada")
    print("4. Apenas carregamento de dados")
    print("5. Tratamento de erros")
    print("6. Executar todos os exemplos")
    
    try:
        choice = input("\nOpção (1-6): ").strip()
        
        if choice == "1":
            example_single_database()
        elif choice == "2":
            example_all_databases()
        elif choice == "3":
            example_custom_config()
        elif choice == "4":
            example_data_loading_only()
        elif choice == "5":
            example_error_handling()
        elif choice == "6":
            print("\n🔄 Executando todos os exemplos...")
            example_single_database()
            print("\n" + "="*60)
            example_all_databases()
            print("\n" + "="*60)
            example_custom_config()
            print("\n" + "="*60)
            example_data_loading_only()
            print("\n" + "="*60)
            example_error_handling()
        else:
            print("❌ Opção inválida")
            
    except KeyboardInterrupt:
        print("\n👋 Saindo...")
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    main()
