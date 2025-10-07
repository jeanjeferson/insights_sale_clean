#!/usr/bin/env python3
"""
Pipeline Unificado - Execução Completa dos Forecasts Essenciais
Executa os 3 pipelines principais: Volume Consolidado, Vendas e Vendas por Empresa.

OPÇÕES DE EXECUÇÃO:
1. Modo Tradicional: Salva arquivos localmente e faz upload FTP
2. Modo Memória: Processa tudo em memória e faz upload único ao final
"""

import time
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Adicionar o diretório raiz ao PYTHONPATH
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Importar os pipelines essenciais
from utils.sql_query import extract_all_clients
from pipelines.run_forecast_vendas import run_all_databases as run_vendas_all
from pipelines.run_forecast_volume import run_all_databases as run_volume_consolidado_all
from pipelines.run_forecast_vendas_empresa import run_all_databases as run_vendas_empresa_all

# Importar nova classe em memória
from pipelines.in_memory_forecast_pipeline import InMemoryForecastPipeline

def format_duration(seconds):
    """Formata duração em formato legível."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds//60:.0f}m {seconds%60:.0f}s"
    else:
        return f"{seconds//3600:.0f}h {(seconds%3600)//60:.0f}m"

def main():
    """Executa todos os pipelines essenciais."""
    print("🚀 EXECUTANDO TODOS OS PIPELINES DE FORECASTING")
    print("===============================================")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Lista dos pipelines a executar
    pipelines = [
        {
            'name': 'Clientes',
            'function': extract_all_clients,
            'emoji': '👥'
        },
        # {
        #     'name': 'Volume Consolidado',
        #     'function': run_volume_consolidado_all,
        #     'emoji': '📦'
        # },
        {
            'name': 'Vendas',
            'function': run_vendas_all,
            'emoji': '💰'
        },
        {
            'name': 'Vendas por Empresa',
            'function': run_vendas_empresa_all,
            'emoji': '🏢'
        }
    ]
    
    results = {}
    total_start_time = time.time()
    
    # Executar cada pipeline
    for i, pipeline in enumerate(pipelines, 1):
        print(f"{pipeline['emoji']} [{i}/{len(pipelines)}] EXECUTANDO: {pipeline['name']}")
        print("-" * 50)
        
        start_time = time.time()
        
        try:
            # Executar pipeline
            result = pipeline['function']()
            
            end_time = time.time()
            duration = end_time - start_time
            
            results[pipeline['name']] = {
                'success': True,
                'duration': duration,
                'result': result
            }
            
            print(f"✅ {pipeline['name']} concluído em {format_duration(duration)}")
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            results[pipeline['name']] = {
                'success': False,
                'duration': duration,
                'error': str(e)
            }
            
            print(f"❌ {pipeline['name']} falhou após {format_duration(duration)}")
            print(f"   Erro: {str(e)}")
        
        print()
    
    # Calcular tempo total
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    
    # Resumo da execução
    print("🏆 RESUMO DA EXECUÇÃO")
    print("=" * 50)
    print(f"Tempo total: {format_duration(total_duration)}")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Detalhes por pipeline
    successful = 0
    failed = 0
    
    for pipeline_name, result in results.items():
        if result['success']:
            print(f"✅ {pipeline_name}: {format_duration(result['duration'])}")
            successful += 1
        else:
            print(f"❌ {pipeline_name}: {format_duration(result['duration'])} - {result['error']}")
            failed += 1
    
    print()
    print(f"📊 Resultado: {successful} sucesso(s), {failed} falha(s)")
    
    # Status de saída
    if failed > 0:
        print("⚠️ Alguns pipelines falharam. Verifique os logs acima.")
        sys.exit(1)
    else:
        sys.exit(0)

def main_in_memory():
    """Executa todos os pipelines em memória (sem salvamento local)."""
    print("🚀 EXECUTANDO PIPELINE EM MEMÓRIA")
    print("==================================")
    print("💾 Processamento completo em memória - sem arquivos locais")
    print("📤 Upload único para FTP ao final")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # Inicializar pipeline em memória
        pipeline = InMemoryForecastPipeline()
        
        # Executar pipeline completo
        results = pipeline.run_all_databases()
        
        # Resumo da execução
        print("\n🏆 RESUMO DA EXECUÇÃO EM MEMÓRIA")
        print("=" * 50)
        
        if results['success']:
            print(f"✅ Sucessos: {results['successful_count']}/{results['total_count']}")
            print(f"🏢 Empresas processadas: {results['metadata']['total_companies']}")
            print(f"🎯 Qualidade geral: {results['metadata']['overall_quality']}")
            
            # Tempo total
            if results['metadata']['start_time'] and results['metadata']['end_time']:
                total_duration = results['metadata']['end_time'] - results['metadata']['start_time']
                print(f"⏱️ Tempo total: {format_duration(total_duration)}")
            
            print(f"📤 Upload FTP: {'✅ Sucesso' if results['success'] else '❌ Falhou'}")
            
        else:
            print("❌ Pipeline falhou")
            if 'error' in results:
                print(f"   Erro: {results['error']}")
        
        print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Status de saída
        if results['success']:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Erro crítico no pipeline em memória: {e}")
        sys.exit(1)

def run_single_pipeline(pipeline_name: str):
    """Executa um pipeline específico."""
    pipeline_map = {
        'clientes': extract_all_clients,
        # 'volume': run_volume_consolidado_all,
        'vendas': run_vendas_all,
        'empresa': run_vendas_empresa_all
    }
    
    if pipeline_name.lower() not in pipeline_map:
        print(f"❌ Pipeline '{pipeline_name}' não encontrado.")
        print(f"Pipelines disponíveis: {', '.join(pipeline_map.keys())}")
        sys.exit(1)
    
    print(f"🚀 Executando pipeline: {pipeline_name}")
    start_time = time.time()
    
    try:
        result = pipeline_map[pipeline_name.lower()]()
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✅ Pipeline '{pipeline_name}' concluído em {format_duration(duration)}")
        return result
        
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"❌ Pipeline '{pipeline_name}' falhou após {format_duration(duration)}")
        print(f"   Erro: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Verificar argumentos da linha de comando
    if len(sys.argv) > 1:
        if sys.argv[1] == "--memory" or sys.argv[1] == "-m":
            # Executar pipeline em memória
            main_in_memory()
        elif sys.argv[1] == "--help" or sys.argv[1] == "-h":
            # Mostrar ajuda
            print("🚀 PIPELINE UNIFICADO DE FORECASTING")
            print("=" * 50)
            print("Uso:")
            print("  python run_all_pipelines.py              # Modo tradicional (com arquivos locais)")
            print("  python run_all_pipelines.py --memory     # Modo memória (sem arquivos locais)")
            print("  python run_all_pipelines.py <pipeline>   # Pipeline específico")
            print()
            print("Pipelines disponíveis:")
            print("  clientes    - Extração de dados de clientes")
            print("  vendas      - Forecast de vendas")
            print("  empresa     - Forecast de vendas por empresa")
            print()
            print("Modos:")
            print("  --memory    - Processa tudo em memória e faz upload único para FTP")
            print("  (padrão)    - Salva arquivos localmente e faz upload individual")
        else:
            # Executar pipeline específico
            pipeline_name = sys.argv[1]
            run_single_pipeline(pipeline_name)
    else:
        # Executar todos os pipelines (modo tradicional)
        main()
