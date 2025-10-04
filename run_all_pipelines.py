#!/usr/bin/env python3
"""
Pipeline Unificado - Execução Completa dos Forecasts Essenciais
Executa os 3 pipelines principais: Volume Consolidado, Vendas e Vendas por Empresa.
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
        {
            'name': 'Volume Consolidado',
            'function': run_volume_consolidado_all,
            'emoji': '📦'
        },
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
        # Executar pipeline específico
        pipeline_name = sys.argv[1]
        run_single_pipeline(pipeline_name)
    else:
        # Executar todos os pipelines
        main()
