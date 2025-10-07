#!/usr/bin/env python3
"""
Script simples para testar apenas a extração de dados de um database específico.
"""

import sys
from pathlib import Path

# Adicionar o diretório raiz ao PYTHONPATH
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pipelines.in_memory_forecast_pipeline import InMemoryForecastPipeline

def main():
    """Testa extração de dados de um database específico."""
    print("🔍 TESTE DE EXTRAÇÃO DE DADOS")
    print("=" * 50)
    
    # Inicializar pipeline
    pipeline = InMemoryForecastPipeline()
    
    # Database para testar (pode ser alterado)
    test_database = "005NO_ERP_BI"
    
    print(f"Extraindo dados para: {test_database}")
    print()
    
    try:
        # Carregar dados
        data = pipeline.load_all_data(test_database)
        
        print(f"📊 RESULTADO DA EXTRAÇÃO")
        print(f"Database: {data['database']}")
        print(f"Carregado em: {data['loaded_at']}")
        print()
        
        # Clientes
        print("👥 CLIENTES:")
        if data['clientes']['success']:
            print("✅ Extraído com sucesso")
            print(f"   Resultado: {data['clientes']['result']}")
        else:
            print("❌ Falhou")
        print()
        
        # Vendas
        print("💰 VENDAS:")
        if data['vendas']['success']:
            df = data['vendas']['data']
            print(f"✅ {len(df)} registros extraídos")
            print(f"   Colunas: {list(df.columns)}")
            if len(df) > 0:
                print(f"   Período: {df.iloc[0, 0]} a {df.iloc[-1, 0]}")
                print(f"   Valores: R$ {df.iloc[:, 1].sum():,.2f} total")
                print(f"   Média diária: R$ {df.iloc[:, 1].mean():,.2f}")
        else:
            print(f"❌ Falhou: {data['vendas'].get('error', 'Erro desconhecido')}")
        print()
        
        # Vendas por Empresa
        print("🏢 VENDAS POR EMPRESA:")
        if data['vendas_empresa']['success']:
            df = data['vendas_empresa']['data']
            empresas = df['Empresa'].nunique()
            print(f"✅ {len(df)} registros, {empresas} empresas")
            if len(df) > 0:
                print(f"   Período: {df['data'].min()} a {df['data'].max()}")
                print(f"   Valores: R$ {df['vendas'].sum():,.2f} total")
                print(f"   Média por empresa: R$ {df.groupby('Empresa')['vendas'].sum().mean():,.2f}")
                
                # Top 5 empresas
                top_empresas = df.groupby('Empresa')['vendas'].sum().nlargest(5)
                print(f"   Top 5 empresas:")
                for empresa, valor in top_empresas.items():
                    print(f"      {empresa}: R$ {valor:,.2f}")
        else:
            print(f"❌ Falhou: {data['vendas_empresa'].get('error', 'Erro desconhecido')}")
        print()
        
        # Resumo
        success_count = sum([
            data['clientes']['success'],
            data['vendas']['success'],
            data['vendas_empresa']['success']
        ])
        
        print(f"📈 RESUMO: {success_count}/3 extrações bem-sucedidas")
        
        if success_count == 3:
            print("🎉 Todos os dados foram extraídos com sucesso!")
        elif success_count > 0:
            print("⚠️ Algumas extrações falharam")
        else:
            print("❌ Todas as extrações falharam")
            
    except Exception as e:
        print(f"❌ Erro na extração: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
