#!/usr/bin/env python3
"""
Exemplos de como usar a API Insights Sale Clean
"""

import requests
import json
import time

# URL base da API
BASE_URL = "http://localhost:8000"

def test_health():
    """Testa o endpoint de health check."""
    print("🔍 Testando health check...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Erro no health check: {e}")
        return False

def test_root():
    """Testa o endpoint raiz."""
    print("\n🏠 Testando endpoint raiz...")
    try:
        response = requests.get(f"{BASE_URL}/")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Erro no endpoint raiz: {e}")
        return False

def test_databases():
    """Testa o endpoint de listagem de databases."""
    print("\n📊 Testando listagem de databases...")
    try:
        response = requests.get(f"{BASE_URL}/databases")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Erro na listagem de databases: {e}")
        return False

def test_status():
    """Testa o endpoint de status."""
    print("\n📈 Testando status das execuções...")
    try:
        response = requests.get(f"{BASE_URL}/status")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Erro no status: {e}")
        return False

def test_data_extraction():
    """Testa a extração de dados."""
    print("\n📥 Testando extração de dados...")
    try:
        response = requests.post(f"{BASE_URL}/data/extract/sales")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Erro na extração de dados: {e}")
        return False

def test_sales_pipeline():
    """Testa o pipeline de vendas."""
    print("\n💰 Testando pipeline de vendas...")
    try:
        # Testar com um database específico
        data = {"database_name": "005ATS_ERP_BI"}
        response = requests.post(f"{BASE_URL}/pipelines/sales/single", json=data)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Erro no pipeline de vendas: {e}")
        return False

def main():
    """Executa todos os testes."""
    print("🧪 TESTANDO API INSIGHTS SALE CLEAN")
    print("=" * 50)
    
    tests = [
        ("Health Check", test_health),
        ("Root Endpoint", test_root),
        ("Databases List", test_databases),
        ("Status", test_status),
        ("Data Extraction", test_data_extraction),
        ("Sales Pipeline", test_sales_pipeline),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            success = test_func()
            results.append((test_name, success))
            if success:
                print(f"✅ {test_name}: PASSOU")
            else:
                print(f"❌ {test_name}: FALHOU")
        except Exception as e:
            print(f"❌ {test_name}: ERRO - {e}")
            results.append((test_name, False))
    
    # Resumo dos resultados
    print("\n" + "="*50)
    print("📊 RESUMO DOS TESTES")
    print("="*50)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASSOU" if success else "❌ FALHOU"
        print(f"{test_name}: {status}")
    
    print(f"\n🎯 Resultado: {passed}/{total} testes passaram ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("🎉 Todos os testes passaram!")
    else:
        print("⚠️ Alguns testes falharam. Verifique a API.")

if __name__ == "__main__":
    main()
