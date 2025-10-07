# Pipeline em Memória - Documentação

## Visão Geral

O `InMemoryForecastPipeline` é uma implementação otimizada que executa todo o processo de forecasting em memória, eliminando operações de I/O desnecessárias e fazendo upload único para FTP ao final.

## Vantagens

- **Performance**: Elimina salvamento/recarregamento de CSVs
- **Eficiência**: Reutiliza dados carregados em memória
- **Simplicidade**: Um único ponto de entrada
- **Escalabilidade**: Fácil adicionar novos tipos de forecast
- **Manutenibilidade**: Código mais limpo e organizado

## Uso

### Modo Tradicional (com arquivos locais)
```bash
python run_all_pipelines.py
```

### Modo Memória (sem arquivos locais)
```bash
python run_all_pipelines.py --memory
```

### Pipeline específico
```bash
python run_all_pipelines.py vendas
python run_all_pipelines.py empresa
```

### Ajuda
```bash
python run_all_pipelines.py --help
```

## Estrutura de Dados

### Carregamento de Dados
```python
data = {
    'database': 'nome_do_banco',
    'clientes': {
        'result': resultado_extracao,
        'success': True/False
    },
    'vendas': {
        'data': DataFrame,
        'success': True/False
    },
    'vendas_empresa': {
        'data': DataFrame,
        'success': True/False
    },
    'loaded_at': datetime
}
```

### Resultados Consolidados
```python
results = {
    'database': 'nome_do_banco',
    'timestamp': '2024-01-01T00:00:00',
    'vendas': {
        'success': True/False,
        'historical': DataFrame,
        'forecasts': {30: DataFrame, 60: DataFrame, 90: DataFrame},
        'metrics': {30: dict, 60: dict, 90: dict},
        'consolidated': DataFrame
    },
    'vendas_empresa': {
        'success': True/False,
        'forecasts_by_company': {empresa: DataFrame},
        'metrics_by_company': {empresa: dict},
        'consolidated': DataFrame,
        'summary_metrics': dict
    },
    'summary': {
        'vendas_success': True/False,
        'vendas_empresa_success': True/False,
        'total_companies': int,
        'overall_quality': str
    }
}
```

## Fluxo de Execução

1. **Carregamento**: Dados extraídos diretamente do banco via `SimpleDataExtractor`
2. **Processamento Vendas**: Forecast univariado com múltiplos horizontes
3. **Processamento Vendas por Empresa**: Forecast por empresa com filtros de qualidade
4. **Consolidação**: Unificação de todos os resultados
5. **Preparação FTP**: Criação de arquivos em memória para upload
6. **Upload**: Envio único para servidor FTP

## Configuração

O pipeline utiliza o arquivo `config/config_databases.yaml` para obter a lista de databases a processar.

## Tratamento de Erros

- **Carregamento**: Falhas individuais não interrompem o pipeline
- **Processamento**: Erros são logados e continuam com próximos databases
- **Upload**: Falhas de upload são reportadas mas não interrompem o processo

## Métricas de Qualidade

### Vendas
- MAPE (Mean Absolute Percentage Error)
- MAE (Mean Absolute Error)
- RMSE (Root Mean Square Error)
- sMAPE (Symmetric MAPE)
- R² (Coefficient of Determination)
- Bias

### Vendas por Empresa
- Métricas individuais por empresa
- Métricas consolidadas (média, melhor, pior)
- Contagem de empresas processadas

## Classificação de Qualidade

- 🟢 **EXCELENTE**: MAPE ≤ 5%
- 🟡 **BOA**: MAPE ≤ 15%
- 🟠 **REGULAR**: MAPE ≤ 30%
- 🔴 **BAIXA**: MAPE > 30%

## Arquivos Gerados para FTP

### Vendas
- `{database}/vendas/vendas_historicas.csv`
- `{database}/vendas/previsoes_vendas_30.csv`
- `{database}/vendas/previsoes_vendas_60.csv`
- `{database}/vendas/previsoes_vendas_90.csv`
- `{database}/vendas/vendas_consolidadas.csv`

### Vendas por Empresa
- `{database}/vendas_empresa/previsao_vendas_empresa.csv`
- `{database}/vendas_empresa/metricas_vendas_empresa.csv`

## Exemplo de Uso Programático

```python
from pipelines.in_memory_forecast_pipeline import InMemoryForecastPipeline

# Inicializar pipeline
pipeline = InMemoryForecastPipeline()

# Executar para um database específico
result = pipeline.run_complete_pipeline('nome_do_banco')

# Executar para todos os databases
results = pipeline.run_all_databases()

# Verificar resultados
if results['success']:
    print(f"Processados: {results['successful_count']}/{results['total_count']}")
    print(f"Empresas: {results['metadata']['total_companies']}")
    print(f"Qualidade: {results['metadata']['overall_quality']}")
```

## Logs

O pipeline gera logs detalhados com prefixo `[in_memory]` para facilitar o debugging e monitoramento.

## Considerações de Memória

- Dados são carregados uma única vez por database
- DataFrames são reutilizados durante o processamento
- Arquivos FTP são criados em buffers de memória
- Garbage collection é automático após upload

## Troubleshooting

### Erro de Memória
- Reduzir número de databases processados simultaneamente
- Ajustar filtros de qualidade para reduzir empresas processadas

### Erro de Upload FTP
- Verificar conectividade com servidor FTP
- Validar credenciais e permissões
- Verificar espaço em disco no servidor

### Erro de Dados
- Verificar conectividade com banco de dados
- Validar estrutura dos dados extraídos
- Verificar configurações de filtros de qualidade
