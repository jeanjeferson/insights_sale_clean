# Insights Sale Clean - Sistema de Previsão de Vendas

Sistema automatizado de previsão de vendas utilizando machine learning com LightGBM e análise de séries temporais.

## 🎯 Funcionalidades

- **Previsão de Vendas Univariada**: Análise de séries temporais para previsão de vendas totais
- **Previsão por Empresa**: Análise individualizada de vendas por empresa
- **Previsão de Volume**: Análise de volumes de vendas
- **Múltiplos Horizontes**: Previsões para 30, 60 e 90 dias
- **Consolidação de Dados**: Arquivo unificado com dados históricos e previsões
- **Upload Automático**: Envio automático de resultados via SFTP
- **Docker Support**: Containerização para fácil deploy

## 🏗️ Arquitetura

```
├── config/                 # Configurações YAML
│   ├── config_databases.yaml
│   ├── vendas.yaml
│   ├── vendas_empresa.yaml
│   └── volume.yaml
├── pipelines/              # Pipelines de ML
│   ├── run_forecast_vendas.py
│   ├── run_forecast_vendas_empresa.py
│   └── run_forecast_volume.py
├── utils/                  # Utilitários
│   ├── sql_query.py
│   └── ftp_uploader.py
├── sql/                    # Queries SQL
├── dataset/                # Dados de entrada
├── results/                # Resultados das previsões
└── docker-compose.yml      # Configuração Docker
```

## 🚀 Instalação e Uso

### Pré-requisitos

- Python 3.8+
- Docker (opcional)

### Instalação Local

```bash
# Clone o repositório
git clone <repository-url>
cd insights_sale_clean

# Crie um ambiente virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou
.venv\Scripts\activate     # Windows

# Instale as dependências
pip install -r requirements.txt
```

### Uso com Docker

```bash
# Build e execução
docker-compose up --build
```

### Execução dos Pipelines

```bash
# Executar todos os pipelines
python run_all_pipelines.py

# Executar pipeline específico
python pipelines/run_forecast_vendas.py
python pipelines/run_forecast_vendas_empresa.py
python pipelines/run_forecast_volume.py
```

## ⚙️ Configuração

### Arquivos de Configuração

- **`config/vendas.yaml`**: Configurações para previsão de vendas univariada
- **`config/vendas_empresa.yaml`**: Configurações para previsão por empresa
- **`config/volume.yaml`**: Configurações para previsão de volume
- **`config/config_databases.yaml`**: Configurações de banco de dados

### Parâmetros Principais

```yaml
extraction:
  default_days_back: 730    # Histórico de dados (dias)
  separator: ';'            # Separador CSV
  encoding: 'utf-8'         # Encoding dos arquivos

forecast_horizons: [30, 60, 90]  # Horizontes de previsão

preprocessing:
  min_data_points: 100      # Mínimo de pontos para análise
  outlier_threshold: 3.0    # Threshold para detecção de outliers
```

## 📊 Saídas

### Arquivos Gerados

Para cada database, são gerados:

**Vendas Univariada:**
- `vendas_historicas.csv` - Dados históricos dos últimos 30 dias
- `previsoes_vendas_30.csv` - Previsões para 30 dias
- `previsoes_vendas_60.csv` - Previsões para 60 dias
- `previsoes_vendas_90.csv` - Previsões para 90 dias
- `vendas_consolidadas.csv` - **Arquivo unificado com dados históricos + previsões + soma acumulada**
- `metricas_vendas.csv` - Métricas de performance

**Vendas por Empresa:**
- `previsao_vendas_empresa.csv` - Previsões por empresa
- `metricas_vendas_empresa.csv` - Métricas por empresa
- `resumo_metricas_empresas.csv` - Resumo consolidado

**Volume:**
- `volume_historico.csv` - Dados históricos de volume
- `previsoes_volume.csv` - Previsões de volume
- `metricas_volume.csv` - Métricas de volume

## 🔧 Tecnologias Utilizadas

- **Python 3.8+**
- **Darts**: Framework de séries temporais
- **LightGBM**: Modelo de machine learning
- **Pandas**: Manipulação de dados
- **NumPy**: Computação numérica
- **PyYAML**: Configurações
- **Docker**: Containerização

## 📈 Métricas de Performance

- **MAPE** (Mean Absolute Percentage Error)
- **MAE** (Mean Absolute Error)
- **RMSE** (Root Mean Square Error)
- **R²** (Coefficient of Determination)

## 🔄 Workflow

1. **Extração de Dados**: Query SQL para extrair dados históricos
2. **Preprocessamento**: Limpeza, detecção de outliers, transformações
3. **Treinamento**: Treinamento do modelo LightGBM
4. **Backtesting**: Validação com dados históricos
5. **Previsão**: Geração de previsões para múltiplos horizontes
6. **Consolidação**: Criação do arquivo unificado
7. **Upload**: Envio automático via SFTP

## 📝 Logs

O sistema gera logs detalhados para:
- Processo de extração de dados
- Preprocessamento e limpeza
- Treinamento e validação
- Geração de previsões
- Upload de arquivos

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 📞 Suporte

Para dúvidas ou suporte, entre em contato através das issues do GitHub.
