# Insights Sale Clean API

API FastAPI para execução de pipelines de forecasting de vendas e volume.

## 🚀 Iniciando a API

### Localmente (Desenvolvimento)
```bash
# Ativar ambiente virtual
source .venv/bin/activate

# Iniciar API
python start_api.py
```

### Com Docker
```bash
# Build da imagem
docker build -t insights-sale-clean .

# Executar container
docker run -p 8000:8000 insights-sale-clean
```

## 📖 Documentação

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

## 🔗 Endpoints Principais

### Informações Gerais
- `GET /` - Informações da API
- `GET /health` - Health check
- `GET /status` - Status das execuções
- `GET /databases` - Lista databases configurados

### Pipelines de Forecasting

#### Vendas (Univariado)
- `POST /pipelines/sales/single` - Pipeline de vendas para um database
- `POST /pipelines/sales/all` - Pipeline de vendas para todos os databases

#### Vendas por Empresa
- `POST /pipelines/vendas-empresa/single` - Pipeline de vendas por empresa para um database
- `POST /pipelines/vendas-empresa/all` - Pipeline de vendas por empresa para todos os databases

#### Volume
- `POST /pipelines/volume/single` - Pipeline de volume para um database
- `POST /pipelines/volume/all` - Pipeline de volume para todos os databases

#### Todos os Pipelines
- `POST /pipelines/all` - Executa todos os pipelines

### Extração de Dados
- `POST /data/extract` - Extrai dados de todos os tipos
- `POST /data/extract/sales` - Extrai apenas dados de vendas
- `POST /data/extract/volume` - Extrai apenas dados de volume
- `POST /data/extract/vendas-empresa` - Extrai apenas dados de vendas por empresa

### Resultados
- `GET /results/{database_name}` - Lista resultados para um database

## 📝 Exemplos de Uso

### 1. Verificar Status da API
```bash
curl http://localhost:8000/health
```

### 2. Listar Databases Configurados
```bash
curl http://localhost:8000/databases
```

### 3. Executar Pipeline de Vendas para um Database
```bash
curl -X POST http://localhost:8000/pipelines/sales/single \
  -H "Content-Type: application/json" \
  -d '{"database_name": "005ATS_ERP_BI"}'
```

### 4. Extrair Dados de Vendas
```bash
curl -X POST http://localhost:8000/data/extract/sales
```

### 5. Executar Todos os Pipelines
```bash
curl -X POST http://localhost:8000/pipelines/all
```

### 6. Verificar Status das Execuções
```bash
curl http://localhost:8000/status
```

## 🧪 Testando a API

Execute o script de testes:
```bash
python api_test_examples.py
```

## 📊 Estrutura de Resposta

### Sucesso
```json
{
  "message": "Pipeline iniciado com sucesso",
  "status": "started"
}
```

### Erro
```json
{
  "detail": "Pipeline já está em execução"
}
```

### Status de Execução
```json
{
  "execution_status": {
    "sales": {
      "running": false,
      "last_run": "2024-01-01T12:00:00",
      "result": {...}
    }
  }
}
```

## ⚙️ Configuração

A API utiliza as mesmas configurações dos pipelines:
- `config/config_databases.yaml` - Configuração dos databases
- `config/vendas.yaml` - Configuração do pipeline de vendas
- `config/vendas_empresa.yaml` - Configuração do pipeline de vendas por empresa
- `config/volume.yaml` - Configuração do pipeline de volume

## 🔧 Desenvolvimento

### Estrutura dos Arquivos
```
api/
├── __init__.py
├── main.py          # Aplicação FastAPI principal
└── README.md        # Esta documentação
```

### Adicionando Novos Endpoints

1. Edite `api/main.py`
2. Adicione o novo endpoint seguindo o padrão existente
3. Teste localmente com `python start_api.py`
4. Atualize esta documentação

### Logs

A API registra logs de:
- Início/fim de execuções de pipelines
- Erros e exceções
- Status de requisições

## 🚨 Tratamento de Erros

- **409 Conflict**: Pipeline já está em execução
- **404 Not Found**: Database ou arquivo não encontrado
- **500 Internal Server Error**: Erro interno do servidor

## 🔒 Segurança

- Execução em background para evitar timeouts
- Validação de entrada com Pydantic
- Tratamento global de exceções
- Logs detalhados para debugging
