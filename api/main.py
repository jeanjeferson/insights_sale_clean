"""
API FastAPI para Insights Sale Clean
Endpoints para execução de pipelines de forecasting e atualização de datasets.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import asyncio
import logging
import sys
from pathlib import Path
import traceback

# Adicionar o diretório raiz ao path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Importar pipelines e utilitários
from pipelines.run_forecast_vendas import run_sales_forecast, run_all_databases as run_all_sales
from pipelines.run_forecast_vendas_empresa import run_vendas_empresa_forecast, run_all_databases as run_all_vendas_empresa
from pipelines.run_forecast_volume import run_single_database as run_volume_single, run_all_databases as run_all_volume
from run_all_pipelines import main as run_all_pipelines_main
from utils.sql_query import SimpleDataExtractor, extract_all_clients

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar FastAPI
app = FastAPI(
    title="Insights Sale Clean API",
    description="API para execução de pipelines de forecasting de vendas e volume",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Modelos Pydantic para requisições
class DatabaseRequest(BaseModel):
    database_name: str

class DataExtractionRequest(BaseModel):
    data_type: Optional[str] = "all"  # "vendas", "volume", "vendas_empresa", "all"
    databases: Optional[List[str]] = None

class PipelineRequest(BaseModel):
    database_name: Optional[str] = None
    background: Optional[bool] = True

# Status global para tracking de execuções
execution_status = {
    "sales": {"running": False, "last_run": None, "result": None},
    "vendas_empresa": {"running": False, "last_run": None, "result": None},
    "volume": {"running": False, "last_run": None, "result": None},
    "all_pipelines": {"running": False, "last_run": None, "result": None},
    "data_extraction": {"running": False, "last_run": None, "result": None}
}

@app.get("/")
async def root():
    """Endpoint raiz com informações da API."""
    return {
        "message": "Insights Sale Clean API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "docs": "/docs",
            "status": "/status",
            "pipelines": {
                "sales": "/pipelines/sales",
                "vendas_empresa": "/pipelines/vendas-empresa", 
                "volume": "/pipelines/volume",
                "all": "/pipelines/all"
            },
            "data_extraction": "/data/extract"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "message": "API is running"}

@app.get("/status")
async def get_status():
    """Retorna o status de todas as execuções."""
    return {"execution_status": execution_status}

# ========================================
# ENDPOINTS PARA PIPELINES
# ========================================

@app.post("/pipelines/sales/single")
async def run_sales_single(request: DatabaseRequest, background_tasks: BackgroundTasks):
    """Executa pipeline de vendas para um database específico."""
    if execution_status["sales"]["running"]:
        raise HTTPException(status_code=409, detail="Sales pipeline já está em execução")
    
    def run_sales_task():
        try:
            execution_status["sales"]["running"] = True
            result = run_sales_forecast(request.database_name)
            execution_status["sales"]["result"] = result
            execution_status["sales"]["running"] = False
            logger.info(f"Sales pipeline completed for {request.database_name}")
        except Exception as e:
            execution_status["sales"]["running"] = False
            execution_status["sales"]["result"] = {"error": str(e)}
            logger.error(f"Sales pipeline failed for {request.database_name}: {e}")
    
    if background_tasks:
        background_tasks.add_task(run_sales_task)
        return {"message": f"Sales pipeline iniciado para {request.database_name}", "status": "started"}
    else:
        try:
            result = run_sales_forecast(request.database_name)
            return {"message": f"Sales pipeline concluído para {request.database_name}", "result": result}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro no pipeline de vendas: {e}")

@app.post("/pipelines/sales/all")
async def run_sales_all(background_tasks: BackgroundTasks):
    """Executa pipeline de vendas para todos os databases."""
    if execution_status["sales"]["running"]:
        raise HTTPException(status_code=409, detail="Sales pipeline já está em execução")
    
    def run_sales_all_task():
        try:
            execution_status["sales"]["running"] = True
            result = run_all_sales()
            execution_status["sales"]["result"] = result
            execution_status["sales"]["running"] = False
            logger.info("Sales pipeline (all databases) completed")
        except Exception as e:
            execution_status["sales"]["running"] = False
            execution_status["sales"]["result"] = {"error": str(e)}
            logger.error(f"Sales pipeline (all databases) failed: {e}")
    
    background_tasks.add_task(run_sales_all_task)
    return {"message": "Sales pipeline para todos os databases iniciado", "status": "started"}

@app.post("/pipelines/vendas-empresa/single")
async def run_vendas_empresa_single(request: DatabaseRequest, background_tasks: BackgroundTasks):
    """Executa pipeline de vendas por empresa para um database específico."""
    if execution_status["vendas_empresa"]["running"]:
        raise HTTPException(status_code=409, detail="Vendas empresa pipeline já está em execução")
    
    def run_vendas_empresa_task():
        try:
            execution_status["vendas_empresa"]["running"] = True
            result = run_vendas_empresa_forecast(request.database_name)
            execution_status["vendas_empresa"]["result"] = result
            execution_status["vendas_empresa"]["running"] = False
            logger.info(f"Vendas empresa pipeline completed for {request.database_name}")
        except Exception as e:
            execution_status["vendas_empresa"]["running"] = False
            execution_status["vendas_empresa"]["result"] = {"error": str(e)}
            logger.error(f"Vendas empresa pipeline failed for {request.database_name}: {e}")
    
    background_tasks.add_task(run_vendas_empresa_task)
    return {"message": f"Vendas empresa pipeline iniciado para {request.database_name}", "status": "started"}

@app.post("/pipelines/vendas-empresa/all")
async def run_vendas_empresa_all(background_tasks: BackgroundTasks):
    """Executa pipeline de vendas por empresa para todos os databases."""
    if execution_status["vendas_empresa"]["running"]:
        raise HTTPException(status_code=409, detail="Vendas empresa pipeline já está em execução")
    
    def run_vendas_empresa_all_task():
        try:
            execution_status["vendas_empresa"]["running"] = True
            result = run_all_vendas_empresa()
            execution_status["vendas_empresa"]["result"] = result
            execution_status["vendas_empresa"]["running"] = False
            logger.info("Vendas empresa pipeline (all databases) completed")
        except Exception as e:
            execution_status["vendas_empresa"]["running"] = False
            execution_status["vendas_empresa"]["result"] = {"error": str(e)}
            logger.error(f"Vendas empresa pipeline (all databases) failed: {e}")
    
    background_tasks.add_task(run_vendas_empresa_all_task)
    return {"message": "Vendas empresa pipeline para todos os databases iniciado", "status": "started"}

@app.post("/pipelines/volume/single")
async def run_volume_single(request: DatabaseRequest, background_tasks: BackgroundTasks):
    """Executa pipeline de volume para um database específico."""
    if execution_status["volume"]["running"]:
        raise HTTPException(status_code=409, detail="Volume pipeline já está em execução")
    
    def run_volume_task():
        try:
            execution_status["volume"]["running"] = True
            result = run_volume_single(request.database_name)
            execution_status["volume"]["result"] = result
            execution_status["volume"]["running"] = False
            logger.info(f"Volume pipeline completed for {request.database_name}")
        except Exception as e:
            execution_status["volume"]["running"] = False
            execution_status["volume"]["result"] = {"error": str(e)}
            logger.error(f"Volume pipeline failed for {request.database_name}: {e}")
    
    background_tasks.add_task(run_volume_task)
    return {"message": f"Volume pipeline iniciado para {request.database_name}", "status": "started"}

@app.post("/pipelines/volume/all")
async def run_volume_all(background_tasks: BackgroundTasks):
    """Executa pipeline de volume para todos os databases."""
    if execution_status["volume"]["running"]:
        raise HTTPException(status_code=409, detail="Volume pipeline já está em execução")
    
    def run_volume_all_task():
        try:
            execution_status["volume"]["running"] = True
            result = run_all_volume()
            execution_status["volume"]["result"] = result
            execution_status["volume"]["running"] = False
            logger.info("Volume pipeline (all databases) completed")
        except Exception as e:
            execution_status["volume"]["running"] = False
            execution_status["volume"]["result"] = {"error": str(e)}
            logger.error(f"Volume pipeline (all databases) failed: {e}")
    
    background_tasks.add_task(run_volume_all_task)
    return {"message": "Volume pipeline para todos os databases iniciado", "status": "started"}

@app.post("/pipelines/all")
async def run_all_pipelines(background_tasks: BackgroundTasks):
    """Executa todos os pipelines (vendas, vendas_empresa, volume)."""
    if execution_status["all_pipelines"]["running"]:
        raise HTTPException(status_code=409, detail="All pipelines já está em execução")
    
    def run_all_pipelines_task():
        try:
            execution_status["all_pipelines"]["running"] = True
            # Executar função principal que roda todos os pipelines
            result = run_all_pipelines_main()
            execution_status["all_pipelines"]["result"] = result
            execution_status["all_pipelines"]["running"] = False
            logger.info("All pipelines completed")
        except Exception as e:
            execution_status["all_pipelines"]["running"] = False
            execution_status["all_pipelines"]["result"] = {"error": str(e)}
            logger.error(f"All pipelines failed: {e}")
    
    background_tasks.add_task(run_all_pipelines_task)
    return {"message": "Todos os pipelines iniciados", "status": "started"}

# ========================================
# ENDPOINTS PARA EXTRAÇÃO DE DADOS
# ========================================

@app.post("/data/extract")
async def extract_data(request: DataExtractionRequest, background_tasks: BackgroundTasks):
    """Extrai dados de todos os databases configurados."""
    if execution_status["data_extraction"]["running"]:
        raise HTTPException(status_code=409, detail="Data extraction já está em execução")
    
    def extract_data_task():
        try:
            execution_status["data_extraction"]["running"] = True
            result = extract_all_clients(data_type=request.data_type)
            execution_status["data_extraction"]["result"] = result
            execution_status["data_extraction"]["running"] = False
            logger.info(f"Data extraction completed for type: {request.data_type}")
        except Exception as e:
            execution_status["data_extraction"]["running"] = False
            execution_status["data_extraction"]["result"] = {"error": str(e)}
            logger.error(f"Data extraction failed: {e}")
    
    background_tasks.add_task(extract_data_task)
    return {
        "message": f"Extração de dados iniciada para tipo: {request.data_type}", 
        "status": "started"
    }

@app.post("/data/extract/sales")
async def extract_sales_data(background_tasks: BackgroundTasks):
    """Extrai apenas dados de vendas."""
    request = DataExtractionRequest(data_type="vendas")
    return await extract_data(request, background_tasks)

@app.post("/data/extract/volume")
async def extract_volume_data(background_tasks: BackgroundTasks):
    """Extrai apenas dados de volume."""
    request = DataExtractionRequest(data_type="volume")
    return await extract_data(request, background_tasks)

@app.post("/data/extract/vendas-empresa")
async def extract_vendas_empresa_data(background_tasks: BackgroundTasks):
    """Extrai apenas dados de vendas por empresa."""
    request = DataExtractionRequest(data_type="vendas_empresa")
    return await extract_data(request, background_tasks)

# ========================================
# ENDPOINTS DE INFORMAÇÕES
# ========================================

@app.get("/databases")
async def list_databases():
    """Lista todos os databases configurados."""
    try:
        import yaml
        config_path = project_root / "config" / "config_databases.yaml"
        
        if not config_path.exists():
            raise HTTPException(status_code=404, detail="Arquivo de configuração não encontrado")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        databases = config.get('databases', [])
        return {"databases": databases, "count": len(databases)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar databases: {e}")

@app.get("/results/{database_name}")
async def list_results(database_name: str):
    """Lista os resultados disponíveis para um database específico."""
    try:
        results_dir = project_root / "results" / database_name
        
        if not results_dir.exists():
            raise HTTPException(status_code=404, detail=f"Resultados não encontrados para {database_name}")
        
        results = {}
        
        # Verificar subdiretórios
        for subdir in ["vendas", "vendas_empresa", "volume"]:
            subdir_path = results_dir / subdir
            if subdir_path.exists():
                files = [f.name for f in subdir_path.iterdir() if f.is_file()]
                results[subdir] = files
        
        return {"database": database_name, "results": results}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar resultados: {e}")

# ========================================
# MIDDLEWARE E CONFIGURAÇÕES
# ========================================

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Handler global para exceções."""
    logger.error(f"Erro não tratado: {exc}")
    logger.error(traceback.format_exc())
    return JSONResponse(
        status_code=500,
        content={"detail": f"Erro interno do servidor: {str(exc)}"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
