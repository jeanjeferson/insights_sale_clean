"""
Pipeline de Forecasting com Darts - Versão para Vendas por Empresa
Processas vendas por empresa separadamente e consolida os resultados.
"""

from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import yaml
import warnings
import os
import sys
import time

# ========================================
# SUPRESSÃO COMPLETA DE WARNINGS
# ========================================

# Suprimir TODOS os warnings do Python
warnings.filterwarnings('ignore')
warnings.simplefilter('ignore')

# Configurar variáveis de ambiente para silenciar LightGBM
os.environ['PYTHONWARNINGS'] = 'ignore'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# Suprimir warnings específicos do LightGBM
import logging
logging.getLogger('lightgbm').setLevel(logging.CRITICAL)
logging.getLogger('py.warnings').setLevel(logging.CRITICAL)

# Desabilitar verbosity do LightGBM globalmente (se disponível, configuramos via logging)
try:
    import lightgbm as lgb  # noqa: E402
except Exception:
    lgb = None

# Redirecionar stderr temporariamente para suprimir warnings
import contextlib
from io import StringIO

@contextlib.contextmanager
def suppress_stdout_stderr():
    """Context manager para suprimir completamente stdout e stderr"""
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = devnull
        sys.stderr = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr

# ========================================

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Darts imports com supressão
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from darts import TimeSeries
    from darts.models import LightGBMModel
    from darts.dataprocessing.transformers import Scaler, MissingValuesFiller, InvertibleMapper
    from darts.utils.timeseries_generation import datetime_attribute_timeseries
from darts.utils.statistics import check_seasonality
from darts.metrics import mape, mae, rmse, smape
import matplotlib.pyplot as plt
from utils.ftp_uploader import ForecastFTPUploader
from utils.sql_query import SimpleDataExtractor

# Define base paths
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATASET_DIR = PROJECT_ROOT / 'dataset'
RESULTS_DIR = PROJECT_ROOT / 'results'

def custom_mape(actual: TimeSeries, pred: TimeSeries) -> float:
    """Custom MAPE that handles zeros in actual values."""
    y_true = actual.values()
    y_pred = pred.values()
    
    # Avoid division by zero by replacing 0s with 1 in the denominator
    denominator = np.where(y_true == 0, 1, y_true)
    
    return float(np.mean(np.abs((y_true - y_pred) / denominator)) * 100)

class VendasEmpresaForecastPipeline:
    """Pipeline especializado para forecasting de vendas por empresa."""
    
    def __init__(self, config_path: str = "config/vendas_empresa.yaml"):
        """Initialize pipeline with configuration."""
        self.config = self._load_config(config_path)
        self.models = {}  # Dicionário para armazenar modelos por empresa
        self.scalers = {}
        self.fillers = {}
        self.log_transformers = {}
        
        # # Criar pasta de resultados se não existir
        # os.makedirs(f"{RESULTS_DIR}/vendas_empresa", exist_ok=True)
        
        # Logging prefix
        self.log_prefix = "[vendas_empresa]"
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _log_info(self, message: str):
        """Log message with prefix."""
        print(f"{self.log_prefix} {message}")
    
    def load_data(self, database: str) -> pd.DataFrame:
        """Carrega dados de vendas por empresa usando sql_query."""
        self._log_info(f"Carregando dados de vendas por empresa para {database}")
        
        try:
            extractor = SimpleDataExtractor()
            result = extractor.extract_vendas_empresa_data(database)
            self._log_info(f"Resultado da extração: {result}")
            
            # Carregar o CSV gerado
            csv_file = f"{DATASET_DIR}/{database}_vendas_empresa.csv"
            if not Path(csv_file).exists():
                raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_file}")
            
            # Carregar dados com dtype=str para preservar zeros à esquerda
            separator = self.config.get('preprocessing', {}).get('separator', ';')
            df = pd.read_csv(csv_file, sep=separator, dtype=str)
            
            # Validar colunas esperadas
            expected_columns = ['data', 'Empresa', 'vendas']
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Colunas esperadas não encontradas: {missing_columns}")
            
            # Converter vendas para float (preserva Empresa como string)
            df['vendas'] = pd.to_numeric(df['vendas'], errors='coerce')
            
            # Converter data para datetime
            df['data'] = pd.to_datetime(df['data'])
            
            self._log_info(f"Dados carregados: {len(df)} registros, {df['Empresa'].nunique()} empresas")
            return df
            
        except Exception as e:
            self._log_info(f"Erro ao carregar dados: {e}")
            raise
    
    def filter_and_prepare_series(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Filtra e prepara séries por empresa usando configurações do YAML."""
        self._log_info("Aplicando filtros e preparando séries por empresa")
        
        # Configurações do YAML
        vendas_config = self.config.get('vendas_processing', {})
        target_percentile = vendas_config.get('target_percentile', 80)
        min_records_per_group = vendas_config.get('min_records_per_group', 40)
        max_zero_ratio = vendas_config.get('max_zero_ratio', 0.8)
        max_groups = vendas_config.get('max_groups', 30)
        min_total_sales = self.config.get('group_processing', {}).get('min_total_sales', 2000)
        
        self._log_info(f"Filtros: top {target_percentile}%, min {min_records_per_group} registros, max {max_zero_ratio*100}% zeros")
        
        # 1. Calcular vendas totais por empresa
        empresa_stats = df.groupby('Empresa').agg({
            'vendas': ['sum', 'count', lambda x: (x == 0).sum() / len(x)]
        }).round(2)
        empresa_stats.columns = ['total_vendas', 'num_records', 'zero_ratio']
        empresa_stats = empresa_stats.reset_index()
        
        # Garantir que Empresa seja string após groupby
        empresa_stats['Empresa'] = empresa_stats['Empresa'].astype(str)
        
        # 2. Aplicar filtros
        # Filtro por vendas mínimas totais
        empresa_stats = empresa_stats[empresa_stats['total_vendas'] >= min_total_sales]
        self._log_info(f"Após filtro vendas mínimas ({min_total_sales}): {len(empresa_stats)} empresas")
        
        # Filtro por número mínimo de registros
        empresa_stats = empresa_stats[empresa_stats['num_records'] >= min_records_per_group]
        self._log_info(f"Após filtro registros mínimos ({min_records_per_group}): {len(empresa_stats)} empresas")
        
        # Filtro por razão de zeros
        empresa_stats = empresa_stats[empresa_stats['zero_ratio'] <= max_zero_ratio]
        self._log_info(f"Após filtro zeros ({max_zero_ratio*100}%): {len(empresa_stats)} empresas")
        
        # 3. Selecionar top percentil
        if len(empresa_stats) > 0:
            cutoff_value = np.percentile(empresa_stats['total_vendas'], 100 - target_percentile)
            empresa_stats = empresa_stats[empresa_stats['total_vendas'] >= cutoff_value]
            self._log_info(f"Após filtro percentil (top {target_percentile}%): {len(empresa_stats)} empresas")
        
        # 4. Limitar número máximo de empresas
        if max_groups and len(empresa_stats) > max_groups:
            empresa_stats = empresa_stats.nlargest(max_groups, 'total_vendas')
            self._log_info(f"Limitado a {max_groups} maiores empresas")
        
        if len(empresa_stats) == 0:
            self._log_info("⚠️ Nenhuma empresa passou pelos filtros")
            return {}
        
        # 5. Preparar séries finais
        empresas_selecionadas = empresa_stats['Empresa'].tolist()
        df_filtered = df[df['Empresa'].isin(empresas_selecionadas)]
        
        series_dict = {}
        for empresa in empresas_selecionadas:
            # Garantir que empresa seja string para preservar zeros à esquerda
            empresa_str = str(empresa)
            empresa_data = df_filtered[df_filtered['Empresa'] == empresa_str].copy()
            empresa_data = empresa_data.sort_values('data')
            series_dict[empresa_str] = empresa_data[['data', 'vendas']].set_index('data')
        
        self._log_info(f"✅ {len(series_dict)} séries preparadas para forecast")
        return series_dict
    
    def create_time_series(self, data: pd.DataFrame, empresa: str) -> Optional[TimeSeries]:
        """Cria TimeSeries do Darts para uma empresa."""
        try:
            # Criar TimeSeries
            ts = TimeSeries.from_dataframe(
                data,
                time_col=None,  # já está como index
                value_cols=['vendas'],
                freq='D'
            )
            
            # Preencher valores faltantes se configurado
            if self.config.get('preprocessing', {}).get('fill_missing', True):
                if empresa not in self.fillers:
                    self.fillers[empresa] = MissingValuesFiller()
                ts = self.fillers[empresa].transform(ts)
            
            # Aplicar transformação logarítmica se configurado
            if self.config.get('preprocessing', {}).get('log_transform', True):
                if empresa not in self.log_transformers:
                    # Adicionar 1 para evitar log(0)
                    # Ensure mapper functions accept numpy arrays / darts TimeSeries values
                    self.log_transformers[empresa] = InvertibleMapper(
                        lambda x: np.log1p(np.maximum(x, 0)),
                        lambda x: np.expm1(np.array(x, dtype=float))
                    )
                ts = self.log_transformers[empresa].transform(ts)
            
            # Escalar dados se configurado
            if self.config.get('preprocessing', {}).get('scale_data', True):
                if empresa not in self.scalers:
                    self.scalers[empresa] = Scaler()
                ts = self.scalers[empresa].fit_transform(ts)
            
            return ts
            
        except Exception as e:
            self._log_info(f"Erro ao criar TimeSeries para {empresa}: {e}")
            return None
    
    def train_model(self, ts: TimeSeries, empresa: str) -> Optional[LightGBMModel]:
        """Treina modelo LightGBM para uma empresa."""
        try:
            model_config = self.config.get('model', {})
            
            # Suprimir warnings durante criação do modelo
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                
                # Criar modelo LightGBM com parâmetros corretos
                model = LightGBMModel(
                    lags=model_config.get('lags', 28),
                    output_chunk_length=model_config.get('forecast_horizon', 30),
                    random_state=model_config.get('random_state', 42),
                    quantiles=model_config.get('quantiles', [0.1, 0.5, 0.9]),
                    verbose=-1,  # Silenciar o LightGBM
                    n_estimators=model_config.get('n_estimators', 100),
                    num_leaves=model_config.get('num_leaves', 15),
                    learning_rate=model_config.get('learning_rate', 0.1),
                    max_depth=model_config.get('max_depth', 3),
                    min_child_samples=model_config.get('min_data_in_leaf', 30),
                    colsample_bytree=model_config.get('feature_fraction', 0.7),
                    subsample=model_config.get('bagging_fraction', 0.7),
                    subsample_freq=model_config.get('bagging_freq', 1),
                    objective='regression',
                    metric='rmse',
                    boosting_type='gbdt',
                    force_col_wise=True
                )
            
            # Dividir dados para treino/validação  
            backtest_config = self.config.get('backtest', {})
            train_ratio = backtest_config.get('start', 0.8)
            
            train_size = int(len(ts) * train_ratio)
            train_ts = ts[:train_size]
            
            # Treinar modelo com supressão completa de warnings
            with suppress_stdout_stderr():
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    model.fit(train_ts)
                    
            self.models[empresa] = model
            
            self._log_info(f"✅ Modelo treinado para {empresa} com {len(train_ts)} pontos")
            return model
            
        except Exception as e:
            self._log_info(f"Erro ao treinar modelo para {empresa}: {e}")
            return None
    
    def generate_forecast(self, model: LightGBMModel, empresa: str, horizon: int) -> Optional[TimeSeries]:
        """Gera forecast para uma empresa."""
        try:
            forecast = model.predict(n=horizon)
            self._log_info(f"✅ Forecast gerado para {empresa}: {len(forecast)} pontos")
            return forecast
            
        except Exception as e:
            self._log_info(f"Erro ao gerar forecast para {empresa}: {e}")
            return None
    
    def inverse_transform_forecast(self, forecast: TimeSeries, empresa: str) -> TimeSeries:
        """Aplica transformações inversas no forecast."""
        try:
            # Inverse scaling
            if empresa in self.scalers:
                forecast = self.scalers[empresa].inverse_transform(forecast)
            
            # Inverse log transform
            if empresa in self.log_transformers:
                forecast = self.log_transformers[empresa].inverse_transform(forecast)
            
            return forecast
            
        except Exception as e:
            self._log_info(f"Erro ao aplicar transformações inversas para {empresa}: {e}")
            return forecast
    
    def calculate_metrics(self, ts: TimeSeries, empresa: str) -> Dict[str, float]:
        """Calcula métricas de qualidade para uma empresa."""
        try:
            if empresa not in self.models:
                return {}
            
            model = self.models[empresa]
            backtest_config = self.config.get('backtest', {})
            train_ratio = backtest_config.get('start', 0.8)
            
            # Dividir dados para backtest
            train_size = int(len(ts) * train_ratio)
            train_ts = ts[:train_size]
            test_ts = ts[train_size:]
            
            if len(test_ts) < 5:  # Muito poucos dados para teste
                return {}
            
            # Gerar previsões históricas para validação
            forecast_horizon = min(30, len(test_ts))  # Usar horizonte menor se necessário
            historical_forecasts = model.historical_forecasts(
                series=ts,
                start=train_ratio,
                forecast_horizon=forecast_horizon,
                stride=1,
                retrain=False,
                verbose=False,
                show_warnings=False
            )
            
            # Extrair previsão mediana se probabilística
            if historical_forecasts.is_probabilistic:
                median_forecast = historical_forecasts.quantile_timeseries(0.5)
            else:
                median_forecast = historical_forecasts
            
            # Calcular métricas no período de sobreposição
            actual_test = ts.slice_intersect(median_forecast)
            pred_test = median_forecast.slice_intersect(ts)
            
            if len(actual_test) == 0 or len(pred_test) == 0:
                return {}
            
            # Calcular métricas
            metrics = {}
            
            # MAPE com tratamento de zeros
            try:
                metrics['MAPE'] = float(mape(actual_test, pred_test))
            except (ValueError, ZeroDivisionError):
                metrics['MAPE'] = float(custom_mape(actual_test, pred_test))
            
            # Outras métricas
            metrics['MAE'] = float(mae(actual_test, pred_test))
            metrics['RMSE'] = float(rmse(actual_test, pred_test))
            metrics['sMAPE'] = float(smape(actual_test, pred_test))
            
            # Métricas específicas de vendas
            actual_values = actual_test.values()
            pred_values = pred_test.values()
            
            # Bias (erro médio)
            metrics['Bias'] = float(np.mean(pred_values - actual_values))
            
            # R-squared
            ss_res = np.sum((actual_values - pred_values) ** 2)
            ss_tot = np.sum((actual_values - np.mean(actual_values)) ** 2)
            metrics['R2'] = float(1 - (ss_res / ss_tot)) if ss_tot != 0 else 0.0
            
            # Adicionar informações do horizonte
            metrics['forecast_horizon'] = float(forecast_horizon)
            metrics['empresa'] = empresa
            
            return metrics
            
        except Exception as e:
            self._log_info(f"Erro ao calcular métricas para {empresa}: {e}")
            return {}
    
    def run_forecast_for_database(self, database: str) -> Dict[str, Any]:
        """Executa forecast completo para um database."""
        self._log_info(f"Iniciando forecast para database: {database}")
        
        start_time = time.perf_counter()
        results = {
            'database': database,
            'successful_forecasts': 0,
            'total_empresas': 0,
            'forecasts': {},
            'metrics': {},
            'errors': []
        }
        
        try:
            # 1. Carregar dados
            df = self.load_data(database)
            
            # 2. Filtrar e preparar séries
            series_dict = self.filter_and_prepare_series(df)
            results['total_empresas'] = len(series_dict)
            
            if not series_dict:
                self._log_info("❌ Nenhuma empresa para processar")
                return results
            
            # 3. Processar cada empresa
            horizon = self.config.get('model', {}).get('forecast_horizon', 30)
            
            for empresa, data in series_dict.items():
                try:
                    self._log_info(f"Processando empresa: {empresa}")
                    
                    # Criar TimeSeries
                    ts = self.create_time_series(data, empresa)
                    if ts is None:
                        continue
                    
                    # Treinar modelo
                    model = self.train_model(ts, empresa)
                    if model is None:
                        continue
                    
                    # Gerar forecast
                    forecast = self.generate_forecast(model, empresa, horizon)
                    if forecast is None:
                        continue
                    
                    # Aplicar transformações inversas
                    forecast_final = self.inverse_transform_forecast(forecast, empresa)
                    
                    # Calcular métricas de qualidade
                    metrics = self.calculate_metrics(ts, empresa)
                    
                    # Armazenar resultado
                    results['forecasts'][empresa] = {
                        'forecast': forecast_final,
                        'original_data': data,
                        'model': model
                    }
                    
                    # Armazenar métricas
                    if metrics:
                        results['metrics'][empresa] = metrics
                        self._log_info(f"✅ Métricas calculadas para {empresa}: MAPE={metrics.get('MAPE', 0):.2f}%, R²={metrics.get('R2', 0):.3f}")
                    
                    results['successful_forecasts'] += 1
                    
                except Exception as e:
                    error_msg = f"Erro ao processar empresa {empresa}: {e}"
                    self._log_info(f"❌ {error_msg}")
                    results['errors'].append(error_msg)
            
            self._log_info(f"✅ Forecast concluído: {results['successful_forecasts']}/{results['total_empresas']} empresas")
            
            # 4. Salvar resultados
            if results['successful_forecasts'] > 0:
                self.save_results(results, database)
            
            # 5. Calcular métricas consolidadas
            if results.get('metrics'):
                all_mape = [m.get('MAPE', 0) for m in results['metrics'].values() if m.get('MAPE', 0) > 0]
                all_r2 = [m.get('R2', 0) for m in results['metrics'].values()]
                
                results['average_mape'] = np.mean(all_mape) if all_mape else 0
                results['average_r2'] = np.mean(all_r2) if all_r2 else 0
                results['best_mape'] = min(all_mape) if all_mape else 0
                results['worst_mape'] = max(all_mape) if all_mape else 0
                
                self._log_info(f"📊 Métricas consolidadas: MAPE médio={results['average_mape']:.2f}%, R² médio={results['average_r2']:.3f}")
            
            # Adicionar campo de sucesso
            results['success'] = results['successful_forecasts'] > 0
            results['companies_processed'] = results['successful_forecasts']
            
            # Log do resultado
            self._log_info(f"✅ Forecast concluído: {results['successful_forecasts']}/{results['total_empresas']} empresas")
            elapsed = time.perf_counter() - start_time
            results['elapsed_seconds'] = elapsed
            self._log_info(f"⏱️ Tempo total (vendas_empresa pipeline): {elapsed:.2f}s")

            return results
            
        except Exception as e:
            error_msg = f"Erro geral no forecast para {database}: {e}"
            self._log_info(f"❌ {error_msg}")
            results['errors'].append(error_msg)
            results['success'] = False
            results['companies_processed'] = 0
            return results
    
    def save_results(self, results: Dict[str, Any], database: str):
        """Salva resultados em CSV."""
        self._log_info("Salvando resultados...")
        
        # Criar diretório específico para o database
        os.makedirs(f"{RESULTS_DIR}/{database}/vendas_empresa", exist_ok=True)
        
        try:
            # 1. Salvar previsões
            output_rows = []
            
            for empresa, data in results['forecasts'].items():
                forecast = data['forecast']
                
                # Converter forecast para DataFrame
                forecast_df = forecast.to_dataframe()
                forecast_df.reset_index(inplace=True)
                forecast_df['Empresa'] = str(empresa)  # Garantir que seja texto
                forecast_df['Database'] = database
                
                # Renomear colunas
                forecast_df.columns = ['data', 'vendas_forecast', 'Empresa', 'Database']
                
                output_rows.append(forecast_df)
            
            # Concatenar todos os resultados
            if output_rows:
                final_df = pd.concat(output_rows, ignore_index=True)
                
                # Salvar CSV de previsões
                output_file = f"{RESULTS_DIR}/{database}/vendas_empresa/previsao_vendas_empresa.csv"
                final_df.to_csv(output_file, index=False, sep=';')
                
                self._log_info(f"✅ Previsões salvas em: {output_file}")
            
            # 2. Salvar métricas por empresa
            if results.get('metrics'):
                metrics_rows = []
                
                for empresa, metrics in results['metrics'].items():
                    for metric_name, metric_value in metrics.items():
                        if metric_name not in ['empresa']:  # Pular campo empresa
                            metrics_rows.append({
                                'empresa': str(empresa),  # Garantir que seja texto
                                'metrica': metric_name,
                                'valor': metric_value
                            })
                
                if metrics_rows:
                    metrics_df = pd.DataFrame(metrics_rows)
                    metrics_file = f"{RESULTS_DIR}/{database}/vendas_empresa/metricas_vendas_empresa.csv"
                    metrics_df.to_csv(metrics_file, index=False, sep=';')
                    
                    self._log_info(f"✅ Métricas salvas em: {metrics_file}")
                    
                    # 3. Salvar resumo consolidado das métricas
                    self._save_metrics_summary(results['metrics'], database)
            
        except Exception as e:
            self._log_info(f"Erro ao salvar resultados: {e}")
    
    def _save_metrics_summary(self, metrics_dict: Dict[str, Dict], database: str):
        """Salva resumo consolidado das métricas por empresa."""
        try:
            summary_rows = []
            
            for empresa, metrics in metrics_dict.items():
                summary_row = {
                    'empresa': str(empresa),  # Garantir que seja texto
                    'MAPE': metrics.get('MAPE', 0),
                    'MAE': metrics.get('MAE', 0),
                    'RMSE': metrics.get('RMSE', 0),
                    'sMAPE': metrics.get('sMAPE', 0),
                    'R2': metrics.get('R2', 0),
                    'Bias': metrics.get('Bias', 0),
                    'forecast_horizon': metrics.get('forecast_horizon', 0)
                }
                summary_rows.append(summary_row)
            
            if summary_rows:
                summary_df = pd.DataFrame(summary_rows)
                summary_file = f"{RESULTS_DIR}/{database}/vendas_empresa/resumo_metricas_empresas.csv"
                summary_df.to_csv(summary_file, index=False, sep=';')
                
                self._log_info(f"✅ Resumo de métricas salvo em: {summary_file}")
                
        except Exception as e:
            self._log_info(f"Erro ao salvar resumo de métricas: {e}")


        # Upload FTP (se configurado)
        # Upload FTP dos resultados
        if True:
            try:
                print("[UPLOAD] Uploading vendas empresa results to FTP...")
                
                with ForecastFTPUploader() as ftp:
                    # Usar o diretório de resultados do database
                    results_folder = f"{RESULTS_DIR}/{database}/vendas_empresa"
                    result = ftp.upload_vendas_empresa_results(database, results_folder)
                    
                    if result.get("success", False):
                        print(f"[SUCCESS] FTP Upload: {result.get('message', 'Success')}")
                        uploaded_files = result.get("uploaded_files", [])
                        if uploaded_files:
                            print(f"[FILES] Uploaded: {', '.join(uploaded_files)}")
                    else:
                        print(f"[WARNING] FTP Upload failed: {result.get('message', 'Unknown error')}")
                        
            except Exception as e:
                print(f"[WARNING] Erro no upload FTP: {e}")

def run_vendas_empresa_forecast(database: str) -> Dict[str, Any]:
    """Função pública para executar forecast de vendas por empresa para um database."""
    start_time = time.perf_counter()
    pipeline = VendasEmpresaForecastPipeline()
    results = pipeline.run_forecast_for_database(database)
    elapsed = time.perf_counter() - start_time
    print(f"⏱️ Tempo total (run_vendas_empresa_forecast:{database}): {elapsed:.2f}s")
    return results

def run_single_database_vendas_empresa(database: str = None) -> Dict[str, Any]:
    """Executa forecast para um database específico."""
    if not database:
        print("❌ Nome do database é obrigatório")
        return {}
    
    print(f"\n{'='*60}")
    print(f"🏢 FORECAST DE VENDAS POR EMPRESA - {database}")
    print(f"{'='*60}")
    start_time = time.perf_counter()
    results = run_vendas_empresa_forecast(database)
    elapsed = time.perf_counter() - start_time
    print(f"⏱️ Tempo total (run_single_database_vendas_empresa:{database}): {elapsed:.2f}s")
    return results

def run_all_databases() -> Dict[str, Any]:
    """Executa forecast de vendas por empresa para todos os databases configurados."""
    
    config_path = Path("config/config_databases.yaml")
    if not config_path.exists():
        print(f"❌ Configuration file not found: {config_path}")
        return {}

    with open(config_path, 'r', encoding='utf-8') as f:
        db_config = yaml.safe_load(f)
    
    databases = db_config.get('databases', [])
    if not databases:
        print("❌ No databases found in the configuration file.")
        return {}

    print("\n🏢 PROCESSAMENTO DE VENDAS POR EMPRESA EM LOTE")
    print(f"🚀 Found {len(databases)} databases to process for COMPANY SALES forecasting.")
    
    successful_count = 0
    total_count = len(databases)
    detailed_results = []
    
    for i, db_name in enumerate(databases, 1):
        print("\n" + "="*60)
        print(f"🏢 Processing COMPANY SALES {i}/{total_count}: {db_name}")
        print("="*60)
        
        # Verificar se arquivo de vendas por empresa existe
        file_path = DATASET_DIR / f"{db_name}_vendas_empresa.csv"
        
        if not file_path.exists():
            print(f"⚠️ Company sales data file not found for {db_name}, skipping.")
            detailed_results.append({
                'database': db_name,
                'status': 'FILE_NOT_FOUND',
                'companies_processed': 0,
                'avg_mape': None,
                'avg_r2': None
            })
            continue
        
        try:
            db_start = time.perf_counter()
            # Executar pipeline de vendas por empresa
            results = run_vendas_empresa_forecast(db_name)
            db_elapsed = time.perf_counter() - db_start
            print(f"⏱️ Tempo (database {db_name}): {db_elapsed:.2f}s")
            
            if results and results.get('success', False):
                # Extrair informações dos resultados
                companies_processed = results.get('companies_processed', 0)
                avg_mape = results.get('average_mape', 0)
                avg_r2 = results.get('average_r2', 0)
                best_mape = results.get('best_mape', 0)
                worst_mape = results.get('worst_mape', 0)
                
                print(f"✅ Successfully processed company sales for {db_name}")
                print(f"   🏢 Companies processed: {companies_processed}")
                if avg_mape > 0:
                    print(f"   📊 Average MAPE: {avg_mape:.2f}%")
                    print(f"   📈 MAPE range: {best_mape:.2f}% - {worst_mape:.2f}%")
                if avg_r2 != 0:
                    print(f"   📊 Average R²: {avg_r2:.3f}")
                
                # Classificar qualidade geral
                if avg_mape > 0:
                    if avg_mape <= 5:
                        quality = "🟢"
                    elif avg_mape <= 15:
                        quality = "🟡"
                    elif avg_mape <= 30:
                        quality = "🟠"
                    else:
                        quality = "🔴"
                    print(f"   {quality} Qualidade geral: {'Excelente' if quality == '🟢' else 'Boa' if quality == '🟡' else 'Regular' if quality == '🟠' else 'Baixa'}")
                
                detailed_results.append({
                    'database': db_name,
                    'status': 'SUCCESS',
                    'companies_processed': companies_processed,
                    'avg_mape': avg_mape,
                    'avg_r2': avg_r2,
                    'best_mape': best_mape,
                    'worst_mape': worst_mape,
                    'quality': quality if avg_mape > 0 else 'N/A'
                })
                
                successful_count += 1
            else:
                error_msg = results.get('error', 'Unknown error') if results else 'No results returned'
                print(f"❌ Failed to process company sales for {db_name}: {error_msg}")
                detailed_results.append({
                    'database': db_name,
                    'status': 'FAILED',
                    'companies_processed': 0,
                    'avg_mape': None,
                    'avg_r2': None,
                    'error': error_msg
                })
                
        except Exception as e:
            print(f"❌ Error processing company sales for {db_name}: {e}")
            detailed_results.append({
                'database': db_name,
                'status': 'ERROR',
                'error': str(e),
                'companies_processed': 0,
                'avg_mape': None,
                'avg_r2': None
            })
    
    # Resumo final
    print("\n" + "="*70)
    print("📊 RESUMO CONSOLIDADO - VENDAS POR EMPRESA")
    print("="*70)
    
    print(f"✅ Sucessos: {successful_count}/{total_count} ({successful_count/total_count*100:.1f}%)")
    
    # Salvar resumo em arquivo se houver sucessos
    if successful_count > 0:
        print("\n💾 Resultados detalhados salvos em: results/vendas_empresa/")
        
        try:
            import pandas as pd
            summary_df = pd.DataFrame(detailed_results)
            summary_path = RESULTS_DIR / 'batch_company_sales_summary.csv'
            summary_df.to_csv(summary_path, index=False, sep=';', encoding='utf-8')
            print(f"📋 Resumo consolidado: {summary_path}")
        except Exception as e:
            print(f"⚠️ Não foi possível salvar resumo: {e}")
    
    return {
        'successful_count': successful_count, 
        'total_count': total_count, 
        'results': detailed_results
    }




def main():
    """Main function com opções para execução de vendas por empresa."""
    import sys
    
    print("🏢 COMPANY SALES FORECASTING PIPELINE")
    print("="*50)

    # Menu interativo
    print("Escolha uma opção:")
    print("1. 🚀 Processar todos os databases (vendas por empresa)")
    print("2. 🎯 Processar database específico (vendas por empresa)")
    print("3. 🧪 Teste com 005RG_ERP_BI")
    
    try:
        choice = input("\nOpção (1-3): ").strip()
        
        if choice == "1":
            run_all_databases()
            
        elif choice == "2":
            db_name = input("Digite o nome do database: ").strip()
            if db_name:
                run_single_database_vendas_empresa(db_name)
            else:
                print("❌ Nome do database é obrigatório")
                
        elif choice == "3":
            run_single_database_vendas_empresa('005RG_ERP_BI')
            
        else:
            print("❌ Opção inválida")
            
    except KeyboardInterrupt:
        print("\n👋 Saindo...")


if __name__ == "__main__":
    main()

