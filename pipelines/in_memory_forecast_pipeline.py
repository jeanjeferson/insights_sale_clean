"""
Pipeline Unificado em Memória - Execução Completa dos Forecasts
Executa todos os pipelines sem salvamento local, enviando apenas resultados finais para FTP.
"""

from __future__ import annotations
import time
import sys
import os
import warnings
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Union
import pandas as pd
import numpy as np
import yaml
from io import StringIO, BytesIO

# Suprimir warnings
warnings.filterwarnings('ignore')
os.environ['PYTHONWARNINGS'] = 'ignore'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# Adicionar o diretório raiz ao PYTHONPATH
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Imports dos pipelines existentes
from utils.sql_query import SimpleDataExtractor, extract_all_clients
from utils.ftp_uploader import ForecastFTPUploader

# Darts imports com supressão
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from darts import TimeSeries
    from darts.models import LightGBMModel
    from darts.dataprocessing.transformers import Scaler, MissingValuesFiller, InvertibleMapper
    from darts.utils.timeseries_generation import datetime_attribute_timeseries
    from darts.utils.statistics import check_seasonality
    from darts.metrics import mape, mae, rmse, smape

class InMemoryForecastPipeline:
    """
    Pipeline unificado que executa todo o processo em memória:
    - Carregamento de dados direto do banco
    - Processamento completo sem salvamento local
    - Consolidação de todos os resultados
    - Upload único para FTP ao final
    """
    
    def __init__(self, config_path: str = "config/config_databases.yaml"):
        """Initialize pipeline with configuration."""
        self.config = self._load_config(config_path)
        self.extractor = SimpleDataExtractor()
        self.results = {}
        self.metadata = {
            'start_time': None,
            'end_time': None,
            'databases_processed': [],
            'total_companies': 0,
            'overall_quality': 'N/A'
        }
        
        # Logging prefix
        self.log_prefix = "[in_memory]"
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        return {'databases': []}
    
    def _log_info(self, message: str):
        """Log message with prefix."""
        print(f"{self.log_prefix} {message}")
    
    def load_all_data(self, database: str) -> Dict[str, Any]:
        """Carrega todos os dados necessários para um database em memória."""
        self._log_info(f"Carregando dados para {database}")
        
        data = {
            'database': database,
            'clientes': None,
            'vendas': None,
            'vendas_empresa': None,
            'loaded_at': datetime.now()
        }
        
        try:
            # 1. Carregar dados de clientes
            self._log_info("Carregando dados de clientes...")
            clientes_result = extract_all_clients()
            data['clientes'] = {
                'result': clientes_result,
                'success': True
            }
            
            # 2. Carregar dados de vendas
            self._log_info("Carregando dados de vendas...")
            vendas_result = self.extractor.extract_vendas_data(database)
            if vendas_result.get('success', False):
                # Converter CSV em DataFrame diretamente
                csv_content = vendas_result.get('csv_content', '')
                if csv_content:
                    df = pd.read_csv(StringIO(csv_content), sep=';')
                    data['vendas'] = {
                        'data': df,
                        'success': True
                    }
                else:
                    data['vendas'] = {'success': False, 'error': 'No CSV content'}
            else:
                data['vendas'] = {'success': False, 'error': vendas_result.get('error', 'Unknown error')}
            
            # 3. Carregar dados de vendas por empresa
            self._log_info("Carregando dados de vendas por empresa...")
            vendas_empresa_result = self.extractor.extract_vendas_empresa_data(database)
            if vendas_empresa_result.get('success', False):
                csv_content = vendas_empresa_result.get('csv_content', '')
                if csv_content:
                    df = pd.read_csv(StringIO(csv_content), sep=';')
                    data['vendas_empresa'] = {
                        'data': df,
                        'success': True
                    }
                else:
                    data['vendas_empresa'] = {'success': False, 'error': 'No CSV content'}
            else:
                data['vendas_empresa'] = {'success': False, 'error': vendas_empresa_result.get('error', 'Unknown error')}
            
            self._log_info(f"✅ Dados carregados para {database}")
            return data
            
        except Exception as e:
            self._log_info(f"❌ Erro ao carregar dados para {database}: {e}")
            return data
    
    def process_vendas_forecast(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Processa forecast de vendas em memória."""
        if not data.get('vendas', {}).get('success', False):
            return {'success': False, 'error': 'No sales data available'}
        
        self._log_info("Processando forecast de vendas...")
        
        try:
            df = data['vendas']['data']
            
            # Preparar dados para TimeSeries
            df_clean = df.copy()
            df_clean.columns = [col.strip() for col in df_clean.columns]
            
            # Identificar colunas
            date_col = df_clean.columns[0]
            sales_col = df_clean.columns[1]
            
            # Limpar e preparar dados
            df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
            df_clean[sales_col] = pd.to_numeric(df_clean[sales_col], errors='coerce')
            df_clean = df_clean.dropna(subset=[date_col, sales_col])
            df_clean = df_clean[df_clean[sales_col] >= 0]
            
            # Remover duplicatas e somar por data
            df_clean = df_clean.groupby(date_col)[sales_col].sum().reset_index()
            df_clean = df_clean.sort_values(date_col)
            
            # Criar TimeSeries
            series = TimeSeries.from_dataframe(
                df_clean,
                time_col=date_col,
                value_cols=sales_col,
                freq='D',
                fill_missing_dates=True,
                fillna_value=0
            )
            
            # Configurações do modelo
            model_config = {
                'lags': 56,
                'forecast_horizon': [30, 60, 90],
                'quantiles': [0.1, 0.5, 0.9],
                'random_state': 42,
                'n_estimators': 500,
                'num_leaves': 63,
                'learning_rate': 0.08,
                'max_depth': -1,
                'min_data_in_leaf': 20,
                'feature_fraction': 0.9,
                'bagging_fraction': 0.9,
                'bagging_freq': 5,
                'verbose': -1
            }
            
            # Preprocessamento
            processed_series = self._preprocess_sales_series(series)
            
            # Criar covariáveis temporais
            covariates = self._create_temporal_covariates(series, model_config['forecast_horizon'])
            
            # Treinar modelo
            model = self._train_sales_model(processed_series, covariates, model_config)
            
            # Gerar previsões para múltiplos horizontes
            forecasts = {}
            metrics = {}
            
            for horizon in model_config['forecast_horizon']:
                self._log_info(f"Gerando previsão para {horizon} dias...")
                
                # Gerar previsão
                forecast = model.predict(n=horizon, future_covariates=covariates)
                
                # Aplicar transformações inversas
                forecast_final = self._inverse_transform_forecast(forecast, processed_series)
                
                # Converter para DataFrame
                forecast_df = forecast_final.to_dataframe().reset_index()
                forecast_df.columns = ['data', 'vendas_previstas']
                forecast_df['forecast_horizon'] = horizon
                
                forecasts[horizon] = forecast_df
                
                # Calcular métricas (usar backtest de 30 dias como base)
                if horizon == 30:
                    metrics[horizon] = self._calculate_sales_metrics(processed_series, model, covariates)
                else:
                    # Estimar métricas para horizontes maiores
                    base_metrics = metrics.get(30, {})
                    metrics[horizon] = self._estimate_horizon_metrics(base_metrics, horizon)
            
            # Criar arquivo consolidado
            consolidated_df = self._create_consolidated_sales_data(series, forecasts)
            
            return {
                'success': True,
                'historical': series.to_dataframe().reset_index(),
                'forecasts': forecasts,
                'metrics': metrics,
                'consolidated': consolidated_df,
                'model': model
            }
            
        except Exception as e:
            self._log_info(f"❌ Erro no processamento de vendas: {e}")
            return {'success': False, 'error': str(e)}
    
    def process_vendas_empresa_forecast(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Processa forecast de vendas por empresa em memória."""
        if not data.get('vendas_empresa', {}).get('success', False):
            return {'success': False, 'error': 'No company sales data available'}
        
        self._log_info("Processando forecast de vendas por empresa...")
        
        try:
            df = data['vendas_empresa']['data']
            
            # Preparar dados
            df_clean = df.copy()
            df_clean.columns = [col.strip() for col in df_clean.columns]
            
            # Validar colunas
            expected_columns = ['data', 'Empresa', 'vendas']
            missing_columns = [col for col in expected_columns if col not in df_clean.columns]
            if missing_columns:
                return {'success': False, 'error': f'Missing columns: {missing_columns}'}
            
            # Converter tipos
            df_clean['vendas'] = pd.to_numeric(df_clean['vendas'], errors='coerce')
            df_clean['data'] = pd.to_datetime(df_clean['data'])
            df_clean = df_clean.dropna()
            
            # Aplicar filtros de qualidade
            filtered_series = self._filter_company_series(df_clean)
            
            if not filtered_series:
                return {'success': False, 'error': 'No companies passed quality filters'}
            
            # Processar cada empresa
            forecasts_by_company = {}
            metrics_by_company = {}
            all_forecasts = []
            
            for empresa, empresa_data in filtered_series.items():
                try:
                    self._log_info(f"Processando empresa: {empresa}")
                    
                    # Criar TimeSeries
                    ts = TimeSeries.from_dataframe(
                        empresa_data,
                        time_col=None,
                        value_cols=['vendas'],
                        freq='D'
                    )
                    
                    # Preprocessamento
                    processed_ts = self._preprocess_company_series(ts, empresa)
                    
                    # Treinar modelo
                    model = self._train_company_model(processed_ts, empresa)
                    
                    # Gerar previsão
                    forecast = model.predict(n=30)
                    forecast_final = self._inverse_transform_company_forecast(forecast, empresa)
                    
                    # Converter para DataFrame
                    forecast_df = forecast_final.to_dataframe().reset_index()
                    forecast_df['Empresa'] = empresa
                    forecast_df['Database'] = data['database']
                    forecast_df.columns = ['data', 'vendas_forecast', 'Empresa', 'Database']
                    
                    forecasts_by_company[empresa] = forecast_df
                    all_forecasts.append(forecast_df)
                    
                    # Calcular métricas
                    metrics_by_company[empresa] = self._calculate_company_metrics(processed_ts, model)
                    
                except Exception as e:
                    self._log_info(f"❌ Erro ao processar empresa {empresa}: {e}")
                    continue
            
            # Consolidar resultados
            if all_forecasts:
                consolidated_df = pd.concat(all_forecasts, ignore_index=True)
                
                # Calcular métricas consolidadas
                all_mape = [m.get('MAPE', 0) for m in metrics_by_company.values() if m.get('MAPE', 0) > 0]
                all_r2 = [m.get('R2', 0) for m in metrics_by_company.values()]
                
                consolidated_metrics = {
                    'average_mape': np.mean(all_mape) if all_mape else 0,
                    'average_r2': np.mean(all_r2) if all_r2 else 0,
                    'best_mape': min(all_mape) if all_mape else 0,
                    'worst_mape': max(all_mape) if all_mape else 0,
                    'companies_processed': len(forecasts_by_company)
                }
                
                return {
                    'success': True,
                    'forecasts_by_company': forecasts_by_company,
                    'metrics_by_company': metrics_by_company,
                    'consolidated': consolidated_df,
                    'summary_metrics': consolidated_metrics
                }
            else:
                return {'success': False, 'error': 'No successful company forecasts'}
                
        except Exception as e:
            self._log_info(f"❌ Erro no processamento de vendas por empresa: {e}")
            return {'success': False, 'error': str(e)}
    
    def _preprocess_sales_series(self, series: TimeSeries) -> TimeSeries:
        """Preprocessa série de vendas."""
        processed = series.copy()
        
        # Fill missing values
        filler = MissingValuesFiller(fill='auto')
        processed = filler.transform(processed)
        
        # Log transformation
        log_transformer = InvertibleMapper(fn=np.log1p, inverse_fn=np.expm1)
        processed = log_transformer.transform(processed)
        
        # Scaling - fit no scaler antes de transformar
        scaler = Scaler()
        scaler.fit(processed)  # Fit antes de transform
        processed = scaler.transform(processed)
        
        # Armazenar scaler para uso posterior
        self.sales_scaler = scaler
        self.sales_log_transformer = log_transformer
        
        return processed
    
    def _create_temporal_covariates(self, series: TimeSeries, horizons: List[int]) -> Optional[TimeSeries]:
        """Cria covariáveis temporais."""
        max_horizon = max(horizons)
        future_index = pd.date_range(
            start=series.start_time(),
            periods=len(series) + max_horizon,
            freq=series.freq
        )
        
        attributes = ['dayofweek', 'month', 'quarter', 'dayofyear', 'weekofyear']
        covariates_list = []
        
        for attr in attributes:
            try:
                cov = datetime_attribute_timeseries(
                    time_index=future_index,
                    attribute=attr,
                    one_hot=False
                )
                covariates_list.append(cov)
            except Exception:
                continue
        
        if not covariates_list:
            return None
        
        # Stack all covariates
        combined_cov = covariates_list[0]
        for cov in covariates_list[1:]:
            combined_cov = combined_cov.stack(cov)
        
        return combined_cov
    
    def _train_sales_model(self, series: TimeSeries, covariates: Optional[TimeSeries], config: Dict) -> LightGBMModel:
        """Treina modelo de vendas."""
        model = LightGBMModel(
            lags=config['lags'],
            lags_future_covariates=(14, 14) if covariates is not None else None,
            output_chunk_length=max(config['forecast_horizon']),
            likelihood='quantile',
            quantiles=config['quantiles'],
            random_state=config['random_state'],
            n_estimators=config['n_estimators'],
            num_leaves=config['num_leaves'],
            learning_rate=config['learning_rate'],
            max_depth=config['max_depth'],
            min_data_in_leaf=config['min_data_in_leaf'],
            feature_fraction=config['feature_fraction'],
            bagging_fraction=config['bagging_fraction'],
            bagging_freq=config['bagging_freq'],
            verbose=config['verbose']
        )
        
        model.fit(series, future_covariates=covariates)
        return model
    
    def _inverse_transform_forecast(self, forecast: TimeSeries, original_series: TimeSeries) -> TimeSeries:
        """Aplica transformações inversas no forecast."""
        try:
            # Aplicar transformações inversas na ordem correta
            processed_forecast = forecast.copy()
            
            # 1. Inverse scaling
            if hasattr(self, 'sales_scaler') and self.sales_scaler is not None:
                processed_forecast = self.sales_scaler.inverse_transform(processed_forecast)
            
            # 2. Inverse log transformation
            if hasattr(self, 'sales_log_transformer') and self.sales_log_transformer is not None:
                processed_forecast = self.sales_log_transformer.inverse_transform(processed_forecast)
            
            return processed_forecast
            
        except Exception as e:
            self._log_info(f"Erro ao aplicar transformações inversas: {e}")
            return forecast
    
    def _calculate_sales_metrics(self, series: TimeSeries, model: LightGBMModel, covariates: Optional[TimeSeries]) -> Dict[str, float]:
        """Calcula métricas de vendas."""
        try:
            # Backtest de 30 dias
            historical_forecasts = model.historical_forecasts(
                series=series,
                future_covariates=covariates,
                start=0.75,
                forecast_horizon=30,
                stride=1,
                retrain=False,
                verbose=False,
                show_warnings=False
            )
            
            # Extrair previsão mediana
            if historical_forecasts.is_probabilistic:
                median_forecast = historical_forecasts.quantile_timeseries(0.5)
            else:
                median_forecast = historical_forecasts
            
            # Calcular métricas
            actual_test = series.slice_intersect(median_forecast)
            pred_test = median_forecast.slice_intersect(series)
            
            metrics = {}
            metrics['MAPE'] = float(mape(actual_test, pred_test))
            metrics['MAE'] = float(mae(actual_test, pred_test))
            metrics['RMSE'] = float(rmse(actual_test, pred_test))
            metrics['sMAPE'] = float(smape(actual_test, pred_test))
            
            # Bias e R²
            actual_values = actual_test.values().flatten()
            pred_values = pred_test.values().flatten()
            metrics['Bias'] = float(np.mean(pred_values - actual_values))
            
            ss_res = np.sum((actual_values - pred_values) ** 2)
            ss_tot = np.sum((actual_values - np.mean(actual_values)) ** 2)
            metrics['R2'] = float(1 - (ss_res / ss_tot)) if ss_tot != 0 else 0.0
            
            return metrics
            
        except Exception as e:
            self._log_info(f"Erro ao calcular métricas: {e}")
            return {'MAPE': 0.0, 'MAE': 0.0, 'RMSE': 0.0, 'sMAPE': 0.0, 'Bias': 0.0, 'R2': 0.0}
    
    def _estimate_horizon_metrics(self, base_metrics: Dict[str, float], horizon: int) -> Dict[str, float]:
        """Estima métricas para horizontes maiores."""
        horizon_metrics = base_metrics.copy()
        horizon_metrics['forecast_horizon'] = horizon
        
        # Ajustar métricas para horizontes mais longos
        if horizon > 30:
            degradation_factor = 1 + (horizon - 30) / 30 * 0.3
            horizon_metrics['MAPE'] = horizon_metrics.get('MAPE', 0) * degradation_factor
            horizon_metrics['MAE'] = horizon_metrics.get('MAE', 0) * degradation_factor
            horizon_metrics['RMSE'] = horizon_metrics.get('RMSE', 0) * degradation_factor
            horizon_metrics['sMAPE'] = horizon_metrics.get('sMAPE', 0) * degradation_factor
            horizon_metrics['R2'] = max(0, horizon_metrics.get('R2', 0) * (1 - (horizon - 30) / 100))
        
        return horizon_metrics
    
    def _create_consolidated_sales_data(self, historical: TimeSeries, forecasts: Dict[int, pd.DataFrame]) -> pd.DataFrame:
        """Cria arquivo consolidado de vendas."""
        # Implementar consolidação de dados históricos + previsões
        # Por simplicidade, retornar DataFrame vazio por enquanto
        return pd.DataFrame()
    
    def _filter_company_series(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Filtra séries de empresas por qualidade."""
        # Configurações de filtro
        min_total_sales = 2000
        min_records = 40
        max_zero_ratio = 0.8
        target_percentile = 80
        max_groups = 30
        
        # Calcular estatísticas por empresa
        empresa_stats = df.groupby('Empresa').agg({
            'vendas': ['sum', 'count', lambda x: (x == 0).sum() / len(x)]
        }).round(2)
        empresa_stats.columns = ['total_vendas', 'num_records', 'zero_ratio']
        empresa_stats = empresa_stats.reset_index()
        
        # Aplicar filtros
        empresa_stats = empresa_stats[empresa_stats['total_vendas'] >= min_total_sales]
        empresa_stats = empresa_stats[empresa_stats['num_records'] >= min_records]
        empresa_stats = empresa_stats[empresa_stats['zero_ratio'] <= max_zero_ratio]
        
        # Selecionar top percentil
        if len(empresa_stats) > 0:
            cutoff_value = np.percentile(empresa_stats['total_vendas'], 100 - target_percentile)
            empresa_stats = empresa_stats[empresa_stats['total_vendas'] >= cutoff_value]
        
        # Limitar número máximo
        if max_groups and len(empresa_stats) > max_groups:
            empresa_stats = empresa_stats.nlargest(max_groups, 'total_vendas')
        
        # Preparar séries filtradas
        empresas_selecionadas = empresa_stats['Empresa'].tolist()
        df_filtered = df[df['Empresa'].isin(empresas_selecionadas)]
        
        series_dict = {}
        for empresa in empresas_selecionadas:
            empresa_data = df_filtered[df_filtered['Empresa'] == empresa].copy()
            empresa_data = empresa_data.sort_values('data')
            series_dict[empresa] = empresa_data[['data', 'vendas']].set_index('data')
        
        return series_dict
    
    def _preprocess_company_series(self, ts: TimeSeries, empresa: str) -> TimeSeries:
        """Preprocessa série de uma empresa."""
        processed = ts.copy()
        
        # Fill missing values
        filler = MissingValuesFiller(fill='auto')
        processed = filler.transform(processed)
        
        # Log transformation
        log_transformer = InvertibleMapper(fn=np.log1p, inverse_fn=np.expm1)
        processed = log_transformer.transform(processed)
        
        # Scaling - fit no scaler antes de transformar
        scaler = Scaler()
        scaler.fit(processed)  # Fit antes de transform
        processed = scaler.transform(processed)
        
        # Armazenar scalers para uso posterior
        if not hasattr(self, 'company_scalers'):
            self.company_scalers = {}
        if not hasattr(self, 'company_log_transformers'):
            self.company_log_transformers = {}
            
        self.company_scalers[empresa] = scaler
        self.company_log_transformers[empresa] = log_transformer
        
        return processed
    
    def _train_company_model(self, ts: TimeSeries, empresa: str) -> LightGBMModel:
        """Treina modelo para uma empresa."""
        model = LightGBMModel(
            lags=28,
            output_chunk_length=30,
            random_state=42,
            quantiles=[0.1, 0.5, 0.9],
            verbose=-1,
            n_estimators=100,
            num_leaves=15,
            learning_rate=0.1,
            max_depth=3,
            min_data_in_leaf=30,
            feature_fraction=0.7,
            bagging_fraction=0.7,
            subsample_freq=1,
            objective='regression',
            metric='rmse',
            boosting_type='gbdt',
            force_col_wise=True
        )
        
        model.fit(ts)
        return model
    
    def _inverse_transform_company_forecast(self, forecast: TimeSeries, empresa: str) -> TimeSeries:
        """Aplica transformações inversas no forecast da empresa."""
        try:
            processed_forecast = forecast.copy()
            
            # 1. Inverse scaling
            if hasattr(self, 'company_scalers') and empresa in self.company_scalers:
                processed_forecast = self.company_scalers[empresa].inverse_transform(processed_forecast)
            
            # 2. Inverse log transformation
            if hasattr(self, 'company_log_transformers') and empresa in self.company_log_transformers:
                processed_forecast = self.company_log_transformers[empresa].inverse_transform(processed_forecast)
            
            return processed_forecast
            
        except Exception as e:
            self._log_info(f"Erro ao aplicar transformações inversas para {empresa}: {e}")
            return forecast
    
    def _calculate_company_metrics(self, ts: TimeSeries, model: LightGBMModel) -> Dict[str, float]:
        """Calcula métricas para uma empresa."""
        try:
            # Backtest
            historical_forecasts = model.historical_forecasts(
                series=ts,
                start=0.8,
                forecast_horizon=30,
                stride=1,
                retrain=False,
                verbose=False,
                show_warnings=False
            )
            
            if historical_forecasts.is_probabilistic:
                median_forecast = historical_forecasts.quantile_timeseries(0.5)
            else:
                median_forecast = historical_forecasts
            
            actual_test = ts.slice_intersect(median_forecast)
            pred_test = median_forecast.slice_intersect(ts)
            
            metrics = {}
            metrics['MAPE'] = float(mape(actual_test, pred_test))
            metrics['MAE'] = float(mae(actual_test, pred_test))
            metrics['RMSE'] = float(rmse(actual_test, pred_test))
            metrics['sMAPE'] = float(smape(actual_test, pred_test))
            
            actual_values = actual_test.values().flatten()
            pred_values = pred_test.values().flatten()
            metrics['Bias'] = float(np.mean(pred_values - actual_values))
            
            ss_res = np.sum((actual_values - pred_values) ** 2)
            ss_tot = np.sum((actual_values - np.mean(actual_values)) ** 2)
            metrics['R2'] = float(1 - (ss_res / ss_tot)) if ss_tot != 0 else 0.0
            
            return metrics
            
        except Exception as e:
            self._log_info(f"Erro ao calcular métricas da empresa: {e}")
            return {'MAPE': 0.0, 'MAE': 0.0, 'RMSE': 0.0, 'sMAPE': 0.0, 'Bias': 0.0, 'R2': 0.0}
    
    def consolidate_results(self, database: str, vendas_result: Dict[str, Any], vendas_empresa_result: Dict[str, Any]) -> Dict[str, Any]:
        """Consolida todos os resultados em uma estrutura unificada."""
        self._log_info("Consolidando resultados...")
        
        consolidated = {
            'database': database,
            'timestamp': datetime.now().isoformat(),
            'vendas': vendas_result,
            'vendas_empresa': vendas_empresa_result,
            'summary': {
                'vendas_success': vendas_result.get('success', False),
                'vendas_empresa_success': vendas_empresa_result.get('success', False),
                'total_companies': vendas_empresa_result.get('summary_metrics', {}).get('companies_processed', 0)
            }
        }
        
        # Calcular qualidade geral
        consolidated['summary']['overall_quality'] = 'N/A'
        
        if vendas_result.get('success', False) and vendas_empresa_result.get('success', False):
            vendas_mape = vendas_result.get('metrics', {}).get(30, {}).get('MAPE', 100)
            empresa_mape = vendas_empresa_result.get('summary_metrics', {}).get('average_mape', 100)
            avg_mape = (vendas_mape + empresa_mape) / 2
            
            if avg_mape <= 5:
                consolidated['summary']['overall_quality'] = '🟢 EXCELENTE'
            elif avg_mape <= 15:
                consolidated['summary']['overall_quality'] = '🟡 BOA'
            elif avg_mape <= 30:
                consolidated['summary']['overall_quality'] = '🟠 REGULAR'
            else:
                consolidated['summary']['overall_quality'] = '🔴 BAIXA'
        elif vendas_result.get('success', False):
            # Apenas vendas funcionou
            vendas_mape = vendas_result.get('metrics', {}).get(30, {}).get('MAPE', 100)
            if vendas_mape <= 5:
                consolidated['summary']['overall_quality'] = '🟢 EXCELENTE (apenas vendas)'
            elif vendas_mape <= 15:
                consolidated['summary']['overall_quality'] = '🟡 BOA (apenas vendas)'
            elif vendas_mape <= 30:
                consolidated['summary']['overall_quality'] = '🟠 REGULAR (apenas vendas)'
            else:
                consolidated['summary']['overall_quality'] = '🔴 BAIXA (apenas vendas)'
        elif vendas_empresa_result.get('success', False):
            # Apenas vendas por empresa funcionou
            empresa_mape = vendas_empresa_result.get('summary_metrics', {}).get('average_mape', 100)
            if empresa_mape <= 5:
                consolidated['summary']['overall_quality'] = '🟢 EXCELENTE (apenas empresas)'
            elif empresa_mape <= 15:
                consolidated['summary']['overall_quality'] = '🟡 BOA (apenas empresas)'
            elif empresa_mape <= 30:
                consolidated['summary']['overall_quality'] = '🟠 REGULAR (apenas empresas)'
            else:
                consolidated['summary']['overall_quality'] = '🔴 BAIXA (apenas empresas)'
        
        return consolidated
    
    def prepare_ftp_files(self, consolidated_results: Dict[str, Any]) -> Dict[str, BytesIO]:
        """Prepara arquivos para upload FTP em memória."""
        self._log_info("Preparando arquivos para FTP...")
        
        files = {}
        database = consolidated_results['database']
        
        try:
            # 1. Arquivo de vendas históricas
            if consolidated_results['vendas'].get('success', False):
                hist_df = consolidated_results['vendas']['historical']
                hist_df.columns = ['data', 'vendas_historicas']
                hist_csv = hist_df.to_csv(index=False, sep=';', encoding='utf-8')
                files[f'{database}/vendas/vendas_historicas.csv'] = BytesIO(hist_csv.encode('utf-8'))
            
            # 2. Arquivos de previsões de vendas por horizonte
            if consolidated_results['vendas'].get('success', False):
                for horizon, forecast_df in consolidated_results['vendas']['forecasts'].items():
                    pred_csv = forecast_df.to_csv(index=False, sep=';', encoding='utf-8')
                    files[f'{database}/vendas/previsoes_vendas_{horizon}.csv'] = BytesIO(pred_csv.encode('utf-8'))
                
                # 3. Arquivo consolidado de vendas
                if consolidated_results['vendas'].get('consolidated') is not None:
                    cons_csv = consolidated_results['vendas']['consolidated'].to_csv(index=False, sep=';', encoding='utf-8')
                    files[f'{database}/vendas/vendas_consolidadas.csv'] = BytesIO(cons_csv.encode('utf-8'))
            
            # 4. Arquivos de vendas por empresa
            if consolidated_results['vendas_empresa'].get('success', False):
                # Previsões consolidadas
                empresa_df = consolidated_results['vendas_empresa']['consolidated']
                empresa_csv = empresa_df.to_csv(index=False, sep=';', encoding='utf-8')
                files[f'{database}/vendas_empresa/previsao_vendas_empresa.csv'] = BytesIO(empresa_csv.encode('utf-8'))
                
                # Métricas por empresa
                metrics_data = []
                for empresa, metrics in consolidated_results['vendas_empresa']['metrics_by_company'].items():
                    for metric_name, metric_value in metrics.items():
                        metrics_data.append({
                            'empresa': empresa,
                            'metrica': metric_name,
                            'valor': metric_value
                        })
                
                if metrics_data:
                    metrics_df = pd.DataFrame(metrics_data)
                    metrics_csv = metrics_df.to_csv(index=False, sep=';', encoding='utf-8')
                    files[f'{database}/vendas_empresa/metricas_vendas_empresa.csv'] = BytesIO(metrics_csv.encode('utf-8'))
            
            self._log_info(f"✅ {len(files)} arquivos preparados para FTP")
            return files
            
        except Exception as e:
            self._log_info(f"❌ Erro ao preparar arquivos FTP: {e}")
            return {}
    
    def upload_to_ftp(self, files: Dict[str, BytesIO]) -> bool:
        """Faz upload dos arquivos para FTP."""
        if not files:
            self._log_info("Nenhum arquivo para upload")
            return False
        
        self._log_info("Fazendo upload para FTP...")
        
        try:
            with ForecastFTPUploader() as ftp:
                success_count = 0
                total_count = len(files)
                
                for file_path, file_content in files.items():
                    try:
                        # Resetar posição do buffer
                        file_content.seek(0)
                        
                        # Fazer upload
                        result = ftp.upload_file(file_path, file_content)
                        
                        if result:
                            success_count += 1
                            self._log_info(f"✅ Uploaded: {file_path}")
                        else:
                            self._log_info(f"❌ Failed: {file_path}")
                            
                    except Exception as e:
                        self._log_info(f"❌ Error uploading {file_path}: {e}")
                
                success_rate = success_count / total_count * 100
                self._log_info(f"📤 Upload concluído: {success_count}/{total_count} ({success_rate:.1f}%)")
                
                return success_count > 0
                
        except Exception as e:
            self._log_info(f"❌ Erro no upload FTP: {e}")
            return False
    
    def run_complete_pipeline(self, database: str) -> Dict[str, Any]:
        """Executa pipeline completo em memória."""
        self._log_info(f"🚀 Iniciando pipeline completo para {database}")
        self.metadata['start_time'] = time.perf_counter()
        
        try:
            # 1. Carregar todos os dados
            data = self.load_all_data(database)
            
            # 2. Processar forecast de vendas
            vendas_result = self.process_vendas_forecast(data)
            
            # 3. Processar forecast de vendas por empresa
            vendas_empresa_result = self.process_vendas_empresa_forecast(data)
            
            # 4. Consolidar resultados
            consolidated_results = self.consolidate_results(database, vendas_result, vendas_empresa_result)
            
            # 5. Preparar arquivos para FTP
            ftp_files = self.prepare_ftp_files(consolidated_results)
            
            # 6. Upload para FTP
            upload_success = self.upload_to_ftp(ftp_files)
            
            # 7. Finalizar
            self.metadata['end_time'] = time.perf_counter()
            self.metadata['databases_processed'].append(database)
            self.metadata['total_companies'] = consolidated_results['summary']['total_companies']
            self.metadata['overall_quality'] = consolidated_results['summary']['overall_quality']
            
            elapsed = self.metadata['end_time'] - self.metadata['start_time']
            
            result = {
                'success': upload_success,
                'database': database,
                'results': consolidated_results,
                'files_uploaded': len(ftp_files),
                'elapsed_seconds': elapsed,
                'metadata': self.metadata
            }
            
            self._log_info(f"✅ Pipeline concluído em {elapsed:.2f}s")
            return result
            
        except Exception as e:
            self._log_info(f"❌ Erro no pipeline: {e}")
            return {
                'success': False,
                'database': database,
                'error': str(e),
                'elapsed_seconds': time.perf_counter() - self.metadata['start_time']
            }
    
    def run_all_databases(self) -> Dict[str, Any]:
        """Executa pipeline para todos os databases configurados."""
        databases = self.config.get('databases', [])
        
        if not databases:
            self._log_info("❌ Nenhum database configurado")
            return {'success': False, 'error': 'No databases configured'}
        
        self._log_info(f"🚀 Processando {len(databases)} databases")
        
        all_results = []
        successful_count = 0
        
        for i, database in enumerate(databases, 1):
            self._log_info(f"\n{'='*60}")
            self._log_info(f"💰 Processando {i}/{len(databases)}: {database}")
            self._log_info(f"{'='*60}")
            
            result = self.run_complete_pipeline(database)
            all_results.append(result)
            
            if result['success']:
                successful_count += 1
                self._log_info(f"✅ {database} processado com sucesso")
            else:
                self._log_info(f"❌ {database} falhou: {result.get('error', 'Unknown error')}")
        
        # Resumo final
        self._log_info(f"\n🏆 RESUMO FINAL")
        self._log_info(f"✅ Sucessos: {successful_count}/{len(databases)}")
        self._log_info(f"📊 Empresas processadas: {self.metadata['total_companies']}")
        self._log_info(f"🎯 Qualidade geral: {self.metadata['overall_quality']}")
        
        return {
            'success': successful_count > 0,
            'successful_count': successful_count,
            'total_count': len(databases),
            'results': all_results,
            'metadata': self.metadata
        }
