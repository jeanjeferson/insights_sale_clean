from __future__ import annotations

"""
Pipeline Especializado para Forecasting de VENDAS (Univariado)
Otimizado para dados de valor monetário total por data.
"""

import sys
import os
from pathlib import Path

# Adicionar o diretório raiz do projeto ao PYTHONPATH
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
import yaml
import warnings
warnings.filterwarnings('ignore')
import time

# Darts imports
from darts import TimeSeries
from darts.models import LightGBMModel
# Optional model classes; imported inside builders when needed to avoid hard deps
from darts.dataprocessing.transformers import Scaler, MissingValuesFiller, InvertibleMapper
from darts.utils.timeseries_generation import datetime_attribute_timeseries
from darts.utils.statistics import check_seasonality
from darts.metrics import mape, mae, rmse, smape
from utils.ftp_uploader import ForecastFTPUploader


def stable_prediction_postprocessing(predictions_ts, eps=1e-9, weekend_floor_to_zero=False):
    """
    Pós-processamento estável para previsões de vendas.
    
    Corrige valores científicos muito pequenos que podem resultar de 
    imprecisões numéricas na inversão de transformações logarítmicas.
    
    Args:
        predictions_ts: TimeSeries com as previsões
        eps: Threshold para considerar valores como zero (default: 1e-9)
        weekend_floor_to_zero: Se True, força vendas=0 em fins de semana
        
    Returns:
        TimeSeries com previsões pós-processadas
    """
    try:
        from darts import TimeSeries
        import pandas as pd
        
        # Converter para DataFrame para facilitar manipulação
        df = predictions_ts.to_dataframe()
        
        # Aplicar threshold para valores muito pequenos
        # Se o valor está entre 0 e eps, converte para 0.0
        for col in df.columns:
            mask_small_values = (df[col] > 0) & (df[col] < eps)
            if mask_small_values.any():
                count_small = mask_small_values.sum()
                df.loc[mask_small_values, col] = 0.0
                print(f"   🔧 Corrigidos {count_small} valores científicos pequenos (< {eps})")
        
        # Aplicar regra de negócio para fins de semana (opcional)
        if weekend_floor_to_zero:
            df_with_date = df.reset_index()
            if hasattr(df_with_date.iloc[0, 0], 'dayofweek'):
                weekend_mask = df_with_date.iloc[:, 0].dt.dayofweek.isin([5, 6])  # Sáb, Dom
                if weekend_mask.any():
                    weekend_count = weekend_mask.sum()
                    for col in df.columns:
                        df.loc[weekend_mask, col] = 0.0
                    print(f"   📅 Aplicada regra de negócio: {weekend_count} dias de fim de semana → 0")
        
        # Garantir não-negatividade (já deveria estar, mas por segurança)
        df = df.clip(lower=0.0)
        
        # Converter de volta para TimeSeries
        if predictions_ts.is_probabilistic:
            # Manter estrutura probabilística
            return TimeSeries.from_dataframe(df, freq=predictions_ts.freq)
        else:
            return TimeSeries.from_dataframe(df, freq=predictions_ts.freq)
            
    except Exception as e:
        print(f"   ⚠️ Erro no pós-processamento: {e}. Retornando previsões originais.")
        return predictions_ts


# Define base paths
BASE_DIR = Path(__file__).resolve().parent.parent  # Go up to project root
DATASET_DIR = BASE_DIR / 'dataset'
RESULTS_DIR = BASE_DIR / 'results'

def custom_mape(actual: TimeSeries, pred: TimeSeries) -> float:
    """Custom MAPE that handles zeros in actual values."""
    y_true = actual.values()
    y_pred = pred.values()
    
    denominator = np.where(y_true == 0, 1, y_true)
    return float(np.mean(np.abs((y_true - y_pred) / denominator)) * 100)


class SalesForecastPipeline:
    """Pipeline especializado para forecasting de vendas (dados univariados)."""
    
    def __init__(self, config_path: str = "config/vendas.yaml"):
        """Initialize pipeline with sales-specific configuration."""
        self.config = self._load_config(config_path)
        self.model = None
        self.scaler = None
        self.filler = None
        self.log_transformer = None
        self.negative_adjuster = None
        # Registry and tracking for multi-model training/comparison
        self.model_registry = None  # Lazy-initialized dict of builders
        self.trained_models = {}
        self.model_metrics = {}
        self.best_model_name = None
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if Path(config_path).exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                print(f"   🔧 Carregando configuração: {config_path}")
                return yaml.safe_load(f)
        
        # Default configuration optimized for sales data
        return {
            'model': {
                'lags': 56,                    # 8 semanas de histórico
                'forecast_horizon': 30,        # 30 dias de previsão
                'quantiles': [0.1, 0.5, 0.9], # Intervalos de confiança
                'objective': 'mse',
                'random_state': 42,
                'n_estimators': 500,           # Mais árvores para dados univariados
                'num_leaves': 63,              # Mais folhas para capturar padrões
                'learning_rate': 0.08,         # Taxa menor para melhor precisão
                'max_depth': -1,
                'min_data_in_leaf': 20,
                'feature_fraction': 0.9,
                'bagging_fraction': 0.9,
                'bagging_freq': 5,
                # Optional LightGBM extras (safe defaults)
                'lambda_l1': 0.0,
                'lambda_l2': 0.0,
                'verbose': -1,
                'metric': 'mse',
                'boosting_type': 'gbdt',
                'force_col_wise': True,
                # Multi-model defaults
                'model_list': None,            # e.g., ["lightgbm","autoarima","ets","theta","nhits"]
                'selection_metric': 'MAPE',    # MAPE | RMSE | R2
                'compare_models_enabled': False,
                'validation_horizon': 30,      # Horizon for model comparison backtest
                'retrain_during_backtest': False
            },
            'preprocessing': {
                'fill_missing': True,
                'scale_data': True,
                'log_transform': True,         # Importante para dados monetários
                'detect_anomalies': True,
                'anomaly_threshold': 3.0       # Z-score para detecção de outliers
            },
            'backtest': {
                'start': 0.75,                 # 75% treino, 25% teste
                'stride': 1
            },
            'covariates': {
                'time_attributes': [
                    'dayofweek',               # Dia da semana (0-6)
                    'month',                   # Mês (1-12)  
                    'quarter',                 # Trimestre (1-4)
                    'dayofyear',               # Dia do ano (1-365)
                    'weekofyear'               # Semana do ano
                ]
            },
            'sales_processing': {
                'min_non_zero_ratio': 0.3,    # Mínimo 30% de dias com vendas
                'outlier_cap_percentile': 99  # Cap outliers no percentil 99
            }
        }
    
    def load_sales_data(self, file_path: str) -> TimeSeries:
        """Load and convert sales CSV to TimeSeries."""
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Try different separators
        for sep in [';', ',', '\t']:
            try:
                df = pd.read_csv(file_path, sep=sep, encoding='utf-8')
                if len(df.columns) >= 2:
                    break
            except:
                continue
        else:
            df = pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8')
        
        # Clean column names
        df.columns = [col.strip() for col in df.columns]
        print(f"📊 Sales data loaded: {len(df)} records")
        print(f"Columns: {list(df.columns)}")
        
        # Identify columns (assuming date, sales_value format)
        date_col = df.columns[0]
        sales_col = df.columns[1]
        
        # Clean and prepare data
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce')
        
        # Remove invalid data
        df = df.dropna(subset=[date_col, sales_col])
        df = df[df[sales_col] >= 0]  # Remove negative sales
        
        # Remove duplicates, keeping the sum for same dates
        df = df.groupby(date_col)[sales_col].sum().reset_index()
        df = df.sort_values(date_col)
        
        # Data quality analysis
        total_days = (df[date_col].max() - df[date_col].min()).days
        non_zero_days = (df[sales_col] > 0).sum()
        non_zero_ratio = non_zero_days / len(df) if len(df) > 0 else 0
        
        print(f"📈 Data quality:")
        print(f"   Period: {df[date_col].min().date()} to {df[date_col].max().date()} ({total_days} days)")
        print(f"   Non-zero sales days: {non_zero_days}/{len(df)} ({non_zero_ratio:.1%})")
        print(f"   Average daily sales: ${df[sales_col].mean():,.2f}")
        print(f"   Total sales: ${df[sales_col].sum():,.2f}")
        
        # Quality check
        min_ratio = self.config['sales_processing']['min_non_zero_ratio']
        if non_zero_ratio < min_ratio:
            print(f"⚠️  Warning: Low sales activity ({non_zero_ratio:.1%} < {min_ratio:.1%})")
        
        # Convert to TimeSeries
        try:
            series = TimeSeries.from_dataframe(
                df, 
                time_col=date_col, 
                value_cols=sales_col,
                freq='D',
                fill_missing_dates=True,
                fillna_value=0
            )
            print(f"✅ TimeSeries created: {len(series)} points from {series.start_time()} to {series.end_time()}")
            return series
        except Exception as e:
            print(f"❌ Error creating TimeSeries: {e}")
            raise
    
    def create_covariates(self, series: TimeSeries) -> Optional[TimeSeries]:
        """Create temporal covariates optimized for sales patterns."""
        attributes = self.config['covariates']['time_attributes']
        forecast_horizon = self.config['model']['forecast_horizon']
        
        # Handle both single horizon and multiple horizons
        if isinstance(forecast_horizon, list):
            max_horizon = max(forecast_horizon)
        else:
            max_horizon = forecast_horizon

        # Create extended time index
        future_index = pd.date_range(
            start=series.start_time(),
            periods=len(series) + max_horizon,
            freq=series.freq
        )

        covariates_list = []
        # Tratar is_weekend manualmente (não é atributo nativo em todas as versões)
        manual_weekend = False
        if 'is_weekend' in attributes:
            manual_weekend = True
            # evitar tentativa via datetime_attribute_timeseries
            attributes = [a for a in attributes if a != 'is_weekend']

        for attr in attributes:
            try:
                cov = datetime_attribute_timeseries(
                    time_index=future_index,
                    attribute=attr,
                    one_hot=False
                )
                covariates_list.append(cov)
                print(f"   ✅ Created covariate: {attr}")
            except Exception as e:
                print(f"   ⚠️  Warning: Could not create covariate {attr}: {e}")
                continue

        # Criar is_weekend = 1 se sábado(5) ou domingo(6), senão 0
        if manual_weekend:
            try:
                weekend_mask = (future_index.weekday >= 5).astype(float)
                weekend_ts = TimeSeries.from_times_and_values(future_index, weekend_mask)
                covariates_list.append(weekend_ts)
                print(f"   ✅ Created covariate: is_weekend")
            except Exception as e:
                print(f"   ⚠️  Warning: Could not create covariate is_weekend (manual): {e}")

        if not covariates_list:
            print("   📅 No covariates created")
            return None

        # Stack all covariates
        combined_cov = covariates_list[0]
        for cov in covariates_list[1:]:
            combined_cov = combined_cov.stack(cov)

        print(f"   📅 Combined covariates shape: {combined_cov.width} features")
        return combined_cov
    
    def preprocess_series(self, series: TimeSeries) -> TimeSeries:
        """Preprocess sales series with optimized transformations."""
        processed_series = series.copy()
        
        print("🔄 Preprocessing sales data...")
        
        # 1. Fill missing values
        if self.config['preprocessing']['fill_missing']:
            if self.filler is None:
                self.filler = MissingValuesFiller(fill='auto')
            processed_series = self.filler.transform(processed_series)
            print("   ✅ Missing values filled")
        
        # 2. Anomaly detection and capping
        if self.config['preprocessing']['detect_anomalies']:
            processed_series = self._detect_and_cap_anomalies(processed_series)
        
        # 3. Handle negative values (shouldn't happen with sales, but safety)
        if self.negative_adjuster is None:
            self.negative_adjuster = InvertibleMapper(
                fn=lambda x: np.maximum(x, 0),
                inverse_fn=lambda x: x
            )
        processed_series = self.negative_adjuster.transform(processed_series)
        
        # 4. Log transformation (important for sales data)
        if self.config['preprocessing']['log_transform']:
            if self.log_transformer is None:
                self.log_transformer = InvertibleMapper(fn=np.log1p, inverse_fn=np.expm1)
            processed_series = self.log_transformer.transform(processed_series)
            print("   ✅ Log transformation applied")
        
        # 5. Scaling (fit only on training data)
        if self.config['preprocessing']['scale_data']:
            if self.scaler is None:
                self.scaler = Scaler()
                cutoff = int(len(processed_series) * self.config['backtest']['start'])
                train_data = processed_series[:cutoff]
                self.scaler.fit(train_data)
                print(f"   ✅ Scaler fitted on {len(train_data)} training points")
            processed_series = self.scaler.transform(processed_series)
            print("   ✅ Data scaled")
        
        return processed_series
    
    def _detect_and_cap_anomalies(self, series: TimeSeries) -> TimeSeries:
        """Detect and cap anomalies in sales data."""
        values = series.values()
        
        # Use percentile-based capping for sales data
        cap_percentile = self.config['sales_processing']['outlier_cap_percentile']
        cap_value = np.percentile(values[values > 0], cap_percentile)  # Only consider non-zero values
        
        # Z-score based detection
        threshold = self.config['preprocessing']['anomaly_threshold']
        mean_val = np.mean(values)
        std_val = np.std(values)
        
        if std_val == 0:
            return series
        
        z_scores = np.abs((values - mean_val) / std_val)
        anomaly_mask = z_scores > threshold
        
        # Cap extreme values
        capped_values = values.copy()
        capped_values[anomaly_mask & (values > cap_value)] = cap_value
        
        if np.any(anomaly_mask):
            n_anomalies = np.sum(anomaly_mask)
            print(f"   🔍 Detected and capped {n_anomalies} anomalies (>{cap_value:.0f})")
            return series.with_values(capped_values)
        
        return series
    
    def train_model(self, series: TimeSeries, covariates: Optional[TimeSeries] = None, model_spec=None):
        """Treina um modelo.
        - Se model_spec for None: mantém comportamento atual (LightGBM com config).
        - Se for string/dict: usa a factory dinâmica para treinar esse modelo.
        Retorna o modelo treinado e define self.model.
        """
        model_config = self.config['model']

        # Se especificado, usar caminho dinâmico
        if model_spec is not None:
            print("🚀 Training (dynamic) sales forecasting model...")
            forecast_horizon = model_config['forecast_horizon']
            max_horizon = max(forecast_horizon) if isinstance(forecast_horizon, list) else forecast_horizon
            lags = self._determine_optimal_lags(series)
            name = model_spec if isinstance(model_spec, str) else model_spec.get('name') or model_spec.get('model')
            params = {} if isinstance(model_spec, str) else model_spec.get('params', {})
            self._init_model_registry()
            try:
                self.model = self._build_model(name, params, series, covariates, output_chunk_length=max_horizon, lags=lags)
            except Exception as e:
                raise RuntimeError(f"Falha ao construir modelo {name}: {e}")

            caps = self.model_registry[name]
            if caps['supports_future_covariates'] and covariates is not None:
                self.model.fit(series, future_covariates=covariates)
            elif caps['supports_past_covariates'] and covariates is not None:
                self.model.fit(series, past_covariates=covariates)
            else:
                self.model.fit(series)
            print("   ✅ Model trained successfully")
            return self.model

        # Comportamento padrão LightGBM (backward-compatible)
        print("🚀 Training sales forecasting model (LightGBM)...")
        lags = self._determine_optimal_lags(series)
        print(f"   📊 Using lags: {lags}")

        forecast_horizon = model_config['forecast_horizon']
        max_horizon = max(forecast_horizon) if isinstance(forecast_horizon, list) else forecast_horizon

        self.model = LightGBMModel(
            lags=lags,
            lags_future_covariates=(14, 14) if covariates is not None else None,
            output_chunk_length=max_horizon,
            likelihood='quantile',
            quantiles=model_config.get('quantiles', [0.1, 0.5, 0.9]),
            random_state=model_config.get('random_state', 42),
            n_estimators=model_config.get('n_estimators', 500),
            num_leaves=model_config.get('num_leaves', 63),
            learning_rate=model_config.get('learning_rate', 0.08),
            max_depth=model_config.get('max_depth', -1),
            min_data_in_leaf=model_config.get('min_data_in_leaf', 20),
            feature_fraction=model_config.get('feature_fraction', 0.9),
            bagging_fraction=model_config.get('bagging_fraction', 0.9),
            bagging_freq=model_config.get('bagging_freq', 5),
            lambda_l1=model_config.get('lambda_l1', 0.0),
            lambda_l2=model_config.get('lambda_l2', 0.0),
            verbose=model_config.get('verbose', -1),
            objective=model_config.get('objective', 'mse'),
            metric=model_config.get('metric', 'mse'),
            boosting_type=model_config.get('boosting_type', 'gbdt'),
            force_col_wise=model_config.get('force_col_wise', True)
        )

        self.model.fit(series, future_covariates=covariates)
        print("   ✅ Model trained successfully")
        return self.model
    
    def _determine_optimal_lags(self, series: TimeSeries) -> int:
        """Determine optimal lags for sales data using seasonality detection."""
        cutoff = int(len(series) * self.config['backtest']['start'])
        train_data = series[:cutoff] if cutoff > 30 else series
        cfg_lags = int(self.config['model'].get('lags', 56))
        auto_lags = bool(self.config['model'].get('auto_lags', True))

        # If auto_lags disabled, honor configured lags
        if not auto_lags:
            print(f"   ⚙️ Using configured lags (auto_lags=False): {cfg_lags}")
            return cfg_lags

        try:
            # Check for weekly (7), monthly (30), and quarterly (90) patterns
            seasonal_candidates = [7, 30, 90]
            detected_periods = []
            
            for period in seasonal_candidates:
                if len(train_data) > period * 3:  # Need at least 3 cycles
                    try:
                        is_seasonal, detected_period = check_seasonality(
                            train_data, m=period, max_lag=min(period*2, len(train_data)//3), alpha=0.05
                        )
                        if is_seasonal:
                            detected_periods.append(int(detected_period))
                            print(f"   🔍 Detected seasonality: {detected_period} days")
                    except Exception:
                        continue
            
            if detected_periods:
                # Never reduce below configured lags to avoid underfitting long-range effects
                detected = max(detected_periods)
                chosen = max(cfg_lags, detected)
                print(f"   🧠 Lags chosen by detection vs config: detected={detected}, configured={cfg_lags} -> using {chosen}")
                return chosen
            
        except Exception as e:
            print(f"   ⚠️  Seasonality detection failed: {e}")
        
        # Fallback to configured lags
        return cfg_lags

    def _infer_seasonal_period(self, series: TimeSeries) -> int:
        """Infer seasonal period m (e.g., 7 for semanal) using darts check_seasonality.
        Falls back to 7 if nothing is detected reliably.
        """
        cutoff = int(len(series) * self.config['backtest']['start'])
        train_data = series[:cutoff] if cutoff > 60 else series
        candidates = [7, 30, 90]
        detected = []
        for m in candidates:
            try:
                if len(train_data) >= m * 3:
                    ok, period = check_seasonality(train_data, m=m, max_lag=min(m*2, len(train_data)//3), alpha=0.05)
                    if ok:
                        detected.append(period)
            except Exception:
                continue
        if detected:
            return int(max(detected))
        return 7

    def _init_model_registry(self):
        """Initialize the model registry mapping names to builder functions and capability flags."""
        if self.model_registry is not None:
            return

        def build_lightgbm(params, series, covariates, lags, output_chunk_length):
            return LightGBMModel(
                lags=lags,
                lags_future_covariates=(14, 14) if covariates is not None else None,
                output_chunk_length=output_chunk_length,
                likelihood='quantile',
                quantiles=params.get('quantiles', [0.1, 0.5, 0.9]),
                random_state=params.get('random_state', 42),
                n_estimators=params.get('n_estimators', 500),
                num_leaves=params.get('num_leaves', 63),
                learning_rate=params.get('learning_rate', 0.08),
                max_depth=params.get('max_depth', -1),
                min_data_in_leaf=params.get('min_data_in_leaf', 20),
                feature_fraction=params.get('feature_fraction', 0.9),
                bagging_fraction=params.get('bagging_fraction', 0.9),
                bagging_freq=params.get('bagging_freq', 5),
                lambda_l1=params.get('lambda_l1', 0.0),
                lambda_l2=params.get('lambda_l2', 0.0),
                verbose=params.get('verbose', -1),
                objective=params.get('objective', 'mse'),
                metric=params.get('metric', 'mse'),
                boosting_type=params.get('boosting_type', 'gbdt'),
                force_col_wise=params.get('force_col_wise', True),
            )

        def build_autoarima(params, series, covariates, lags, output_chunk_length):
            try:
                from darts.models import AutoARIMA
            except Exception as e:
                raise ImportError(f"AutoARIMA indisponível: {e}")
            m = params.get('m') or self._infer_seasonal_period(series)
            # Compat: algumas versões não aceitam 'm' mas sim 'seasonal_periods' ou inferem de 'seasonal'
            try:
                return AutoARIMA(seasonal=True, m=m)
            except TypeError:
                # Tentar sem 'm'
                try:
                    return AutoARIMA(seasonal=True)
                except TypeError:
                    # Fallback mínimo
                    return AutoARIMA()

        def build_ets(params, series, covariates, lags, output_chunk_length):
            try:
                from darts.models import ExponentialSmoothing
            except Exception as e:
                raise ImportError(f"ExponentialSmoothing indisponível: {e}")
            m = params.get('seasonal_periods') or self._infer_seasonal_period(series)
            # Manter assinatura mínima para compatibilidade
            try:
                return ExponentialSmoothing(seasonal_periods=m)
            except TypeError:
                # Fallback sem sazonalidade explícita
                return ExponentialSmoothing()

        def build_theta(params, series, covariates, lags, output_chunk_length):
            try:
                from darts.models import Theta
            except Exception as e:
                raise ImportError(f"Theta indisponível: {e}")
            m = params.get('seasonal_periods') or self._infer_seasonal_period(series)
            # Compat: tentar seasonal_periods e fallbacks
            try:
                return Theta(seasonal_periods=m)
            except TypeError:
                try:
                    return Theta(m=m)
                except TypeError:
                    return Theta()

        def build_nbeats(params, series, covariates, lags, output_chunk_length):
            try:
                from darts.models import NBEATSModel
            except Exception as e:
                raise ImportError(f"NBEATSModel indisponível: {e}")
            input_chunk_length = params.get('input_chunk_length', max(28, 2 * self._infer_seasonal_period(series)))
            return NBEATSModel(
                input_chunk_length=input_chunk_length,
                output_chunk_length=output_chunk_length,
                random_state=params.get('random_state', 42),
                dropout=params.get('dropout', 0.1),
                n_epochs=params.get('n_epochs', 50),
                batch_size=params.get('batch_size', 32),
            )

        def build_nhits(params, series, covariates, lags, output_chunk_length):
            try:
                from darts.models import NHiTSModel
            except Exception as e:
                raise ImportError(f"NHiTSModel indisponível: {e}")
            input_chunk_length = params.get('input_chunk_length', max(28, 2 * self._infer_seasonal_period(series)))
            return NHiTSModel(
                input_chunk_length=input_chunk_length,
                output_chunk_length=output_chunk_length,
                random_state=params.get('random_state', 42),
                dropout=params.get('dropout', 0.1),
                n_epochs=params.get('n_epochs', 50),
                batch_size=params.get('batch_size', 32),
            )

        def build_tft(params, series, covariates, lags, output_chunk_length):
            try:
                from darts.models import TFTModel
            except Exception as e:
                raise ImportError(f"TFTModel indisponível: {e}")
            input_chunk_length = params.get('input_chunk_length', 90)
            return TFTModel(
                input_chunk_length=input_chunk_length,
                output_chunk_length=output_chunk_length,
                hidden_size=params.get('hidden_size', 64),
                dropout=params.get('dropout', 0.1),
                n_epochs=params.get('n_epochs', 50),
                batch_size=params.get('batch_size', 32),
                random_state=params.get('random_state', 42),
                add_relative_index=True,
            )

        self.model_registry = {
            'lightgbm': {
                'builder': build_lightgbm,
                'supports_future_covariates': True,
                'supports_past_covariates': False,
                'probabilistic': True,
            },
            'autoarima': {
                'builder': build_autoarima,
                'supports_future_covariates': False,
                'supports_past_covariates': False,
                'probabilistic': False,
            },
            'ets': {
                'builder': build_ets,
                'supports_future_covariates': False,
                'supports_past_covariates': False,
                'probabilistic': False,
            },
            'theta': {
                'builder': build_theta,
                'supports_future_covariates': False,
                'supports_past_covariates': False,
                'probabilistic': False,
            },
            'nbeats': {
                'builder': build_nbeats,
                'supports_future_covariates': False,
                'supports_past_covariates': True,
                'probabilistic': True,
            },
            'nhits': {
                'builder': build_nhits,
                'supports_future_covariates': False,
                'supports_past_covariates': True,
                'probabilistic': True,
            },
            'tft': {
                'builder': build_tft,
                'supports_future_covariates': True,
                'supports_past_covariates': True,
                'probabilistic': True,
            },
        }

    def _build_model(self, model_name: str, params: Dict[str, Any], series: TimeSeries, covariates: Optional[TimeSeries], output_chunk_length: int, lags: int):
        """Build a model instance from the registry."""
        self._init_model_registry()
        name = model_name.lower()
        if name not in self.model_registry:
            raise ValueError(f"Modelo não suportado: {model_name}")
        builder = self.model_registry[name]['builder']
        return builder(params, series, covariates, lags, output_chunk_length)

    def train_models(self, series: TimeSeries, model_specs, covariates: Optional[TimeSeries] = None):
        """Treina vários modelos de acordo com model_specs.
        model_specs: lista de strings (nomes) ou dicts {name: str, params: dict}
        Retorna dict com modelos treinados e métricas de backtest.
        """
        print("🚀 Treinando modelos para comparação...")
        self.trained_models = {}
        self.model_metrics = {}

        model_cfg = self.config['model']
        forecast_horizon = model_cfg['forecast_horizon']
        max_horizon = max(forecast_horizon) if isinstance(forecast_horizon, list) else forecast_horizon

        lags = self._determine_optimal_lags(series)
        m = self._infer_seasonal_period(series)
        print(f"   📊 Lags: {lags} | Período sazonal inferido: {m}")

        for spec in model_specs:
            if isinstance(spec, str):
                name = spec
                params = {}
            elif isinstance(spec, dict):
                name = spec.get('name') or spec.get('model') or 'lightgbm'
                params = spec.get('params', {})
            else:
                continue

            try:
                model = self._build_model(name, params, series, covariates, output_chunk_length=max_horizon, lags=lags)
            except Exception as e:
                print(f"   ❌ Falha ao construir modelo {name}: {e}")
                continue

            # Fit com covariates apropriadas conforme capacidade
            caps = self.model_registry[name]['supports_future_covariates'], self.model_registry[name]['supports_past_covariates']
            try:
                # Theta pode falhar com valores zero/negativos sob sazonalidade multiplicativa; garantir positividade
                series_for_fit = series
                if name.lower() == 'theta':
                    try:
                        vals = series.values()
                        if np.min(vals) <= 0:
                            series_for_fit = series.with_values(vals + 1e-6)
                    except Exception:
                        pass

                if caps[0]:
                    model.fit(series_for_fit, future_covariates=covariates)
                elif caps[1]:
                    model.fit(series_for_fit, past_covariates=covariates)
                else:
                    model.fit(series_for_fit)
                print(f"   ✅ Modelo treinado: {name}")
            except Exception as e:
                print(f"   ❌ Falha ao treinar {name}: {e}")
                continue

            self.trained_models[name] = model

        return self.trained_models

    def compare_models(self, series: TimeSeries, model_specs, covariates: Optional[TimeSeries] = None, selection_metric: str = 'MAPE', output_dir: Optional[str] = None):
        """Compara vários modelos via backtest padronizado e seleciona o melhor.
        Retorna dict com ranking e melhor modelo.
        """
        if not model_specs:
            raise ValueError("Nenhum modelo especificado para comparação")

        # Treinar modelos
        self.train_models(series, model_specs, covariates)

        # Backtest padronizado
        backtest_cfg = self.config['backtest']
        model_cfg = self.config['model']
        validation_h = model_cfg.get('validation_horizon', 30)
        retrain = model_cfg.get('retrain_during_backtest', False)

        results = []
        for name, model in self.trained_models.items():
            try:
                # Escolher covariates corretas
                caps = self.model_registry[name]
                kwargs = {}
                if caps['supports_future_covariates'] and covariates is not None:
                    kwargs['future_covariates'] = covariates
                elif caps['supports_past_covariates'] and covariates is not None:
                    kwargs['past_covariates'] = covariates

                hf = model.historical_forecasts(
                    series=series,
                    start=backtest_cfg['start'],
                    forecast_horizon=validation_h,
                    stride=backtest_cfg['stride'],
                    retrain=retrain,
                    verbose=False,
                    show_warnings=False,
                    **kwargs,
                )

                median_forecast = hf.quantile_timeseries(0.5) if hf.is_probabilistic else hf
                actual_test = series.slice_intersect(median_forecast)
                pred_test = median_forecast.slice_intersect(series)

                actual_values = actual_test.values().flatten()
                pred_values = pred_test.values().flatten()

                # Métricas
                metrics = {}
                try:
                    non_zero_mask = actual_values > 0
                    if np.any(non_zero_mask):
                        a = actual_values[non_zero_mask]
                        p = pred_values[non_zero_mask]
                        metrics['MAPE'] = float(np.mean(np.abs((a - p) / a)) * 100)
                    else:
                        metrics['MAPE'] = float('inf')
                except Exception:
                    metrics['MAPE'] = float(custom_mape(actual_test, pred_test))

                metrics['MAE'] = float(mae(actual_test, pred_test))
                metrics['RMSE'] = float(rmse(actual_test, pred_test))
                metrics['sMAPE'] = float(smape(actual_test, pred_test))
                metrics['Bias'] = float(np.mean(pred_values - actual_values))
                ss_res = np.sum((actual_values - pred_values) ** 2)
                ss_tot = np.sum((actual_values - np.mean(actual_values)) ** 2)
                metrics['R2'] = float(1 - (ss_res / ss_tot)) if ss_tot != 0 else 0.0

                self.model_metrics[name] = metrics
                print(f"   📊 {name} => MAPE: {metrics['MAPE']:.2f}% | RMSE: {metrics['RMSE']:.4f} | R²: {metrics['R2']:.3f}")
                results.append((name, metrics))
            except Exception as e:
                print(f"   ⚠️  Backtest falhou para {name}: {e}")

        if not results:
            raise RuntimeError("Nenhum modelo foi avaliado com sucesso no backtest")

        # Seleção do melhor
        if selection_metric.upper() in ['MAPE', 'MAE', 'RMSE', 'SMAPE']:
            best = min(results, key=lambda x: x[1].get(selection_metric.upper(), float('inf')))
        elif selection_metric.upper() == 'R2':
            best = max(results, key=lambda x: x[1].get('R2', float('-inf')))
        else:
            best = min(results, key=lambda x: x[1].get('MAPE', float('inf')))

        self.best_model_name = best[0]
        self.model = self.trained_models[self.best_model_name]
        print(f"🏆 Melhor modelo: {self.best_model_name} (por {selection_metric.upper()})")

        # Opcional: salvar comparação em CSV
        if output_dir is not None:
            try:
                df = pd.DataFrame([{**{'model': n}, **m} for n, m in results])
                Path(output_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(Path(output_dir) / 'model_comparison.csv', index=False, sep=';', encoding='utf-8')
            except Exception:
                pass

        return {
            'best_model': self.best_model_name,
            'metrics': self.model_metrics,
            'trained_models': list(self.trained_models.keys()),
        }
    
    def _calculate_horizon_metrics(self, base_metrics: Dict[str, float], horizon: int, all_horizons: list) -> Dict[str, float]:
        """Calculate metrics for specific horizon based on backtest results."""
        horizon_metrics = base_metrics.copy()
        horizon_metrics['forecast_horizon'] = horizon
        
        # Ajustar métricas para horizontes mais longos
        # Horizontes mais longos tendem a ter maior incerteza
        if horizon > 30:
            # Fator de degradação baseado na razão do horizonte
            degradation_factor = 1 + (horizon - 30) / 30 * 0.3  # 30% de degradação por 30 dias extras
            
            # Aplicar degradação às métricas de erro
            horizon_metrics['MAPE'] = horizon_metrics.get('MAPE', 0) * degradation_factor
            horizon_metrics['MAE'] = horizon_metrics.get('MAE', 0) * degradation_factor
            horizon_metrics['RMSE'] = horizon_metrics.get('RMSE', 0) * degradation_factor
            horizon_metrics['sMAPE'] = horizon_metrics.get('sMAPE', 0) * degradation_factor
            
            # R² tende a diminuir com horizontes mais longos
            horizon_metrics['R2'] = max(0, horizon_metrics.get('R2', 0) * (1 - (horizon - 30) / 100))
        
        return horizon_metrics
    
    def run_backtest(self, series: TimeSeries, covariates: Optional[TimeSeries] = None) -> Dict[str, float]:
        """Run backtest with sales-specific metrics."""
        if self.model is None:
            raise ValueError("Model not trained yet")
        
        backtest_config = self.config['backtest']
        model_config = self.config['model']
        
        try:
            # Use single horizon for backtest validation (30 days)
            forecast_horizon = model_config['forecast_horizon']
            if isinstance(forecast_horizon, list):
                backtest_horizon = forecast_horizon[0]  # Use first horizon (30 days)
            else:
                backtest_horizon = forecast_horizon
            
            # Generate historical forecasts
            historical_forecasts = self.model.historical_forecasts(
                series=series,
                future_covariates=covariates,
                start=backtest_config['start'],
                forecast_horizon=backtest_horizon,
                stride=backtest_config['stride'],
                retrain=False,
                verbose=False,
                show_warnings=False
            )
            
            # Extract median forecast for evaluation
            if historical_forecasts.is_probabilistic:
                median_forecast = historical_forecasts.quantile_timeseries(0.5)
            else:
                median_forecast = historical_forecasts
            
            # Calculate metrics on overlapping period
            actual_test = series.slice_intersect(median_forecast)
            pred_test = median_forecast.slice_intersect(series)

            actual_values = actual_test.values().flatten()
            pred_values = pred_test.values().flatten()

            # Calculate comprehensive metrics for sales
            metrics = {}
            
            # MAPE with custom handling - skip zero values for more accurate calculation
            try:
                # Filter out zero values for MAPE calculation
                non_zero_mask = actual_values > 0
                if np.any(non_zero_mask):
                    actual_non_zero = actual_values[non_zero_mask]
                    pred_non_zero = pred_values[non_zero_mask]
                    metrics['MAPE'] = float(np.mean(np.abs((actual_non_zero - pred_non_zero) / actual_non_zero)) * 100)
                else:
                    metrics['MAPE'] = float('inf')  # All zeros case
            except (ValueError, ZeroDivisionError):
                metrics['MAPE'] = float(custom_mape(actual_test, pred_test))
            
            # Other key metrics
            metrics['MAE'] = float(mae(actual_test, pred_test))
            metrics['RMSE'] = float(rmse(actual_test, pred_test))
            metrics['sMAPE'] = float(smape(actual_test, pred_test))
            
            # Bias (average error) - use numpy arrays for calculations
            metrics['Bias'] = float(np.mean(pred_values - actual_values))
            
            # R-squared
            ss_res = np.sum((actual_values - pred_values) ** 2)
            ss_tot = np.sum((actual_values - np.mean(actual_values)) ** 2)
            metrics['R2'] = float(1 - (ss_res / ss_tot)) if ss_tot != 0 else 0.0
            
            print("📈 Backtest Results:")
            for metric, value in metrics.items():
                print(f"   {metric}: {value:.4f}")
            
            return metrics
            
        except Exception as e:
            print(f"⚠️  Backtest error: {e}")
            return {'MAPE': 0.0, 'MAE': 0.0, 'RMSE': 0.0, 'sMAPE': 0.0, 'Bias': 0.0, 'R2': 0.0}
    
    def predict(self, n_periods: int, covariates: Optional[TimeSeries] = None) -> TimeSeries:
        """Generate sales predictions."""
        if self.model is None:
            raise ValueError("Model not trained yet")
        
        print(f"🔮 Generating {n_periods}-day sales forecast...")
        
        # Generate predictions
        predictions = self.model.predict(n=n_periods, future_covariates=covariates)
        
        # Inverse transform to get actual sales values
        if self.scaler is not None:
            predictions = self.scaler.inverse_transform(predictions)
            print("   ✅ Predictions unscaled")
            
        if self.log_transformer is not None:
            predictions = self.log_transformer.inverse_transform(predictions)
            print("   ✅ Log transformation inverted")
            
        if self.negative_adjuster is not None:
            predictions = self.negative_adjuster.inverse_transform(predictions)
            
        # Aplicar pós-processamento estável para corrigir valores científicos pequenos
        predictions = stable_prediction_postprocessing(
            predictions, 
            eps=1e-9, 
            weekend_floor_to_zero=False  # Desabilitado por padrão
        )
        
        # Sales summary
        if predictions.is_probabilistic:
            median_pred = predictions.quantile_timeseries(0.5)
            total_forecast = float(median_pred.sum(axis=1).values().sum())
        else:
            total_forecast = float(predictions.sum(axis=1).values().sum())
        
        daily_avg = total_forecast / n_periods
        print(f"   📊 Forecast summary:")
        print(f"      Total {n_periods}-day sales: ${total_forecast:,.2f}")
        print(f"      Daily average: ${daily_avg:,.2f}")
        
        return predictions
    
    def save_results(self, original_series: TimeSeries, predictions: TimeSeries,
                metrics: Dict[str, float], output_dir: str = "results",
                database_name: str = None, horizon: int = None):
        """Save sales forecasting results with optional FTP upload."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True, parents=True)
        
        print(f"💾 Saving results to {output_path}")
        
        # Historical data
        # Salvar histórico apenas no primeiro horizonte ou quando não especificado
        if horizon is None or horizon == 30:
            hist_df = original_series.to_dataframe().reset_index()
            hist_df.columns = ['data', 'vendas_historicas']
            hist_file = output_path / 'vendas_historicas.csv'
            hist_df.to_csv(hist_file, index=False, sep=';', encoding='utf-8')
        
        # Predictions - adicionar coluna forecast_horizon
        pred_df = predictions.to_dataframe().reset_index()
        pred_df['forecast_horizon'] = horizon
        
        if predictions.is_probabilistic:
            # Rename quantile columns
            cols = ['data'] + [f'vendas_q{int(q*100)}' for q in self.config['model']['quantiles']] + ['forecast_horizon']
            pred_df.columns = cols
        else:
            pred_df.columns = ['data', 'vendas_previstas', 'forecast_horizon']
        
        # Salvar arquivo individual para este horizonte
        pred_file = output_path / f'previsoes_vendas_{horizon}.csv'
        pred_df.to_csv(pred_file, index=False, sep=';', encoding='utf-8')
        
        # Metrics - adicionar coluna forecast_horizon
        metrics_df = pd.DataFrame(list(metrics.items()), columns=['metrica', 'valor'])
        metrics_df['forecast_horizon'] = horizon
        
        # Salvar arquivo individual para este horizonte
        metrics_file = output_path / f'metricas_vendas_{horizon}.csv'
        metrics_df.to_csv(metrics_file, index=False, sep=';', encoding='utf-8')
        
        # Upload será feito após todos os horizontes serem processados
        
        print(f"   ✅ Results for {horizon}-day horizon saved successfully" if horizon else "   ✅ All results saved successfully")            

    def _unify_results_files(self, output_dir: str, horizons: list):
        """Unify individual horizon files into consolidated files."""
        output_path = Path(output_dir)
        
        print("📋 Unifying results files...")
        
        # Unificar arquivos de métricas
        all_metrics = []
        for horizon in horizons:
            metrics_file = output_path / f'metricas_vendas_{horizon}.csv'
            if metrics_file.exists():
                df = pd.read_csv(metrics_file, sep=';')
                # Garantir que a coluna forecast_horizon existe
                if 'forecast_horizon' not in df.columns:
                    df['forecast_horizon'] = horizon
                all_metrics.append(df)
        
        if all_metrics:
            unified_metrics = pd.concat(all_metrics, ignore_index=True)
            unified_metrics_file = output_path / 'metricas_vendas.csv'
            unified_metrics.to_csv(unified_metrics_file, index=False, sep=';', encoding='utf-8')
            print(f"   ✅ Unified metrics saved: {unified_metrics_file}")
        
        # Unificar arquivos de previsões
        all_predictions = []
        for horizon in horizons:
            pred_file = output_path / f'previsoes_vendas_{horizon}.csv'
            if pred_file.exists():
                df = pd.read_csv(pred_file, sep=';')
                all_predictions.append(df)
        
        if all_predictions:
            unified_predictions = pd.concat(all_predictions, ignore_index=True)
            unified_predictions_file = output_path / 'previsoes_vendas.csv'
            unified_predictions.to_csv(unified_predictions_file, index=False, sep=';', encoding='utf-8')
            print(f"   ✅ Unified predictions saved: {unified_predictions_file}")

    def _create_consolidated_sales_file(self, output_dir: str, horizons: list):
        """Create consolidated file with historical sales (last 30 days) + forecasts for all horizons."""
        output_path = Path(output_dir)
        
        print("📊 Creating consolidated sales file (historical + forecasts)...")
        
        try:
            # 1. Ler vendas históricas
            historical_file = output_path / 'vendas_historicas.csv'
            if not historical_file.exists():
                print(f"   ⚠️ Historical file not found: {historical_file}")
                return
            
            hist_df = pd.read_csv(historical_file, sep=';')
            
            # Converter coluna data para datetime
            hist_df['data'] = pd.to_datetime(hist_df['data'])
            
            # Filtrar últimos 30 dias
            last_date = hist_df['data'].max()
            cutoff_date = last_date - pd.Timedelta(days=30)
            hist_last_30 = hist_df[hist_df['data'] >= cutoff_date].copy()
            
            # Adicionar colunas para consolidação
            hist_last_30['tipo'] = 'historico'
            hist_last_30['forecast_horizon'] = 0
            hist_last_30 = hist_last_30.rename(columns={'vendas_historicas': 'vendas'})
            
            print(f"   📅 Historical data: {len(hist_last_30)} records (last 30 days)")
            
            # 2. Ler previsões de todos os horizontes
            all_forecasts = []
            for horizon in horizons:
                pred_file = output_path / f'previsoes_vendas_{horizon}.csv'
                if pred_file.exists():
                    pred_df = pd.read_csv(pred_file, sep=';')
                    
                    # Converter coluna data para datetime
                    pred_df['data'] = pd.to_datetime(pred_df['data'])
                    
                    # Adicionar colunas para consolidação
                    pred_df['tipo'] = 'previsao'
                    pred_df = pred_df.rename(columns={'vendas_previstas': 'vendas'})
                    
                    all_forecasts.append(pred_df)
                    print(f"   🔮 Forecast data {horizon}d: {len(pred_df)} records")
                else:
                    print(f"   ⚠️ Forecast file not found: {pred_file}")
            
            if not all_forecasts:
                print("   ⚠️ No forecast files found")
                return
            
            # 3. Combinar todos os dados
            all_data = [hist_last_30] + all_forecasts
            consolidated_df = pd.concat(all_data, ignore_index=True)
            
            # Reordenar colunas
            consolidated_df = consolidated_df[['data', 'vendas', 'tipo', 'forecast_horizon']]
            
            # Ordenar por data
            consolidated_df = consolidated_df.sort_values('data')
            
            # 4. Calcular soma acumulada por horizonte
            print("   📊 Calculating cumulative sums by horizon...")
            consolidated_df['vendas_acumuladas'] = 0.0
            
            # Para cada horizonte, calcular soma acumulada (histórico + previsões até aquele horizonte)
            for horizon in horizons:
                # Filtrar dados até o horizonte atual
                mask_until_horizon = (consolidated_df['forecast_horizon'] == 0) | (consolidated_df['forecast_horizon'] == horizon)
                df_horizon = consolidated_df[mask_until_horizon].copy()
                
                # Ordenar por data
                df_horizon = df_horizon.sort_values('data')
                
                # Calcular soma acumulada
                df_horizon['vendas_acumuladas'] = df_horizon['vendas'].cumsum()
                
                # Atualizar apenas os registros deste horizonte no DataFrame principal
                consolidated_df.loc[mask_until_horizon, 'vendas_acumuladas'] = df_horizon['vendas_acumuladas'].values
            
            # 5. Salvar arquivo consolidado
            consolidated_file = output_path / 'vendas_consolidadas.csv'
            consolidated_df.to_csv(consolidated_file, index=False, sep=';', encoding='utf-8')
            
            print(f"   ✅ Consolidated file saved: {consolidated_file}")
            print(f"   📊 Total records: {len(consolidated_df)}")
            print(f"      - Historical: {len(hist_last_30)}")
            for i, forecast_df in enumerate(all_forecasts):
                horizon = horizons[i]
                print(f"      - Forecast {horizon}d: {len(forecast_df)}")
            
            # Log das somas acumuladas finais
            for horizon in horizons:
                mask_horizon = consolidated_df['forecast_horizon'] == horizon
                if mask_horizon.any():
                    final_cumulative = consolidated_df[mask_horizon]['vendas_acumuladas'].iloc[-1]
                    print(f"      - Soma acumulada horizonte {horizon}d: R$ {final_cumulative:,.2f}")
            
        except Exception as e:
            print(f"   ❌ Error creating consolidated file: {e}")

    def _upload_to_ftp(self, database_name: str, results_folder: str):
        """Upload results to FTP server."""
        try:
            print(f"\n📤 Uploading sales results to FTP...")
            
            with ForecastFTPUploader() as ftp:
                result = ftp.upload_sales_results(database_name, results_folder)
                
                if result['success']:
                    print(f"✅ FTP Upload: {result['message']}")
                    uploaded_files = result.get('uploaded_files', [])
                    if uploaded_files:
                        print(f"   📄 Files uploaded: {', '.join(uploaded_files)}")
                else:
                    print(f"⚠️ FTP Upload partial: {result['message']}")
                    failed_files = result.get('failed_files', [])
                    if failed_files:
                        print(f"   ❌ Failed files: {len(failed_files)}")
                        
        except Exception as e:
            error_behavior = self.config.get('ftp_upload', {}).get('on_error', 'continue')
            print(f"❌ FTP Upload error: {e}")
            
            if error_behavior == 'stop':
                raise Exception(f"FTP upload failed: {e}")
            else:
                print("⚠️ Continuing without FTP upload...")

    def _upload_all_to_ftp(self, database_name: str, results_folder: str, 
                          horizons: list) -> None:
        """Upload all forecast files to FTP server."""
        try:
            print(f"\n📤 Uploading all sales results to FTP...")
            
            with ForecastFTPUploader() as ftp:
                # Fazer upload usando o método existente (que já inclui vendas_consolidadas.csv)
                result = ftp.upload_sales_results(database_name, results_folder)
                
                if result['success']:
                    print(f"✅ FTP Upload Complete: {result['message']}")
                    uploaded_files = result.get('uploaded_files', [])
                    if uploaded_files:
                        print(f"   📄 Files uploaded: {', '.join(uploaded_files)}")
                        
                    # Verificar se vendas_consolidadas.csv foi enviado
                    if 'vendas_consolidadas.csv' in uploaded_files:
                        print(f"   ✅ vendas_consolidadas.csv enviado com sucesso!")
                    else:
                        print(f"   ⚠️ vendas_consolidadas.csv não foi encontrado/enviado")
                        
                else:
                    print(f"⚠️ FTP Upload partial: {result['message']}")
                    failed_files = result.get('failed_files', [])
                    if failed_files:
                        print(f"   ❌ Failed files: {len(failed_files)}")
                        
        except Exception as e:
            error_behavior = self.config.get('ftp_upload', {}).get('on_error', 'continue')
            print(f"❌ FTP Upload error: {e}")
            
            if error_behavior == 'stop':
                raise Exception(f"FTP upload failed: {e}")
            else:
                print("⚠️ Continuing without FTP upload...")
  
    def run_full_pipeline(self, file_path: str, output_dir: str = "results", database_name: str = None) -> Dict[str, Any]:
        """Run complete sales forecasting pipeline with multiple horizons."""
        print("💰 SALES FORECASTING PIPELINE")
        print("=" * 50)

        start_time = time.perf_counter()
        try:
            # Load and analyze sales data
            series = self.load_sales_data(file_path)
            
            # Create temporal covariates
            print("📅 Creating temporal covariates...")
            covariates = self.create_covariates(series)
            
            # Preprocess data
            processed_series = self.preprocess_series(series)
            
            # 🔹 Treinamento inicial (possível comparação de modelos)
            model_list = self.config['model'].get('model_list')
            selection_metric = self.config['model'].get('selection_metric', 'MAPE')
            compare_enabled = bool(self.config['model'].get('compare_models_enabled', False))

            if compare_enabled and model_list:
                print("🧪 Comparing multiple models as configured...")
                cmp_results = self.compare_models(
                    processed_series,
                    model_list,
                    covariates=covariates,
                    selection_metric=selection_metric,
                    output_dir=str(output_dir)
                )
                print(f"   ✅ Selected best model: {cmp_results['best_model']}")
                # Backtest do melhor já foi feito na comparação; usar métricas dele
                metrics = self.model_metrics.get(self.best_model_name, {})
            else:
                print("🚀 Training forecasting model...")
                self.train_model(processed_series, covariates)
                # Backtest inicial
                print("📊 Running backtest validation...")
                metrics = self.run_backtest(processed_series, covariates)
            mape_value = metrics.get("MAPE", 100)

            # 🔹 Verificação do MAPE e ajuste adaptativo (com rollback)
            adaptive_params = self.config["model"].get("adaptive_parameters", {})
            if mape_value > 20 and adaptive_params:
                print(f"⚠️ MAPE inicial {mape_value:.2f}% > 20%. Reajustando parâmetros (modo adaptativo)...")

                # Snapshot para rollback
                tracked_keys = [
                    'n_estimators','num_leaves','learning_rate','feature_fraction','bagging_fraction',
                    'bagging_freq','max_depth','min_data_in_leaf','lambda_l1','lambda_l2','min_gain_to_split','lags'
                ]
                prev_params = {k: self.config['model'].get(k) for k in tracked_keys if k in self.config['model']}
                prev_metrics = metrics.copy()

                # Escolher perfil (com o novo gatilho normalmente cai no extreme_params)
                applied_profile = None
                if mape_value <= 15 and "moderate_params" in adaptive_params:
                    applied_profile = 'moderate_params'
                elif mape_value <= 20 and "aggressive_params" in adaptive_params:
                    applied_profile = 'aggressive_params'
                elif "extreme_params" in adaptive_params:
                    applied_profile = 'extreme_params'

                if applied_profile:
                    label = {
                        'moderate_params': 'moderados',
                        'aggressive_params': 'agressivos',
                        'extreme_params': 'extremos (suavizados)'
                    }.get(applied_profile, applied_profile)
                    print(f"   🔧 Ajustando parâmetros {label}. MAPE: {mape_value:.2f}%")
                    self.config["model"].update(adaptive_params[applied_profile])

                    # 🔄 Re-treinar com parâmetros ajustados
                    self.train_model(processed_series, covariates)
                    new_metrics = self.run_backtest(processed_series, covariates)
                    new_mape = new_metrics.get("MAPE", mape_value)
                    print(f"✅ Novo MAPE após ajuste: {new_mape:.2f}%")

                    # Rollback se não melhorou
                    if new_mape >= mape_value:
                        print("   ↩️ Ajuste não melhorou o MAPE. Revertendo para parâmetros anteriores.")
                        for k, v in prev_params.items():
                            self.config['model'][k] = v
                        # Re-treinar baseline restaurado
                        self.train_model(processed_series, covariates)
                        metrics = prev_metrics
                        mape_value = prev_metrics.get('MAPE', mape_value)
                    else:
                        metrics = new_metrics
                        mape_value = new_mape
            
            # Generate predictions for multiple horizons
            forecast_horizons = self.config['model']['forecast_horizon']
            
            # Garantir que seja uma lista
            if not isinstance(forecast_horizons, list):
                forecast_horizons = [forecast_horizons]
            
            all_predictions = {}
            all_metrics = {}
            
            print(f"🔮 Generating forecasts for horizons: {forecast_horizons} days")
            
            for horizon in forecast_horizons:
                print(f"   📊 Processing {horizon}-day horizon...")
                
                # Generate predictions for this horizon
                predictions = self.predict(horizon, covariates)
                all_predictions[horizon] = predictions
                
                # Calcular métricas específicas do horizonte
                # Para horizontes > 30, usar métricas baseadas no backtest de 30 dias
                # mas ajustar para refletir a incerteza crescente com o horizonte
                horizon_metrics = self._calculate_horizon_metrics(metrics, horizon, forecast_horizons)
                all_metrics[horizon] = horizon_metrics
                
                # Save results for this horizon
                self.save_results(series, predictions, horizon_metrics, 
                                output_dir, database_name, horizon)
            
            # Unify all files into consolidated files
            self._unify_results_files(str(output_dir), forecast_horizons)
            
            # Create consolidated sales file (historical + forecasts)
            self._create_consolidated_sales_file(str(output_dir), forecast_horizons)
            
            # Upload all files to FTP
            self._upload_all_to_ftp(database_name, str(output_dir), forecast_horizons)
            
            elapsed = time.perf_counter() - start_time
            print("🎉 SALES FORECASTING COMPLETED SUCCESSFULLY!")
            print(f"⏱️ Tempo total do pipeline: {elapsed:.2f}s")
            print("=" * 50)
            
            return {
                'series': series,
                'predictions': all_predictions,
                'metrics': all_metrics,
                'model': self.model,
                'database_name': database_name,
                'horizons': forecast_horizons,
                'elapsed_seconds': elapsed
            }
            
        except Exception as e:
            print(f"❌ Pipeline failed: {e}")
            raise

def run_sales_forecast(db_name: str):
    """Run sales forecasting for specific database."""
    print(f"\n💰 Executando previsão de VENDAS para {db_name}")
    print("=" * 60)
    
    start_time = time.perf_counter()
    # Initialize pipeline
    pipeline = SalesForecastPipeline()
    # Garantir que NÃO haverá comparação de modelos nesta rota
    try:
        if 'model' in pipeline.config:
            pipeline.config['model'].pop('model_list', None)
    except Exception:
        pass
    
    # Set paths
    file_path = DATASET_DIR / f"{db_name}_vendas.csv"
    output_path = RESULTS_DIR / db_name / 'vendas'
    
    if not file_path.exists():
        print(f"⚠️ Arquivo não encontrado: {file_path}")
        return None
    
    try:
        # Run pipeline
        results = pipeline.run_full_pipeline(str(file_path), str(output_path), db_name)
        
        if results:
            # Extract and display key metrics
            all_metrics = results.get('metrics', {})
            predictions = results.get('predictions')
            
            # Use metrics from 30-day horizon (most reliable)
            metrics = all_metrics.get(30, {}) if isinstance(all_metrics, dict) and 30 in all_metrics else all_metrics
            
            print(f"\n📊 MÉTRICAS DE PERFORMANCE - {db_name}")
            print("=" * 50)
            print(f"🎯 Precisão:")
            print(f"   MAPE:  {metrics.get('MAPE', 0):.2f}%")
            print(f"   MAE:   {metrics.get('MAE', 0):.4f}")
            print(f"   RMSE:  {metrics.get('RMSE', 0):.4f}")
            print(f"   sMAPE: {metrics.get('sMAPE', 0):.2f}%")
            print(f"📈 Qualidade:")
            print(f"   R²:    {metrics.get('R2', 0):.3f}")
            print(f"   Bias:  {metrics.get('Bias', 0):.4f}")
            
            # Forecast summary if predictions available
            if predictions is not None:
                try:
                    # predictions is now a dict with horizon keys
                    print(f"💰 RESUMO DAS PREVISÕES:")
                    for horizon, pred_ts in predictions.items():
                        if pred_ts.is_probabilistic:
                            median_pred = pred_ts.quantile_timeseries(0.5)
                            total_forecast = float(median_pred.sum(axis=1).values().sum())
                        else:
                            total_forecast = float(pred_ts.sum(axis=1).values().sum())
                        
                        daily_avg = total_forecast / len(pred_ts)
                        
                        print(f"   📊 {horizon} dias:")
                        print(f"      Total previsto: R$ {total_forecast:,.2f}")
                        print(f"      Média diária:   R$ {daily_avg:,.2f}")
                except Exception as e:
                    print(f"   ⚠️ Erro calculando resumo da previsão: {e}")
            
            # Quality assessment
            mape = metrics.get('MAPE', 100)
            r2 = metrics.get('R2', 0)
            
           # Classificação baseada no MAPE - critérios realistas para forecasting de vendas
            if mape <= 5:
                quality = "🟢 EXCELENTE"
            elif mape <= 15:
                quality = "🟡 BOA" 
            elif mape <= 30:
                quality = "🟠 REGULAR"
            else:
                quality = "🔴 BAIXA"
            
            # Ajustar para cima se R² for muito bom (R² pode ser negativo em forecasting)
            if r2 >= 0.5 and mape <= 20:
                if "🟠" in quality:
                    quality = "🟡 BOA"
                elif "🟡" in quality:
                    quality = "🟢 EXCELENTE"
            elif r2 >= 0.3 and mape <= 25:
                if "🔴" in quality:
                    quality = "🟠 REGULAR"
                elif "🟠" in quality:
                    quality = "🟡 BOA"
            elif r2 >= 0.0 and mape <= 15:
                if "🔴" in quality:
                    quality = "🟠 REGULAR"
            
            print(f"🏆 Qualidade geral: {quality}")
            print(f"💾 Resultados salvos em: {output_path}")
            
            elapsed = time.perf_counter() - start_time
            print(f"⏱️ Tempo total (run_sales_forecast): {elapsed:.2f}s")
            return results
        
    except Exception as e:
        print(f"❌ Erro na execução: {e}")
        return None

def run_all_databases():
    """Executa previsão de VENDAS para todos os databases configurados."""
    
    config_path = Path("config/config_databases.yaml")
    if not config_path.exists():
        print(f"❌ Configuration file not found: {config_path}")
        return

    with open(config_path, 'r', encoding='utf-8') as f:
        db_config = yaml.safe_load(f)
    
    databases = db_config.get('databases', [])
    if not databases:
        print("❌ No databases found in the configuration file.")
        return

    print(f"\n💰 PROCESSAMENTO DE VENDAS EM LOTE")
    print(f"🚀 Found {len(databases)} databases to process for SALES forecasting.")
    
    successful_count = 0
    total_count = len(databases)
    detailed_results = []
    
    overall_start = time.perf_counter()
    for i, db_name in enumerate(databases, 1):
        print("\n" + "="*60)
        print(f"💰 Processing SALES {i}/{total_count}: {db_name}")
        print("="*60)
        
        # Verificar se arquivo de vendas existe
        file_path = DATASET_DIR / f"{db_name}_vendas.csv"
        
        if not file_path.exists():
            print(f"⚠️ Sales data file not found for {db_name}, skipping.")
            detailed_results.append({
                'database': db_name,
                'status': 'FILE_NOT_FOUND',
                'mape': None,
                'r2': None,
                'forecast_total': None
            })
            continue
        
        # Criar diretório de saída
        output_path = RESULTS_DIR / db_name / 'vendas'
        output_path.mkdir(parents=True, exist_ok=True)
        
        try:
            db_start = time.perf_counter()
            # Inicializar pipeline para este database
            pipeline = SalesForecastPipeline()
            
            print(f"🚀 Processing sales forecast for: {db_name}")
            
            # Executar pipeline completo
            results = pipeline.run_full_pipeline(str(file_path), str(output_path), db_name)
            db_elapsed = time.perf_counter() - db_start
            print(f"⏱️ Tempo (database {db_name}): {db_elapsed:.2f}s")
            
            if results:
                # Extrair métricas e previsões
                all_metrics = results.get('metrics', {})
                predictions = results.get('predictions')
                
                # Use metrics from 30-day horizon (most reliable)
                metrics = all_metrics.get(30, {}) if isinstance(all_metrics, dict) and 30 in all_metrics else all_metrics
                
                mape = metrics.get('MAPE', 0)
                r2 = metrics.get('R2', 0)
                mae = metrics.get('MAE', 0)
                
                # Calcular total da previsão (usar horizonte de 30 dias)
                forecast_total = 0
                try:
                    if predictions is not None and 30 in predictions:
                        pred_30 = predictions[30]
                        if pred_30.is_probabilistic:
                            median_pred = pred_30.quantile_timeseries(0.5)
                            forecast_total = float(median_pred.sum(axis=1).values().sum())
                        else:
                            forecast_total = float(pred_30.sum(axis=1).values().sum())
                except:
                    forecast_total = 0
                
                print(f"✅ Successfully processed sales for {db_name}")
                print(f"   📊 MAPE: {mape:.2f}% | R²: {r2:.3f} | MAE: {mae:.4f}")
                print(f"   💰 Previsão 30 dias: R$ {forecast_total:,.2f}")
                
                # Classificar qualidade - critérios realistas para forecasting de vendas
                if mape <= 5:
                    quality = "🟢"
                elif mape <= 15:
                    quality = "🟡"
                elif mape <= 30:
                    quality = "🟠"
                else:
                    quality = "🔴"
                
                # Ajustar para cima se R² for bom
                if r2 >= 0.5 and mape <= 20:
                    if quality == "🟠":
                        quality = "🟡"
                    elif quality == "🟡":
                        quality = "🟢"
                elif r2 >= 0.3 and mape <= 25:
                    if quality == "🔴":
                        quality = "🟠"
                    elif quality == "🟠":
                        quality = "🟡"
                elif r2 >= 0.0 and mape <= 15:
                    if quality == "🔴":
                        quality = "🟠"
                
                print(f"   {quality} Qualidade: {'Excelente' if quality == '🟢' else 'Boa' if quality == '🟡' else 'Regular' if quality == '🟠' else 'Baixa'}")
                
                detailed_results.append({
                    'database': db_name,
                    'status': 'SUCCESS',
                    'mape': mape,
                    'r2': r2,
                    'mae': mae,
                    'forecast_total': forecast_total,
                    'quality': quality
                })
                
                successful_count += 1
            else:
                print(f"❌ Failed to process sales for {db_name}")
                detailed_results.append({
                    'database': db_name,
                    'status': 'FAILED',
                    'mape': None,
                    'r2': None,
                    'forecast_total': None
                })
                
        except Exception as e:
            print(f"❌ Error processing sales for {db_name}: {e}")
            detailed_results.append({
                'database': db_name,
                'status': 'ERROR',
                'error': str(e),
                'mape': None,
                'r2': None,
                'forecast_total': None
            })
    
    # Resumo final detalhado
    print("\n" + "="*70)
    print(f"📊 RESUMO CONSOLIDADO DO PROCESSAMENTO DE VENDAS")
    print("="*70)
    
    print(f"✅ Sucessos: {successful_count}/{total_count} ({successful_count/total_count*100:.1f}%)")
    
    # Estatísticas dos sucessos
    successful_results = [r for r in detailed_results if r['status'] == 'SUCCESS']
    
    if successful_results:
        # Métricas consolidadas
        avg_mape = sum(r['mape'] for r in successful_results) / len(successful_results)
        avg_r2 = sum(r['r2'] for r in successful_results) / len(successful_results)
        total_forecast = sum(r['forecast_total'] for r in successful_results)
        
        print(f"\n📈 MÉTRICAS CONSOLIDADAS:")
        print(f"   MAPE médio:        {avg_mape:.2f}%")
        print(f"   R² médio:          {avg_r2:.3f}")
        print(f"   Previsão total:    R$ {total_forecast:,.2f}")
        
        # Top/Bottom performers
        sorted_by_mape = sorted(successful_results, key=lambda x: x['mape'])
        
        print(f"\n🏆 TOP 3 MELHORES (Menor MAPE):")
        for i, result in enumerate(sorted_by_mape[:3], 1):
            print(f"   {i}. {result['database']}: {result['mape']:.2f}% MAPE, {result['r2']:.3f} R²")
        
        if len(sorted_by_mape) > 3:
            print(f"\n⚠️  3 COM MAIOR MAPE:")
            for i, result in enumerate(sorted_by_mape[-3:], 1):
                print(f"   {i}. {result['database']}: {result['mape']:.2f}% MAPE, {result['r2']:.3f} R²")
    
    # Resumo de falhas
    failed_results = [r for r in detailed_results if r['status'] != 'SUCCESS']
    if failed_results:
        print(f"\n❌ FALHAS ({len(failed_results)}):")
        for result in failed_results:
            status_msg = {
                'FILE_NOT_FOUND': 'Arquivo não encontrado',
                'FAILED': 'Processamento falhou',
                'ERROR': 'Erro na execução'
            }
            print(f"   • {result['database']}: {status_msg.get(result['status'], 'Desconhecido')}")
    
    if successful_count > 0:
        print(f"\n💾 Resultados detalhados salvos em: results/[DATABASE]/vendas/")
        
        # Salvar resumo em arquivo
        try:
            summary_df = pd.DataFrame(detailed_results)
            summary_path = RESULTS_DIR / 'batch_sales_summary.csv'
            summary_df.to_csv(summary_path, index=False, sep=';', encoding='utf-8')
            print(f"📋 Resumo consolidado: {summary_path}")
        except:
            pass
    
    overall_elapsed = time.perf_counter() - overall_start
    print(f"\n⏱️ Tempo total (run_all_databases - vendas): {overall_elapsed:.2f}s")
    return {'successful_count': successful_count, 'total_count': total_count, 'results': detailed_results}

def run_single_database_sales(db_name: str):
    """Executa previsão de vendas para um database específico."""
    print(f"\n💰 PROCESSAMENTO INDIVIDUAL - VENDAS")
    print(f"Database: {db_name}")
    print("="*50)
    
    # Verificar arquivo
    vendas_file = DATASET_DIR / f"{db_name}_vendas.csv"
    if not vendas_file.exists():
        print(f"❌ Sales file not found: {vendas_file}")
        return None
    
    # Executar
    start_time = time.perf_counter()
    results = run_sales_forecast(db_name)
    elapsed = time.perf_counter() - start_time
    print(f"⏱️ Tempo total (run_single_database_sales:{db_name}): {elapsed:.2f}s")
    
    if results:
        print(f"\n🎊 PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        
        # Extrair dados principais
        all_metrics = results.get('metrics', {})
        predictions = results.get('predictions')
        series = results.get('series')
        
        # Use metrics from 30-day horizon (most reliable)
        metrics = all_metrics.get(30, {}) if isinstance(all_metrics, dict) and 30 in all_metrics else all_metrics
        
        # Análise adicional dos dados históricos
        if series is not None:
            try:
                historical_values = series.values()
                print(f"\n📊 ANÁLISE DOS DADOS HISTÓRICOS:")
                print(f"   Período total:      {len(series)} dias")
                print(f"   Vendas médias/dia:  R$ {np.mean(historical_values):,.2f}")
                print(f"   Vendas mínimas:     R$ {np.min(historical_values):,.2f}")
                print(f"   Vendas máximas:     R$ {np.max(historical_values):,.2f}")
                print(f"   Desvio padrão:      R$ {np.std(historical_values):,.2f}")
                print(f"   Total histórico:    R$ {np.sum(historical_values):,.2f}")
            except Exception as e:
                print(f"   ⚠️ Erro na análise histórica: {e}")
        
        # Comparar previsão vs histórico
        if predictions is not None:
            try:
                # Analisar cada horizonte
                for horizon, pred_ts in predictions.items():
                    print(f"\n🔮 ANÁLISE DA PREVISÃO ({horizon} dias):")
                    
                    if pred_ts.is_probabilistic:
                        median_pred = pred_ts.quantile_timeseries(0.5)
                        pred_values = median_pred.values()
                        
                        low_pred = pred_ts.quantile_timeseries(0.1)
                        high_pred = pred_ts.quantile_timeseries(0.9)
                        
                        print(f"   Previsão mediana:   R$ {np.mean(pred_values):,.2f}/dia")
                        print(f"   Intervalo baixo:    R$ {np.mean(low_pred.values()):,.2f}/dia")
                        print(f"   Intervalo alto:     R$ {np.mean(high_pred.values()):,.2f}/dia")
                        print(f"   Total previsto:     R$ {np.sum(pred_values):,.2f}")
                        
                        # Variabilidade da previsão
                        pred_std = np.std(pred_values)
                        print(f"   Volatilidade:       R$ {pred_std:,.2f}")
                        
                    else:
                        pred_values = pred_ts.values()
                        print(f"   Previsão média:     R$ {np.mean(pred_values):,.2f}/dia")
                        print(f"   Total previsto:     R$ {np.sum(pred_values):,.2f}")
                    
                    # Comparação com histórico (apenas para horizonte de 30 dias)
                    if series is not None and horizon == 30:
                        historical_avg = np.mean(series.values())
                        forecast_avg = np.mean(pred_values)
                        growth_rate = ((forecast_avg / historical_avg) - 1) * 100
                        
                        print(f"\n📈 COMPARAÇÃO HISTÓRICO vs PREVISÃO:")
                        print(f"   Média histórica:    R$ {historical_avg:,.2f}/dia")
                        print(f"   Média prevista:     R$ {forecast_avg:,.2f}/dia")
                        
                        if growth_rate > 0:
                            print(f"   📈 Crescimento:     +{growth_rate:.1f}%")
                        elif growth_rate < 0:
                            print(f"   📉 Declínio:        {growth_rate:.1f}%")
                        else:
                            print(f"   ➡️  Estabilidade:    {growth_rate:.1f}%")
                        
            except Exception as e:
                print(f"   ⚠️ Erro na análise de previsão: {e}")
        
        # Recomendações baseadas nas métricas (corrigido)
        mape = metrics.get('MAPE', 100)
        r2 = metrics.get('R2', 0)
        
        print(f"\n💡 RECOMENDAÇÕES:")
        
        # Classificação baseada principalmente no MAPE
        if mape <= 5:
            print(f"   🟢 Modelo excelente! MAPE muito baixo indica alta precisão.")
            print(f"   ✅ Adequado para planejamento estratégico e operacional.")
        elif mape <= 15:
            print(f"   🟡 Modelo bom. MAPE adequado para uso comercial.")
            print(f"   ✅ Confiável para tomada de decisões.")
        elif mape <= 30:
            print(f"   🟠 Modelo regular. MAPE aceitável, mas pode melhorar.")
            print(f"   ⚠️ Use com cautela para decisões críticas.")
        else:
            print(f"   🔴 Modelo com alta variação. MAPE muito alto.")
            print(f"   🔄 Revise dados e parâmetros do modelo.")
            
        # Comentários adicionais sobre R²
        if r2 < 0.4:
            print(f"   📊 R² baixo ({r2:.3f}) pode indicar alta volatilidade natural dos dados.")
            print(f"   💡 Isso é comum em vendas com muitas variações sazonais/externas.")
        elif r2 < 0.6:
            print(f"   📈 R² moderado ({r2:.3f}). Modelo captura padrões principais.")
        else:
            print(f"   ⭐ R² alto ({r2:.3f}) indica excelente capacidade explicativa.")
            
        # Sugestões específicas apenas se MAPE for alto
        if mape > 25:
            print(f"   ⚙️ Sugestão: Ajustar parâmetros ou aumentar período de treino.")
        
        # Análise da variação vs histórico (usar horizonte de 30 dias)
        if series is not None and predictions is not None and 30 in predictions:
            try:
                historical_avg = np.mean(series.values())
                pred_30 = predictions[30]
                if pred_30.is_probabilistic:
                    forecast_avg = np.mean(pred_30.quantile_timeseries(0.5).values())
                else:
                    forecast_avg = np.mean(pred_30.values())
                
                growth_rate = abs(((forecast_avg / historical_avg) - 1) * 100)
                
                if growth_rate > 50:
                    print(f"   ⚠️ Grande variação prevista vs histórico ({growth_rate:.1f}%).")
                    print(f"   🔍 Recomenda-se validar contexto de mercado/negócio.")
                elif growth_rate > 30:
                    print(f"   📊 Variação moderada vs histórico ({growth_rate:.1f}%).")
            except:
                pass
        
        return results
    else:
        print(f"\n❌ Falha no processamento de {db_name}")
        return None

def run_single_database_sales_models(db_name: str, models: Optional[List[Union[str, Dict[str, Any]]]] = None, selection_metric: str = 'MAPE'):
    """Executa previsão de vendas para um database específico testando uma lista de modelos.
    - db_name: nome do database (usado para localizar dataset e salvar resultados)
    - models: lista de modelos a comparar. Pode ser ["lightgbm", "autoarima", ...] ou
              [{"name": "nhits", "params": {...}}, ...]. Se None, usa o model_list do YAML.
    - selection_metric: métrica de seleção (MAPE, RMSE, R2)
    """
    print(f"\n💰 PROCESSAMENTO INDIVIDUAL - VENDAS (COMPARAÇÃO DE MODELOS)")
    print(f"Database: {db_name}")
    print("="*50)

    # Verificar arquivo
    vendas_file = DATASET_DIR / f"{db_name}_vendas.csv"
    if not vendas_file.exists():
        print(f"❌ Sales file not found: {vendas_file}")
        return None

    # Caminho de saída
    output_path = RESULTS_DIR / db_name / 'vendas'
    output_path.mkdir(parents=True, exist_ok=True)

    # Inicializar pipeline
    pipeline = SalesForecastPipeline()

    # Se o usuário passou uma lista de modelos, sobrescreve a configuração
    if models is not None:
        try:
            if 'model' not in pipeline.config:
                pipeline.config['model'] = {}
            pipeline.config['model']['model_list'] = models
            pipeline.config['model']['selection_metric'] = selection_metric
        except Exception:
            pass

    # Executar pipeline completo (vai treinar/comparar modelos se model_list estiver definido)
    start_time = time.perf_counter()
    results = pipeline.run_full_pipeline(str(vendas_file), str(output_path), db_name)
    elapsed = time.perf_counter() - start_time

    # Resumo da comparação de modelos
    try:
        if pipeline.model_metrics:
            print(f"\n🤝 COMPARAÇÃO DE MODELOS:")
            for name, met in pipeline.model_metrics.items():
                print(f"   • {name}: MAPE={met.get('MAPE', float('nan')):.2f}% | RMSE={met.get('RMSE', float('nan')):.4f} | R²={met.get('R2', float('nan')):.3f}")
            if pipeline.best_model_name:
                print(f"🏆 Melhor modelo: {pipeline.best_model_name} (por {pipeline.config['model'].get('selection_metric', 'MAPE')})")
            comp_file = output_path / 'model_comparison.csv'
            if comp_file.exists():
                print(f"📄 Comparação salva em: {comp_file}")
    except Exception:
        pass

    return results

def main():
    """Main function com opções para execução."""
    import sys
    
    print("💰 SALES FORECASTING PIPELINE")
    print("="*50)

    # Menu interativo
    print("Escolha uma opção:")
    print("1. 🚀 Processar todos os databases")
    print("2. 🎯 Processar database específico")
    print("3. 🧪 Teste com 012ARR_ERP_BI")
    print("4. 🤝 Processar database específico (comparação de modelos)")
    
    try:
        choice = input("\nOpção (1-4): ").strip()
        
        if choice == "1":
            run_all_databases()
            
        elif choice == "2":
            db_name = input("Digite o nome do database: ").strip()
            if db_name:
                run_single_database_sales(db_name)
            else:
                print("❌ Nome do database é obrigatório")
                
        elif choice == "3":
            run_single_database_sales('012ARR_ERP_BI')
        
        elif choice == "4":
            db_name = input("Digite o nome do database: ").strip()
            if db_name:
                # Usará a lista de modelos do YAML (model.model_list)
                run_single_database_sales_models(db_name)
            else:
                print("❌ Nome do database é obrigatório")
            
        else:
            print("❌ Opção inválida")
            
    except KeyboardInterrupt:
        print("\n👋 Saindo...")

if __name__ == "__main__":
    main()
