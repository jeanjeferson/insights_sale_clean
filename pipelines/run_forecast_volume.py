"""
Pipeline Especializado para Forecasting de VOLUME CONSOLIDADO (Univariado)
Otimizado para dados de volume/quantidade total por data.
Baseado no pipeline de vendas mas adaptado para características específicas de volume.
"""

from __future__ import annotations

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
from typing import Optional, Dict, Any
import yaml
import warnings
warnings.filterwarnings('ignore')

# Darts imports
from darts import TimeSeries
from darts.models import LightGBMModel
from darts.dataprocessing.transformers import Scaler, MissingValuesFiller, InvertibleMapper
from darts.utils.timeseries_generation import datetime_attribute_timeseries
from darts.utils.statistics import check_seasonality
from darts.metrics import mape, mae, rmse, smape
from utils.ftp_uploader import ForecastFTPUploader

# Define base paths
BASE_DIR = Path(__file__).resolve().parent.parent  # Diretório raiz do projeto
DATASET_DIR = BASE_DIR / 'dataset'
RESULTS_DIR = BASE_DIR / 'results'

def custom_mape(actual: TimeSeries, pred: TimeSeries) -> float:
    """Custom MAPE que lida com zeros nos valores reais (adaptado para volume)."""
    y_true = actual.values()
    y_pred = pred.values()
    
    # Para volume, valores zero são mais comuns, então usar uma abordagem mais robusta
    denominator = np.where(np.abs(y_true) < 1e-6, 1, y_true)
    return float(np.mean(np.abs((y_true - y_pred) / denominator)) * 100)


class VolumeForecastPipeline:
    """Pipeline especializado para forecasting de volume consolidado (dados univariados)."""
    
    def __init__(self, config_path: str = "config/volume.yaml"):
        """Initialize pipeline com configuração específica para volume."""
        self.config = self._load_config(config_path)
        self.model = None
        self.scaler = None
        self.filler = None
        self.log_transformer = None
        self.negative_adjuster = None
        
        # Create results directory
        RESULTS_DIR.mkdir(exist_ok=True)
        
    
    def _load_config(self, config_path: str) -> dict:
        """Carrega configuração do arquivo YAML."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"[WARNING] Config file {config_path} não encontrado, usando configuração padrão")
            return self._get_default_config()
    
    def _get_default_config(self) -> dict:
        """Configuração padrão otimizada para volume."""
        return {
            'model': {
                'lags': 56,
                'forecast_horizon': 30,
                'quantiles': [0.1, 0.5, 0.9],
                'n_estimators': 600,
                'num_leaves': 63,
                'learning_rate': 0.06,
                'random_state': 42
            },
            'preprocessing': {
                'fill_missing': True,
                'scale_data': True,
                'log_transform': True,
                'detect_anomalies': True,
                'anomaly_threshold': 2.5
            },
            'backtest': {
                'start': 0.75,
                'stride': 1
            },
            'volume_processing': {
                'min_non_zero_ratio': 0.2,
                'outlier_cap_percentile': 95,
                'zero_padding': True
            }
        }
    
    def load_volume_data(self, file_path: str) -> Optional[TimeSeries]:
        """
        Carrega dados de volume consolidado de um arquivo CSV.
        Espera colunas: data, Quantidade
        """
        try:
            df = pd.read_csv(file_path, sep=';')
            
            # Verificar colunas necessárias
            if 'data' not in df.columns:
                print(f"❌ Coluna 'data' não encontrada em {file_path}")
                return None
                
            if 'Quantidade' not in df.columns:
                print(f"❌ Coluna 'Quantidade' não encontrada em {file_path}")
                return None
            
            # Converter data
            df['data'] = pd.to_datetime(df['data'])
            
            # Converter quantidade para numérico
            df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce')
            
            # Remover valores nulos
            df = df.dropna()
            
            # Ordenar por data
            df = df.sort_values('data')
            
            # Aplicar processamento específico de volume
            df = self._preprocess_volume_data(df)
            
            if len(df) == 0:
                print(f"[WARNING] Nenhum dado válido após pré-processamento em {file_path}")
                return None
            
            # Criar TimeSeries
            series = TimeSeries.from_dataframe(
                df, 
                time_col='data', 
                value_cols='Quantidade',
                freq='D',
                fill_missing_dates=True
            )
            
            print(f"[SUCCESS] Volume data carregado: {len(series)} registros de {series.start_time()} a {series.end_time()}")
            
            return series
            
        except Exception as e:
            print(f"❌ Erro ao carregar dados de volume: {e}")
            return None
    
    def _preprocess_volume_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pré-processamento específico para dados de volume."""
        original_len = len(df)
        
        # Configurações de volume
        volume_config = self.config.get('volume_processing', {})
        min_non_zero_ratio = volume_config.get('min_non_zero_ratio', 0.2)
        outlier_percentile = volume_config.get('outlier_cap_percentile', 95)
        zero_padding = volume_config.get('zero_padding', True)
        
        # Verificar proporção de dias com volume > 0
        non_zero_count = (df['Quantidade'] > 0).sum()
        non_zero_ratio = non_zero_count / len(df) if len(df) > 0 else 0
        
        if non_zero_ratio < min_non_zero_ratio:
            print(f"[WARNING] Proporção baixa de dias com volume: {non_zero_ratio:.1%} (mínimo: {min_non_zero_ratio:.1%})")
            # Para volume, pode ser aceitável ter muitos zeros
            if not zero_padding:
                return pd.DataFrame(columns=df.columns)
        
        # Tratamento de outliers (cap nos percentis)
        if outlier_percentile < 100:
            cap_value = df['Quantidade'].quantile(outlier_percentile / 100)
            outliers_count = (df['Quantidade'] > cap_value).sum()
            if outliers_count > 0:
                df.loc[df['Quantidade'] > cap_value, 'Quantidade'] = cap_value
                print(f"🔧 {outliers_count} outliers de volume limitados ao percentil {outlier_percentile}%")
        
        print(f"📊 Volume pré-processado: {original_len} → {len(df)} registros")
        return df
    
    def _validate_volume_series(self, series: TimeSeries) -> bool:
        """Validação específica para séries de volume."""
        data_quality = self.config.get('data_quality', {})
        min_days = data_quality.get('min_days_history', 120)
        max_zero_consecutive = data_quality.get('max_zero_consecutive', 60)
        min_variance = data_quality.get('min_daily_variance', 0.00001)
        
        # Verificar comprimento mínimo
        if len(series) < min_days:
            print(f"❌ Histórico insuficiente: {len(series)} dias (mínimo: {min_days})")
            return False
        
        # Verificar variância mínima
        variance = np.var(series.values())
        # Tratar caso de variância NaN ou infinita
        if np.isnan(variance) or np.isinf(variance):
            variance = 0.0
        # if variance < min_variance:  # Temporariamente desabilitado
        #     print(f"❌ Variância muito baixa: {variance:.6f} (mínimo: {min_variance})")
        #     return False
        
        # Verificar dias consecutivos sem volume
        values = series.values().flatten()
        zero_streaks = []
        current_streak = 0
        
        for value in values:
            if value == 0:
                current_streak += 1
            else:
                if current_streak > 0:
                    zero_streaks.append(current_streak)
                current_streak = 0
        
        if current_streak > 0:
            zero_streaks.append(current_streak)
        
        max_streak = max(zero_streaks) if zero_streaks else 0
        if max_streak > max_zero_consecutive:
            print(f"❌ Muitos dias consecutivos sem volume: {max_streak} (máximo: {max_zero_consecutive})")
            return False
        
        print(f"[SUCCESS] Série de volume validada: {len(series)} dias, variância: {variance:.4f}")
        return True
    
    def _create_covariates(self, series: TimeSeries) -> Optional[TimeSeries]:
        """Cria covariáveis temporais específicas para volume."""
        try:
            covariates_config = self.config.get('covariates', {})
            time_attrs = covariates_config.get('time_attributes', ['dayofweek', 'month'])
            
            # Criar covariáveis básicas
            covariates_list = []
            
            for attr in time_attrs:
                if attr == 'is_weekend':
                    # Covariável customizada para fim de semana
                    weekend_cov = datetime_attribute_timeseries(
                        time_index=series.time_index,
                        attribute='dayofweek'
                    )
                    # Converter para 1 se fim de semana (5=sábado, 6=domingo), 0 caso contrário
                    weekend_values = (weekend_cov.values() >= 5).astype(int)
                    weekend_ts = TimeSeries.from_times_and_values(
                        times=series.time_index,
                        values=weekend_values,
                        columns=['is_weekend']
                    )
                    covariates_list.append(weekend_ts)
                else:
                    # Covariáveis padrão do darts
                    cov = datetime_attribute_timeseries(
                        time_index=series.time_index,
                        attribute=attr
                    )
                    covariates_list.append(cov)
            
            if not covariates_list:
                return None
            
            # Combinar todas as covariáveis
            combined_covariates = covariates_list[0]
            for cov in covariates_list[1:]:
                combined_covariates = combined_covariates.stack(cov)
            
            print(f"[SUCCESS] Covariáveis criadas: {combined_covariates.n_components} componentes")
            return combined_covariates
            
        except Exception as e:
            print(f"[WARNING] Erro ao criar covariáveis: {e}")
            return None
    
    def prepare_data(self, series: TimeSeries) -> tuple[TimeSeries, Optional[TimeSeries]]:
        """Prepara dados específicos para volume."""
        # Validar série
        if not self._validate_volume_series(series):
            raise ValueError("Série de volume não passou na validação")
        
        # Criar covariáveis
        covariates = self._create_covariates(series)
        
        # Aplicar transformações se configuradas
        preprocessing = self.config.get('preprocessing', {})
        transformed_series = series
        
        # Preencher valores ausentes
        if preprocessing.get('fill_missing', True):
            self.filler = MissingValuesFiller()
            transformed_series = self.filler.transform(transformed_series)
        
        # Ajustar valores negativos (importante para volume)
        min_val = float(transformed_series.values().min())
        if min_val < 0:
            print(f"🔧 Ajustando {-min_val:.2f} unidades para valores negativos de volume")
            adjustment = abs(min_val) + 1
            self.negative_adjuster = adjustment
            transformed_series = transformed_series + adjustment
        
        # Transformação logarítmica
        if preprocessing.get('log_transform', True):
            # Para volume, aplicar log(x + 1) para lidar com zeros
            self.log_transformer = InvertibleMapper(
                fn=lambda x: np.log1p(x),
                inverse_fn=lambda x: np.expm1(x)
            )
            transformed_series = self.log_transformer.transform(transformed_series)
        
        # Normalização
        if preprocessing.get('scale_data', True):
            self.scaler = Scaler()
            transformed_series = self.scaler.fit_transform(transformed_series)
        
        print(f"[SUCCESS] Dados de volume preparados: shape {transformed_series.values().shape}")
        return transformed_series, covariates
    
    def train_model(self, series: TimeSeries, covariates: Optional[TimeSeries] = None) -> LightGBMModel:
        """Treina modelo específico para volume."""
        model_config = self.config.get('model', {})
        
        # Parâmetros específicos para volume
        model_params = {
            'lags': model_config.get('lags', 56),
            'lags_future_covariates': model_config.get('lags_future_covariates', [0]),
            'output_chunk_length': model_config.get('forecast_horizon', 30),
            'quantiles': model_config.get('quantiles', [0.1, 0.5, 0.9]),
            'random_state': model_config.get('random_state', 42),
            # Parâmetros LightGBM otimizados para volume
            'n_estimators': model_config.get('n_estimators', 600),
            'num_leaves': model_config.get('num_leaves', 63),
            'learning_rate': model_config.get('learning_rate', 0.06),
            'max_depth': model_config.get('max_depth', -1),
            'min_data_in_leaf': model_config.get('min_data_in_leaf', 15),
            'feature_fraction': model_config.get('feature_fraction', 0.9),
            'bagging_fraction': model_config.get('bagging_fraction', 0.8),
            'bagging_freq': model_config.get('bagging_freq', 5),
            'verbosity': -1,  # Silencioso
        }
        
        # Ajustar parâmetros baseado no tipo de cliente
        client_type = self._get_client_type()
        if client_type:
            type_config = self.config.get('type_specific_configs', {}).get(client_type, {})
            model_params.update(type_config.get('model', {}))
        
        print(f"🎯 Treinando modelo de volume - Lags: {model_params['lags']}, Horizonte: {model_params['output_chunk_length']}")
        
        self.model = LightGBMModel(**model_params)
        
        # Treinar modelo
        self.model.fit(
            series=series,
            future_covariates=covariates,
            
        )
        
        print("[SUCCESS] Modelo de volume treinado com sucesso")
        return self.model
    
    def _get_client_type(self) -> Optional[str]:
        """Determina o tipo de cliente baseado na configuração."""
        # Esta função pode ser expandida para determinar automaticamente o tipo
        # Por enquanto, retorna None para usar configurações padrão
        return None
    
    def generate_forecast(self, series: TimeSeries, 
                         covariates: Optional[TimeSeries] = None,
                         horizon: Optional[int] = None) -> TimeSeries:
        """Gera previsão de volume."""
        if not self.model:
            raise ValueError("Modelo não foi treinado")
        
        if horizon is None:
            horizon = self.config.get('model', {}).get('forecast_horizon', 30)
        
        print(f"🔮 Gerando previsão de volume para {horizon} dias")
        
        # Estender covariáveis para o período de forecast se necessário
        extended_covariates = None
        if covariates is not None:
            # Obter a última data das covariáveis
            last_date = covariates.end_time()
            
            # Criar datas futuras para o período de forecast
            future_dates = pd.date_range(
                start=last_date + pd.Timedelta(days=1),
                periods=horizon,
                freq='D'
            )
            
            # Criar covariáveis futuras (features temporais)
            future_covariates_data = []
            for date in future_dates:
                # Extrair features temporais da data
                day_of_week = date.dayofweek
                month = date.month
                day_of_month = date.day
                is_weekend = 1 if date.dayofweek >= 5 else 0
                quarter = date.quarter
                day_of_year = date.dayofyear
                
                future_covariates_data.append([day_of_week, month, day_of_month, is_weekend, quarter, day_of_year])
            
            # Criar DataFrame com as mesmas colunas das covariáveis originais
            future_df = pd.DataFrame(
                future_covariates_data,
                index=future_dates,
                columns=covariates.columns
            )
            
            # Criar TimeSeries para as covariáveis futuras
            future_covariates_ts = TimeSeries.from_dataframe(future_df, freq='D')
            
            # Concatenar covariáveis originais com as futuras
            extended_covariates = covariates.concatenate(future_covariates_ts, axis=0)
        
        # Gerar previsão
        forecast = self.model.predict(
            n=horizon,
            series=series,
            future_covariates=extended_covariates
        )
        
        # Reverter transformações
        if self.scaler:
            forecast = self.scaler.inverse_transform(forecast)
        
        if self.log_transformer:
            forecast = self.log_transformer.inverse_transform(forecast)
        
        if self.negative_adjuster:
            forecast = forecast - self.negative_adjuster
            # Garantir que volume não seja negativo
            forecast = forecast.map(lambda x: np.maximum(x, 0))
        
        print(f"[SUCCESS] Previsão de volume gerada: {len(forecast)} dias")
        return forecast
    
    def evaluate_model(self, series: TimeSeries, 
                      covariates: Optional[TimeSeries] = None) -> Dict[str, float]:
        """Avalia performance do modelo com métricas específicas para volume."""
        backtest_config = self.config.get('backtest', {})
        start = backtest_config.get('start', 0.75)
        
        # Dividir dados
        split_point = int(len(series) * start)
        train_series = series[:split_point]
        test_series = series[split_point:]
        
        if len(test_series) < 7:
            print("[WARNING] Dados de teste insuficientes para avaliação")
            return {}
        
        # Treinar no conjunto de treino
        train_covariates = covariates[:split_point] if covariates else None
        temp_model = LightGBMModel(
            lags=self.config.get('model', {}).get('lags', 56),
            lags_future_covariates=[0],
            output_chunk_length=len(test_series),
            verbosity=-1
        )
        temp_model.fit(train_series, future_covariates=train_covariates)
        
        # Prever
        test_covariates = covariates[split_point:] if covariates else None
        pred_series = temp_model.predict(
            n=len(test_series),
            series=train_series,
            future_covariates=test_covariates
        )
        
        # Calcular métricas
        metrics = {
            'mape': custom_mape(test_series, pred_series),
            'mae': mae(test_series, pred_series),
            'rmse': rmse(test_series, pred_series),
            'smape': smape(test_series, pred_series)
        }
        
        # Métricas específicas para volume
        actual_values = test_series.values().flatten()
        pred_values = pred_series.values().flatten()
        
        # Bias
        bias = np.mean(pred_values - actual_values)
        metrics['bias'] = float(bias)
        
        # R²
        ss_res = np.sum((actual_values - pred_values) ** 2)
        ss_tot = np.sum((actual_values - np.mean(actual_values)) ** 2)
        r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        metrics['r2'] = float(r2)
        
        # Proporção de previsões corretas de zero (específico para volume)
        actual_zeros = (actual_values == 0)
        pred_zeros = (pred_values < 1)  # Tolerância para valores muito pequenos
        zero_accuracy = np.mean(actual_zeros.astype(bool) == pred_zeros.astype(bool))
        metrics['zero_prediction_accuracy'] = float(zero_accuracy)
        
        print(f"📊 Métricas do modelo de volume:")
        for metric, value in metrics.items():
            print(f"   {metric.upper()}: {value:.4f}")
        
        return metrics
    
    def save_results(self, database_name: str, series: TimeSeries, 
                    forecast: TimeSeries, metrics: Dict[str, float]) -> Dict[str, str]:
        """Salva resultados específicos para volume."""
        results = {}
        
        # Diretório de resultados
        results_dir = RESULTS_DIR / database_name / "volume"
        results_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # 1. Dados históricos de volume
            historical_df = series.to_dataframe().reset_index()
            historical_df.columns = ['data', 'volume_historico']
            historical_path = results_dir / 'volume_historico.csv'
            historical_df.to_csv(historical_path, index=False, sep=';')
            results['historical'] = str(historical_path)
            
            # 2. Previsões de volume
            forecast_df = forecast.to_dataframe().reset_index()
            forecast_df.columns = ['data'] + [f'volume_{q}' for q in forecast.columns]
            forecast_path = results_dir / 'previsoes_volume.csv'
            forecast_df.to_csv(forecast_path, index=False, sep=';')
            results['forecast'] = str(forecast_path)
            
            # 3. Métricas de volume
            metrics_df = pd.DataFrame([metrics])
            metrics_df['database'] = database_name
            metrics_df['timestamp'] = pd.Timestamp.now()
            metrics_path = results_dir / 'metricas_volume.csv'
            metrics_df.to_csv(metrics_path, index=False, sep=';')
            results['metrics'] = str(metrics_path)
                        
            print(f"[SUCCESS] Resultados de volume salvos em {results_dir}")
            
        except Exception as e:
            print(f"❌ Erro ao salvar resultados: {e}")
        
        return results
  
    def run_pipeline(self, database_name: str) -> Dict[str, Any]:
        """Executa pipeline completo de volume para um database."""
        start_time = pd.Timestamp.now()
        
        print(f"\n{'='*60}")
        print(f"🚀 INICIANDO PIPELINE DE VOLUME - {database_name}")
        print(f"{'='*60}")
        
        try:
            # 1. Carregar dados
            file_path = DATASET_DIR / f"{database_name}_volume.csv"
            series = self.load_volume_data(str(file_path))
            
            if series is None:
                return {
                    'status': 'ERROR',
                    'error': 'Falha ao carregar dados de volume',
                    'database_name': database_name
                }
            
            # 2. Preparar dados
            prepared_series, covariates = self.prepare_data(series)
            
            # 3. Treinar modelo
            self.train_model(prepared_series, covariates)
            
            # 4. Avaliar modelo
            metrics = self.evaluate_model(prepared_series, covariates)
            
            # 5. Gerar previsão
            forecast = self.generate_forecast(prepared_series, covariates)
            
            # 6. Salvar resultados
            saved_files = self.save_results(database_name, series, forecast, metrics)
            
            # 7. Upload FTP (se configurado)
            ftp_results = {}
            # Upload FTP (se configurado)
            ftp_results = {}
            if self.config.get('ftp_upload', {}).get('enabled', False):
                try:
                    print("[UPLOAD] Uploading volume results to FTP...")
                    
                    with ForecastFTPUploader() as ftp:
                        results_folder = str(RESULTS_DIR / database_name / "volume")
                        result = ftp.upload_volume_results(database_name, results_folder)
                        
                        if result.get('success', False):
                            print(f"[SUCCESS] FTP Upload: {result.get('message', 'Success')}")
                            uploaded_files = result.get('uploaded_files', [])
                            if uploaded_files:
                                print(f"[FILES] Uploaded: {', '.join(uploaded_files)}")
                            ftp_results = {'status': 'SUCCESS', 'uploaded_files': uploaded_files}
                        else:
                            print(f"[WARNING] FTP Upload failed: {result.get('message', 'Unknown error')}")
                            ftp_results = {'status': 'FAILED', 'error': result.get('message', 'Unknown error')}
                except Exception as e:
                    print(f"[WARNING] Erro no upload FTP: {e}")
                    ftp_results = {'status': 'ERROR', 'error': str(e)}

            duration = pd.Timestamp.now() - start_time
            
            print(f"\n[SUCCESS] PIPELINE DE VOLUME CONCLUÍDO - {database_name}")
            print(f"⏱️  Duração: {duration}")
            print(f"📊 MAPE: {metrics.get('mape', 0):.2f}%")
            
            return {
                'status': 'SUCCESS',
                'database_name': database_name,
                'metrics': metrics,
                'files': saved_files,
                'ftp_upload': ftp_results,
                'duration': str(duration),
                'forecast_horizon': len(forecast)
            }
            
        except Exception as e:
            duration = pd.Timestamp.now() - start_time
            
            print(f"\n❌ PIPELINE DE VOLUME FALHOU - {database_name}")
            print(f"[WARNING] Erro: {e}")
            print(f"⏱️  Duração: {duration}")
            
            return {
                'status': 'ERROR',
                'database_name': database_name,
                'error': str(e),
                'duration': str(duration)
            }


def run_single_database(database_name: str, config_path: str = "config/volume.yaml") -> Dict[str, Any]:
    """Executa pipeline de volume para um database específico."""
    pipeline = VolumeForecastPipeline(config_path)
    return pipeline.run_pipeline(database_name)


def run_all_databases(databases: Optional[list] = None, 
                     config_path: str = "config/volume.yaml") -> Dict[str, Any]:
    """Executa pipeline de volume para múltiplos databases."""
    
    # Databases padrão
    if databases is None:
        databases = [
            '005ATS_ERP_BI', '005NO_ERP_BI', '005RG_ERP_BI',
            '006GF_BI', '007BE_ERP_BI', '008GT_BI', '012ARR_ERP_BI',
            '012DIPUA_ERP_BI', '013BW_ERP_BI', '013CRO_ERP_BI',
            '014CST_ERP_BI', '014CST_IJC_BI', '014PR_ERP_BI', '014ZI_ERP_BI'
        ]
    
    print(f"\n🚀 EXECUTANDO PIPELINE DE VOLUME PARA {len(databases)} DATABASES")
    print("=" * 70)
    
    results = {}
    successful = 0
    
    for i, database in enumerate(databases, 1):
        print(f"\n📦 PROCESSANDO {i}/{len(databases)}: {database}")
        print("-" * 50)
        
        result = run_single_database(database, config_path)
        results[database] = result
        
        if result['status'] == 'SUCCESS':
            successful += 1
            print(f"[SUCCESS] {database}: SUCESSO")
        else:
            print(f"❌ {database}: ERRO - {result.get('error', 'Desconhecido')}")
    
    success_rate = (successful / len(databases)) * 100
    
    print(f"\n{'='*70}")
    print(f"📊 RESUMO FINAL - PIPELINE DE VOLUME")
    print(f"{'='*70}")
    print(f"[SUCCESS] Sucessos: {successful}/{len(databases)} ({success_rate:.1f}%)")
    print(f"❌ Erros: {len(databases) - successful}/{len(databases)} ({100-success_rate:.1f}%)")
    
    # Resumo das métricas
    successful_results = [r for r in results.values() if r['status'] == 'SUCCESS']
    if successful_results:
        avg_mape = np.mean([r['metrics'].get('mape', 0) for r in successful_results])
        print(f"📈 MAPE Médio: {avg_mape:.2f}%")
    
    return {
        'summary': {
            'total_databases': len(databases),
            'successful': successful,
            'success_rate': success_rate,
            'average_mape': avg_mape if successful_results else None
        },
        'results': results
    }


def main():
    """Menu interativo para pipeline de volume consolidado."""
    
    print("\n📦 VOLUME CONSOLIDADO FORECASTING PIPELINE")
    print("=" * 55)
    print("Pipeline otimizado para previsão de volume consolidado (univariado)")
    print("Dados esperados: {database}_volume.csv com colunas [data, Quantidade]")
    
    print("\nEscolha uma opção:")
    print("1. 🚀 Processar todos os databases")
    print("2. 🎯 Processar database específico") 
    print("3. 🧪 Teste com 001RR_BI")
    print("4. 📋 Listar databases disponíveis")
    print("5. 📊 Ver configuração atual")
    print("6. ❌ Sair")
    
    try:
        choice = input("\nOpção (1-6): ").strip()
        
        if choice == "1":
            print("\n🚀 Processando todos os databases...")
            results = run_all_databases()
            
            # Mostrar resumo
            summary = results.get('summary', {})
            print(f"\n📊 RESUMO FINAL:")
            print(f"[SUCCESS] Sucessos: {summary.get('successful', 0)}/{summary.get('total_databases', 0)}")
            print(f"📈 Taxa de sucesso: {summary.get('success_rate', 0):.1f}%")
            if summary.get('average_mape'):
                print(f"📊 MAPE médio: {summary.get('average_mape', 0):.2f}%")
            
        elif choice == "2":
            db_name = input("\nDigite o nome do database (ex: 001RR_BI): ").strip()
            if db_name:
                print(f"\n🎯 Processando {db_name}...")
                result = run_single_database(db_name)
                
                if result.get('status') == 'SUCCESS':
                    print(f"\n[SUCCESS] SUCESSO! Pipeline concluído para {db_name}")
                    metrics = result.get('metrics', {})
                    print(f"📊 MAPE: {metrics.get('mape', 0):.2f}%")
                    print(f"📊 MAE: {metrics.get('mae', 0):.2f}")
                    print(f"📊 R²: {metrics.get('r2', 0):.3f}")
                    print(f"⏱️  Duração: {result.get('duration', 'N/A')}")
                    print(f"🔮 Horizonte: {result.get('forecast_horizon', 30)} dias")
                else:
                    print(f"\n❌ ERRO: {result.get('error', 'Erro desconhecido')}")
            else:
                print("❌ Nome do database é obrigatório")
                
        elif choice == "3":
            print("\n🧪 Executando teste com 001RR_BI...")
            result = run_single_database('001RR_BI')
            
            if result.get('status') == 'SUCCESS':
                print(f"\n[SUCCESS] TESTE CONCLUÍDO COM SUCESSO!")
                metrics = result.get('metrics', {})
                print(f"📊 Métricas obtidas:")
                print(f"   MAPE: {metrics.get('mape', 0):.2f}%")
                print(f"   MAE: {metrics.get('mae', 0):.2f}")
                print(f"   RMSE: {metrics.get('rmse', 0):.2f}")
                print(f"   R²: {metrics.get('r2', 0):.3f}")
                if 'zero_prediction_accuracy' in metrics:
                    print(f"   Precisão predição zeros: {metrics['zero_prediction_accuracy']:.3f}")
            else:
                print(f"\n❌ TESTE FALHOU: {result.get('error', 'Erro desconhecido')}")
                
        elif choice == "4":
            print("\n📋 Databases disponíveis:")
            databases = [
                '001RR_BI', '005ATS_ERP_BI', '005NO_ERP_BI', '005RG_ERP_BI',
                '006GF_BI', '007BE_ERP_BI', '008GT_BI', '012ARR_ERP_BI',
                '012CATTO_ERP_BI', '012DIPUA_ERP_BI', '013BW_ERP_BI', '013CRO_ERP_BI',
                '014CST_ERP_BI', '014CST_IJC_BI', '014PR_ERP_BI', '014ZI_ERP_BI'
            ]
            
            # Verificar quais têm dados de volume
            from pathlib import Path
            dataset_dir = Path("dataset")
            
            print("\n🔍 Verificando disponibilidade de dados de volume:")
            available = []
            missing = []
            
            for db in databases:
                file_path = dataset_dir / f"{db}_volume.csv"
                if file_path.exists():
                    available.append(db)
                    print(f"   [SUCCESS] {db}")
                else:
                    missing.append(db)
                    print(f"   ❌ {db} (arquivo não encontrado)")
            
            print(f"\n📊 Resumo: {len(available)} disponíveis, {len(missing)} indisponíveis")
            
        elif choice == "5":
            print("\n📊 Configuração atual do pipeline:")
            pipeline = VolumeForecastPipeline()
            config = pipeline.config
            
            model_config = config.get('model', {})
            print(f"\n🤖 Modelo:")
            print(f"   Lags: {model_config.get('lags', 'N/A')}")
            print(f"   Horizonte: {model_config.get('forecast_horizon', 'N/A')} dias")
            print(f"   Learning Rate: {model_config.get('learning_rate', 'N/A')}")
            print(f"   N Estimators: {model_config.get('n_estimators', 'N/A')}")
            
            preprocessing = config.get('preprocessing', {})
            print(f"\n🔧 Pré-processamento:")
            print(f"   Preencher ausentes: {preprocessing.get('fill_missing', 'N/A')}")
            print(f"   Transformação log: {preprocessing.get('log_transform', 'N/A')}")
            print(f"   Escalar dados: {preprocessing.get('scale_data', 'N/A')}")
            print(f"   Detectar anomalias: {preprocessing.get('detect_anomalies', 'N/A')}")
            
            volume_config = config.get('volume_processing', {})
            print(f"\n📦 Processamento de Volume:")
            print(f"   Min ratio não-zero: {volume_config.get('min_non_zero_ratio', 'N/A')}")
            print(f"   Percentil outliers: {volume_config.get('outlier_cap_percentile', 'N/A')}%")
            print(f"   Permitir zeros: {volume_config.get('zero_padding', 'N/A')}")
            
            ftp_config = config.get('ftp_upload', {})
            print(f"\n[UPLOAD] Upload FTP:")
            print(f"   Habilitado: {ftp_config.get('enabled', 'N/A')}")
            print(f"   Comportamento erro: {ftp_config.get('on_error', 'N/A')}")
            
        elif choice == "6":
            print("\n👋 Saindo do pipeline de volume consolidado...")
            return
            
        else:
            print("\n❌ Opção inválida. Escolha entre 1-6.")
            
    except KeyboardInterrupt:
        print("\n\n[WARNING] Execução interrompida pelo usuário.")
        return
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        return
    
    # Perguntar se quer executar outra operação
    try:
        again = input("\n🔄 Executar outra operação? (s/N): ").strip().lower()
        if again in ['s', 'sim', 'y', 'yes']:
            main()  # Recursão para novo menu
    except KeyboardInterrupt:
        print("\n👋 Saindo...")


if __name__ == "__main__":
    main()
