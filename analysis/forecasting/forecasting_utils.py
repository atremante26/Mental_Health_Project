"""Forecasting utilities and evaluation functions"""

import pandas as pd
import pickle
import os
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def evaluate_forecast_model(model, train_data, test_data, model_type='prophet'):
    """Evaluate forecasting model performance"""
    if model_type == 'prophet':
        # Generate predictions for test period
        test_future = model.make_future_dataframe(periods=len(test_data))
        forecast = model.predict(test_future)
        predictions = forecast['yhat'][-len(test_data):]
    else:
        raise ValueError(f"Model type {model_type} not supported")
    
    # Calculate metrics
    mae = mean_absolute_error(test_data['y'], predictions)
    mse = mean_squared_error(test_data['y'], predictions)
    rmse = np.sqrt(mse)
    
    # Calculate percentage error
    mape = np.mean(np.abs((test_data['y'] - predictions) / test_data['y'])) * 100
    
    return {
        'mae': mae,
        'mse': mse, 
        'rmse': rmse,
        'mape': mape
    }

def train_prophet_model(data, config):
    """Train Prophet model with configuration"""
    model = Prophet(
        seasonality_mode=config.get('seasonality_mode', 'multiplicative'),
        yearly_seasonality=config.get('yearly_seasonality', True),
        weekly_seasonality=config.get('weekly_seasonality', True),
        daily_seasonality=config.get('daily_seasonality', False)
    )
    
    model.fit(data)
    return model

def cross_validate_timeseries(data, config, initial_days=180, period_days=30, horizon_days=90):
    """Perform time series cross-validation"""
    
    model = train_prophet_model(data, config)
    
    # Perform cross-validation
    cv_results = cross_validation(
        model, 
        initial=f'{initial_days} days',
        period=f'{period_days} days', 
        horizon=f'{horizon_days} days'
    )
    
    # Calculate performance metrics
    metrics = performance_metrics(cv_results)
    
    return cv_results, metrics

def save_forecast_results(model, forecast, filename):
    """Save forecast results and model"""
    
    os.makedirs('../outputs/results', exist_ok=True)
    
    # Save forecast data
    forecast.to_csv(f'../outputs/results/{filename}_forecast.csv', index=False)
    
    # Save model
    with open(f'../models/saved_models/{filename}_model.pkl', 'wb') as f:
        pickle.dump(model, f)
    
    logger.info(f"Forecast results saved: {filename}")