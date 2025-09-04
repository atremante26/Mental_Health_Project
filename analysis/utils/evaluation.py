from sklearn.metrics import silhouette_score, mean_absolute_error, mean_squared_error
from insights.prompt_templates import COMPREHENSIVE_ANALYSIS_PROMPT
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def evaluate_clustering_quality(distance_matrix, labels):
    """Comprehensive clustering evaluation"""
    # Remove noise points
    non_noise_mask = labels != -1
    if len(np.unique(labels[non_noise_mask])) < 2:
        return {"silhouette_score": None, "noise_ratio": np.mean(labels == -1)}
    
    silhouette_avg = silhouette_score(
        1 - distance_matrix[non_noise_mask][:, non_noise_mask], 
        labels[non_noise_mask], 
        metric='precomputed'
    )
    
    return {
        "silhouette_score": silhouette_avg,
        "n_clusters": len(np.unique(labels[non_noise_mask])),
        "noise_ratio": np.mean(labels == -1)
    }

def evaluate_forecast_accuracy(actual, predicted):
    """Time series forecasting metrics"""
    mae = mean_absolute_error(actual, predicted)
    mse = mean_squared_error(actual, predicted)
    mape = np.mean(np.abs((actual - predicted) / actual)) * 100
    
    return {"mae": mae, "rmse": np.sqrt(mse), "mape": mape}

def analyze_comprehensive_mental_health(reddit_text, stats_text, llm_model):
    """Analyze Reddit discussions and statistical data using LLM"""
    try:
        
        prompt = COMPREHENSIVE_ANALYSIS_PROMPT.format(
            stats_text=stats_text,
            reddit_text=reddit_text
        )
        
        response = llm_model.generate_content(prompt)
        return response.text
    
    except Exception as e:
        logging.error(f"Error in comprehensive mental health analysis: {e}")
        return None