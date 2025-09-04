from sklearn.metrics import silhouette_score, mean_absolute_error, mean_squared_error
import numpy as np

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