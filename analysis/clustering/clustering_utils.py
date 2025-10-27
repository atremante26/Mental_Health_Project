import numpy as np
import pandas as pd
from hdbscan import HDBSCAN
from sklearn.metrics import silhouette_score
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_hdbscan_clustering(distance_matrix, min_cluster_size=15, min_samples=10):
    """Run HDBSCAN clustering on distance matrix"""
    clusterer = HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric='precomputed'
    )
    
    cluster_labels = clusterer.fit_predict(distance_matrix)
    
    return clusterer, cluster_labels

def evaluate_clustering(distance_matrix, cluster_labels):
    # Filter out noise points
    non_noise_mask = cluster_labels != -1
    
    if sum(non_noise_mask) < 2:
        logger.warning("Less than 2 non-noise points, cannot compute silhouette score")
        return None
    
    n_clusters = len(set(cluster_labels[non_noise_mask]))
    if n_clusters < 2:
        logger.warning("Less than 2 clusters found, cannot compute silhouette score")
        return None
    
    # Extract distance matrix for non-noise points
    filtered_distance = distance_matrix[non_noise_mask][:, non_noise_mask]
    
    # Diagonal is exactly zero for precomputed distances
    np.fill_diagonal(filtered_distance, 0)
    
    silhouette_avg = silhouette_score(
        filtered_distance,
        cluster_labels[non_noise_mask], 
        metric='precomputed'
    )
    
    return silhouette_avg