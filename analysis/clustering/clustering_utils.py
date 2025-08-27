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
    """Evaluate clustering quality"""
    # Remove noise points (-1 labels) for silhouette score
    non_noise_mask = cluster_labels != -1
    
    if len(np.unique(cluster_labels[non_noise_mask])) < 2:
        logger.warning("Less than 2 clusters found, cannot compute silhouette score")
        return None
    
    silhouette_avg = silhouette_score(
        1 - distance_matrix[non_noise_mask][:, non_noise_mask], 
        cluster_labels[non_noise_mask], 
        metric='precomputed'
    )
    
    return silhouette_avg