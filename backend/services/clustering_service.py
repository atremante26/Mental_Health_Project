import pickle
import logging
from pathlib import Path
import numpy as np

# Configure logging
logger = logging.getLogger(__name__)

class ClusteringService:
    def __init__(self):
        self.models = {}
        self.models_dir = Path(__file__).parent.parent / "models"
    
    def load_models(self):
        try:
            model_files = list(self.models_dir.glob("*.pkl"))
            
            if not model_files:
                logger.warning("No model files found")
                return
            
            for file in model_files:
                if "hdbscan_clusterer" in file.name:
                    with open(file, 'rb') as f:
                        self.models['clusterer'] = pickle.load(f)
                    logger.info(f"Loaded clusterer from {file.name}")
                
                elif "clustering_preprocessing" in file.name:
                    with open(file, 'rb') as f:
                        self.models['preprocessing'] = pickle.load(f)
                    logger.info(f"Loaded preprocessing from {file.name}")
                
                elif "cluster_results" in file.name:
                    with open(file, 'rb') as f:
                        self.models['results'] = pickle.load(f)
                    logger.info(f"Loaded results from {file.name}")
            
            logger.info("Clustering models loaded successfully")
            
        except Exception as e:
            logger.error(f"Error loading models: {e}", exc_info=True)
    
    def is_loaded(self) -> bool:
        return all(key in self.models for key in ['clusterer', 'preprocessing', 'results'])
    
    def get_all_clusters(self):
        if not self.is_loaded():
            raise ValueError("Models not loaded")
        
        results = self.models['results']
        labels = results['labels']
        cluster_profiles = results.get('cluster_profiles', {})
        unique_labels = [l for l in np.unique(labels) if l != -1]
        
        descriptions = {
            0: "Remote Workers with Severe Impact",
            1: "Mainstream Tech Workers",
            2: "Uninformed/Uncertain Group"
        }
        
        clusters = []
        for label in unique_labels:
            cluster_size = int(np.sum(labels == label))
            cluster_pct = float(cluster_size / len(labels) * 100)
            profile = cluster_profiles.get(label, {})
            
            clusters.append({
                "cluster_id": int(label),
                "size": cluster_size,
                "percentage": round(cluster_pct, 1),
                "description": descriptions.get(label, f"Cluster {label}"),
                "characteristics": profile
            })
        
        return clusters
    
    def get_cluster_detail(self, cluster_id: int):
        if not self.is_loaded():
            raise ValueError("Models not loaded")
        
        results = self.models['results']
        labels = results['labels']
        
        if cluster_id not in np.unique(labels):
            raise ValueError(f"Cluster {cluster_id} not found")
        
        cluster_profiles = results.get('cluster_profiles', {})
        profile = cluster_profiles.get(cluster_id, {})
        
        cluster_size = int(np.sum(labels == cluster_id))
        cluster_pct = float(cluster_size / len(labels) * 100)
        
        descriptions = {
            0: "Remote Workers with Severe Impact",
            1: "Mainstream Tech Workers",
            2: "Uninformed/Uncertain Group"
        }
        
        return {
            "cluster_id": cluster_id,
            "name": descriptions.get(cluster_id, f"Cluster {cluster_id}"),
            "size": cluster_size,
            "percentage": round(cluster_pct, 1),
            "profile": profile,
            "description": descriptions.get(cluster_id, "")
        }
    
    def predict_cluster(self, user_data: dict):
        if not self.is_loaded():
            raise ValueError("Models not loaded")
        
        # Simple heuristic-based prediction
        # In production, implement proper distance-based prediction
        cluster_id = 1  # Default
        
        if user_data.get('remote_work', '').lower() == 'yes' and \
           user_data.get('work_interfere', '').lower() == 'often':
            cluster_id = 0
        elif user_data.get('benefits', '').lower() == "don't know":
            cluster_id = 2
        
        descriptions = {
            0: "Remote Workers with Severe Impact - You match the profile of remote workers experiencing frequent mental health interference at work",
            1: "Mainstream Tech Workers - You match the typical tech worker profile with moderate symptoms and workplace awareness",
            2: "Uninformed/Uncertain Group - You match the profile of employees who may lack awareness about workplace mental health resources"
        }
        
        cluster_names = {
            0: "Remote Workers with Severe Impact",
            1: "Mainstream Tech Workers",
            2: "Uninformed/Uncertain Group"
        }
        
        results = self.models.get('results', {})
        cluster_profiles = results.get('cluster_profiles', {})
        characteristics = cluster_profiles.get(cluster_id, {})
        
        return {
            "cluster_id": cluster_id,
            "cluster_name": cluster_names[cluster_id],
            "description": descriptions[cluster_id],
            "characteristics": characteristics,
            "confidence": 0.75
        }