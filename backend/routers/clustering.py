from fastapi import APIRouter, HTTPException, Depends
from typing import List
import logging

from schemas.clustering import UserInput, ClusterInfo, PredictionResponse

logger = logging.getLogger(__name__)
router = APIRouter()

def get_clustering_service():
    from main import clustering_service
    if not clustering_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return clustering_service

@router.get("/clusters", response_model=List[ClusterInfo])
def get_clusters(service = Depends(get_clustering_service)):
    try:
        return service.get_all_clusters()
    except Exception as e:
        logger.error(f"Error in get_clusters: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/clusters/{cluster_id}")
def get_cluster_detail(cluster_id: int, service = Depends(get_clustering_service)):
    try:
        return service.get_cluster_detail(cluster_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error in get_cluster_detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/clusters/predict", response_model=PredictionResponse)
def predict_cluster(user_input: UserInput, service = Depends(get_clustering_service)):
    try:
        return service.predict_cluster(user_input.dict())
    except Exception as e:
        logger.error(f"Error in predict_cluster: {e}")
        raise HTTPException(status_code=500, detail=str(e))