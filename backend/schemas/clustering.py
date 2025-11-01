from pydantic import BaseModel, Field
from typing import Dict, Optional

class UserInput(BaseModel):
    age: int = Field(..., ge=18, le=100)
    gender: str
    country: str
    treatment: str
    work_interfere: str
    family_history: str
    remote_work: str
    mental_health_consequence: str
    benefits: str

class ClusterInfo(BaseModel):
    cluster_id: int
    size: int
    percentage: float
    description: str
    characteristics: Dict

class PredictionResponse(BaseModel):
    cluster_id: int
    cluster_name: str
    description: str
    characteristics: Dict
    confidence: Optional[float] = None