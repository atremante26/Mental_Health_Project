import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_response_similarity(user_features, survey_df, feature_columns):
    """Calculate similarity between user and survey responses"""
    
    # Encode categorical features
    survey_encoded = pd.get_dummies(survey_df[feature_columns])
    user_encoded = pd.get_dummies(pd.DataFrame([user_features], columns=feature_columns))
    
    # Align columns (handle missing categories)
    survey_encoded = survey_encoded.reindex(columns=user_encoded.columns, fill_value=0)
    
    # Calculate cosine similarity
    similarity_scores = cosine_similarity(user_encoded, survey_encoded)[0]
    
    return similarity_scores

def get_resource_recommendations(similarity_scores, survey_df, threshold=0.3, max_recommendations=10):
    """Generate resource recommendations based on similar responses"""
    
    # Find responses above similarity threshold
    similar_mask = similarity_scores > threshold
    similar_responses = survey_df[similar_mask]
    
    if len(similar_responses) == 0:
        logger.warning("No similar responses found, using general recommendations")
        return generate_general_recommendations()
    
    # Extract patterns from similar responses (example logic)
    recommendations = []
    
    # Add logic based on your survey structure
    # This is a template - customize based on actual survey questions
    if 'treatment' in similar_responses.columns:
        treatment_seekers = similar_responses['treatment'].value_counts()
        if treatment_seekers.get('Yes', 0) > treatment_seekers.get('No', 0):
            recommendations.append("Consider professional mental health support")
    
    return recommendations[:max_recommendations]

def generate_general_recommendations():
    """Fallback general mental health resources"""
    return [
        "National Mental Health Hotline: 988",
        "Mental Health America resources",
        "Crisis Text Line: Text HOME to 741741",
        "Psychology Today therapist directory",
        "Mindfulness and meditation apps"
    ]

class ResourceMatcher:
    """Main class for resource matching functionality"""
    
    def __init__(self, survey_data):
        self.survey_data = survey_data
        self.feature_columns = []
        self.scaler = StandardScaler()
    
    def fit(self, feature_columns):
        """Prepare the matcher with survey data"""
        self.feature_columns = feature_columns
        
        # Prepare data for similarity calculations
        feature_data = self.survey_data[feature_columns]
        encoded_data = pd.get_dummies(feature_data)
        self.encoded_survey = encoded_data
        
        logger.info(f"ResourceMatcher fitted with {len(self.survey_data)} responses")
    
    def match_resources(self, user_input, top_k=5):
        """Find matching resources for user input"""
        
        similarity_scores = calculate_response_similarity(
            user_input, 
            self.survey_data, 
            self.feature_columns
        )
        
        recommendations = get_resource_recommendations(
            similarity_scores, 
            self.survey_data
        )
        
        return recommendations