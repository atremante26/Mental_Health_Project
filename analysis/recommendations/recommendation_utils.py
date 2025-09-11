import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# CORE SIMILARITY FUNCTIONS
# =============================================================================

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
    
    # Extract patterns from similar responses
    recommendations = []
    
    if 'treatment' in similar_responses.columns:
        treatment_seekers = similar_responses['treatment'].value_counts()
        if treatment_seekers.get('Yes', 0) > treatment_seekers.get('No', 0):
            recommendations.append("Consider professional mental health support")
    
    return recommendations[:max_recommendations]

# =============================================================================
# RESOURCE DATABASES
# =============================================================================

def generate_general_recommendations():
    """Fallback general mental health resources"""
    return [
        "National Mental Health Hotline: 988",
        "Crisis Text Line: Text HOME to 741741", 
        "Mental Health America resources",
        "Psychology Today therapist directory",
        "Mindfulness and meditation apps"
    ]

def prepare_geographic_resources(mental_health_care_df, who_df):
    """Create geographic resource database from mental health care access data"""
    geographic_resources = {}
    
    if 'country' in mental_health_care_df.columns:
        countries = mental_health_care_df['country'].unique()
        
        for country in countries:
            country_data = mental_health_care_df[mental_health_care_df['country'] == country]
            
            geographic_resources[country] = {
                'general_resources': [
                    f"Local mental health services in {country}",
                    f"National mental health hotlines for {country}",
                    f"Community health centers in {country}"
                ],
                'data_available': len(country_data) > 0
            }
    
    return geographic_resources

def enhance_recommendations_with_location(base_recommendations, user_country, geographic_resources):
    """Add location-specific resources to base recommendations"""
    enhanced_recs = base_recommendations.copy()
    
    # Add location-specific resources
    if user_country in geographic_resources:
        local_resources = geographic_resources[user_country]['general_resources']
        enhanced_recs.extend(local_resources)
    
    # Add universal crisis resources
    universal_resources = [
        "Crisis Text Line: Text HOME to 741741",
        "National Suicide Prevention Lifeline: 988",
        "Psychology Today therapist directory",
        "Mental Health America resources"
    ]
    enhanced_recs.extend(universal_resources)
    
    return list(set(enhanced_recs))  # Remove duplicates

# =============================================================================
# MAIN RESOURCE MATCHER CLASS
# =============================================================================

class ResourceMatcher:
    """
    Main class for resource matching functionality.
    
    Matches users to relevant mental health resources based on similarity 
    to existing survey responses, with optional geographic enhancement.
    """
    
    def __init__(self, survey_data, geographic_resources=None):
        """
        Initialize ResourceMatcher
        
        Args:
            survey_data (pd.DataFrame): Survey response data for similarity matching
            geographic_resources (dict, optional): Geographic resource database
        """
        self.survey_data = survey_data
        self.geographic_resources = geographic_resources or {}
        self.feature_columns = []
        self.scaler = StandardScaler()
        self.encoded_survey = None
    
    def fit(self, feature_columns):
        """Prepare the matcher with survey data"""
        self.feature_columns = feature_columns
        
        # Prepare data for similarity calculations
        feature_data = self.survey_data[feature_columns]
        self.encoded_survey = pd.get_dummies(feature_data)
        
        logger.info(f"ResourceMatcher fitted with {len(self.survey_data)} responses")
    
    def match_resources(self, user_input, include_location=True):
        """
        Find matching resources for user input
        
        Args:
            user_input (dict): User profile for matching
            include_location (bool): Whether to include location-based resources
            
        Returns:
            list: Recommended resources
        """
        # Get similarity-based recommendations
        similarity_scores = calculate_response_similarity(
            user_input, 
            self.survey_data, 
            self.feature_columns
        )
        
        base_recommendations = get_resource_recommendations(
            similarity_scores, 
            self.survey_data
        )
        
        # Enhance with location if requested and available
        if include_location and 'Country' in user_input and self.geographic_resources:
            user_country = user_input['Country']
            return enhance_recommendations_with_location(
                base_recommendations, 
                user_country, 
                self.geographic_resources
            )
        
        return base_recommendations
    
    def get_similarity_stats(self, user_input):
        """Get similarity statistics for debugging/analysis"""
        similarity_scores = calculate_response_similarity(
            user_input, 
            self.survey_data, 
            self.feature_columns
        )
        
        return {
            'mean_similarity': similarity_scores.mean(),
            'max_similarity': similarity_scores.max(),
            'above_threshold_count': (similarity_scores > 0.3).sum()
        }