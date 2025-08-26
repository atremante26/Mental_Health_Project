import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder
from gower import gower_matrix
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def prepare_clustering_features(df, categorical_cols, numeric_cols):
    """Prepare features for clustering with mixed data types"""
    # Select only relevant columns
    feature_df = df[categorical_cols + numeric_cols].copy()
    
    # Handle any remaining nulls
    feature_df = feature_df.dropna()
    
    # Encode categorical variables for Gower distance
    le_dict = {}
    for col in categorical_cols:
        le = LabelEncoder()
        feature_df[col] = le.fit_transform(feature_df[col].astype(str))
        le_dict[col] = le
    
    # Scale numeric variables
    scaler = StandardScaler()
    feature_df[numeric_cols] = scaler.fit_transform(feature_df[numeric_cols])
    
    return feature_df, le_dict, scaler

def compute_gower_distance(df, categorical_indices):
    """Compute Gower distance matrix for mixed data types"""
    # Create categorical mask
    cat_features = [i in categorical_indices for i in range(df.shape[1])]
    
    # Compute Gower distance
    distance_matrix = gower_matrix(df.values, cat_features=cat_features)
    
    return distance_matrix

def prepare_time_series(df, date_col, value_col):
    """Prepare time series data for forecasting"""
    ts_df = df[[date_col, value_col]].copy()
    ts_df[date_col] = pd.to_datetime(ts_df[date_col])
    ts_df = ts_df.sort_values(date_col)
    ts_df = ts_df.set_index(date_col)
    
    return ts_df