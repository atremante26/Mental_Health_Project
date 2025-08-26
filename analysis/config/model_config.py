# Clustering configuration
CLUSTERING_CONFIG = {
    'hdbscan': {
        'min_cluster_size': 50,
        'min_samples': 10,
        'cluster_selection_epsilon': 0.1
    },
    'preprocessing': {
        'categorical_columns': ['Gender', 'Country', 'state', 'treatment'],
        'numeric_columns': ['Age'],
        'scale_numeric': True
    }
}

# Forecasting configuration
FORECASTING_CONFIG = {
    'prophet': {
        'seasonality_mode': 'multiplicative',
        'yearly_seasonality': True,
        'weekly_seasonality': True
    },
    'lstm': {
        'sequence_length': 30,
        'hidden_size': 50,
        'num_layers': 2
    }
}