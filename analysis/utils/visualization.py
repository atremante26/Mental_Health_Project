import matplotlib.pyplot as plt
import pandas as pd

def plot_cluster_distribution(labels, title="Cluster Distribution"):
    """Plot cluster size distribution"""
    plt.figure(figsize=(10, 6))
    cluster_counts = pd.Series(labels).value_counts().sort_index()
    cluster_counts.plot(kind='bar')
    plt.title(title)
    plt.xlabel('Cluster Label (-1 = Noise)')
    plt.ylabel('Number of Points')
    plt.show()

def plot_forecast_with_confidence(dates, actual, forecast, confidence_intervals):
    """Plot time series forecast with confidence bands"""
    plt.figure(figsize=(12, 8))
    plt.plot(dates, actual, label='Actual', color='black')
    plt.plot(dates, forecast, label='Forecast', color='blue')
    plt.fill_between(dates, confidence_intervals['lower'], 
                     confidence_intervals['upper'], alpha=0.3)
    plt.legend()
    plt.title('Time Series Forecast')
    plt.show()