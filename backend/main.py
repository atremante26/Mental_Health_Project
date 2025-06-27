from fastapi import FastAPI # import FastAPI framework
from fastapi.middleware.cors import CORSMiddleware # import CORS middleware to allow cross-origin requests from frontend
import os
import json


app = FastAPI() # Create FastAPI instance

# Allow localhost frontend to access backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",                # local frontend dev
        "https://atremante26.github.io",        # GitHub Pages
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health") # GET endpoint of path /health
def health_check():
    return {"status": "ok"}

@app.get("/insights") # Placeholder API endpoint for insights
def get_insights():
    file_path = os.path.join(os.path.dirname(__file__), "data", "insights.json")
    with open(file_path, "r") as f:
        data = json.load(f)
    return data

@app.get("/timeseries")
def get_timeseries():
    file_path = os.path.join(os.path.dirname(__file__), "../data/processed/cdc_timeseries.json")
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        return {"error": f"Could not load timeseries data: {e}"}