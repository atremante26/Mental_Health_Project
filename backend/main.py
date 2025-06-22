from fastapi import FastAPI # import FastAPI framework

app = FastAPI() # Create FastAPI instance

@app.get("/health") # GET endpoint of path /health
def health_check():
    return {"status": "ok"}

@app.get("/insights") # Placeholder API endpoint for insights
def get_insights():
    return {
        "summary": "Mental health trends for this week will appear here.",
        "date_range": "Coming soon"
    }