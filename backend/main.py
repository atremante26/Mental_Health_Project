from fastapi import FastAPI # import FastAPI framework
from fastapi.middleware.cors import CORSMiddleware # import CORS middleware to allow cross-origin requests from frontend



app = FastAPI() # Create FastAPI instance

# Allow localhost frontend to access backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",                # local frontend dev
        "https://your-username.github.io",     # GitHub Pages (optional)
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
    return {
        "summary": "Mental health trends for this week will appear here.",
        "date_range": "Coming soon"
    }