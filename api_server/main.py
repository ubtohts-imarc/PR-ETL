from fastapi import FastAPI
from api_server.app.data.api.endpoints import router as data_router
from api_server.app.dashboard.admin import setup_admin
# from api_server.core.config import settings

app = FastAPI(
    title="ETL API Server",
    version="1.0.0"
)

# Include API routes
app.include_router(data_router, prefix="/api/v1", tags=["Data APIs"])

# Setup SQLAdmin dashboard
setup_admin(app)

# Root health check
@app.get("/")
async def root():
    return {"message": "Welcome to ETL API Server"}
