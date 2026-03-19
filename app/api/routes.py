from fastapi import APIRouter
from app.api.routers import (
    enrichment,
    hypothesis,
    projects,
    analysis,
    phenotypes,
    credible_sets,
    gwas,
    files,
    chat,
    internal
)

api_router = APIRouter()

# Register routes
api_router.include_router(enrichment.router, prefix="/enrich", tags=["enrichment"])
api_router.include_router(hypothesis.router, prefix="/hypothesis", tags=["hypothesis"])
api_router.include_router(projects.router, prefix="/projects", tags=["projects"])
api_router.include_router(analysis.router, prefix="/analysis-pipeline", tags=["analysis"])
api_router.include_router(phenotypes.router, prefix="/phenotypes", tags=["phenotypes"])
api_router.include_router(credible_sets.router, prefix="/credible-sets", tags=["credible-sets"])
api_router.include_router(gwas.router, prefix="/gwas-files", tags=["gwas"])
api_router.include_router(files.router, prefix="/user-files", tags=["files"])
api_router.include_router(chat.router, prefix="/chat", tags=["chat"])
api_router.include_router(internal.router, prefix="/internal", tags=["internal"])