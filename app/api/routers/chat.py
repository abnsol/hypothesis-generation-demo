from __future__ import annotations

from fastapi import (
    APIRouter,
    Depends,    
    HTTPException,
    Request,
)

from app.api.dependencies import get_current_user_id

from app.core.deps import get_dependencies

router = APIRouter()

# ──────────────────────────────────────────────────────────────────────────────
# /chat
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/")
async def chat(
    request: Request,
    current_user_id: str = Depends(get_current_user_id),
    deps: dict = Depends(get_dependencies)
):
    form = await request.form()
    query = form.get("query")
    hypothesis_id = form.get("hypothesis_id")

    hypotheses = deps["hypotheses"]
    llm = deps["llm"]

    hypothesis = hypotheses.get_hypotheses(current_user_id, hypothesis_id)
    if not hypothesis:
        raise HTTPException(
            status_code=404, detail="Hypothesis not found or access denied"
        )

    graph = hypothesis.get("graph")
    response = llm.chat(query, graph)
    return {"response": response}

