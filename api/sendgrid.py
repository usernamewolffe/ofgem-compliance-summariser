from fastapi import Form, Request, APIRouter
from fastapi.responses import JSONResponse
from tools.email_utils import send_article_email
from storage.db import DB

router = APIRouter()
db = DB("ofgem.db")

@router.post("/send")
async def send_article(request: Request, guid: str = Form(...), email: str = Form(...)):
    """Send an article to a colleague by email."""
    items = [i for i in db.list_items(limit=5000) if i["guid"] == guid]
    if not items:
        return JSONResponse({"ok": False, "error": "Article not found"}, status_code=404)

    item = items[0]
    ok = send_article_email(email, item)
    return JSONResponse({"ok": ok})
