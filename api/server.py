from fastapi import FastAPI, Query
from storage.db import DB

app = FastAPI()
db = DB("ofgem.db")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/items")
def items(limit: int = Query(50, ge=1, le=200)):
    return db.list_items(limit=limit)
