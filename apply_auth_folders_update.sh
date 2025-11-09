#!/usr/bin/env bash
set -euo pipefail

echo "== Compliance Summariser: Auth + Folders one-shot update =="

# --- helpers ---------------------------------------------------------------
add_line_if_missing () {
  local file="$1" line="$2"
  grep -Fqx "$line" "$file" 2>/dev/null || echo "$line" >> "$file"
}

insert_after_first_match () {
  # insert $3 after first line that CONTAINS $2 in file $1 (if $3 not already present)
  local file="$1" pattern="$2" insert="$3"
  grep -Fq "$insert" "$file" && return 0
  awk -v pat="$pattern" -v ins="$insert" '
    BEGIN{done=0}
    {
      print
      if(!done && index($0, pat) > 0){
        print ins
        done=1
      }
    }' "$file" > "$file.tmp" && mv "$file.tmp" "$file"
}


append_block_if_missing () {
  local file="$1" marker="$2" block="$3"
  grep -Fq "$marker" "$file" 2>/dev/null && return 0
  printf "\n%s\n" "$block" >> "$file"
}

ensure_dir () { mkdir -p "$1"; }

project_root="$(pwd)"

# --- sanity checks ---------------------------------------------------------
req_files=("api/server.py" "storage/db.py" "summariser/templates/summariser/summaries.html")
for f in "${req_files[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: required file missing: $f"
    exit 1
  fi
done

# --- 0) requirements + .env.example ---------------------------------------
touch requirements.txt
add_line_if_missing requirements.txt "passlib[bcrypt]>=1.7"
add_line_if_missing requirements.txt "python-multipart>=0.0.9"
add_line_if_missing requirements.txt "itsdangerous>=2.2"
add_line_if_missing requirements.txt "pypdf>=5.0.0"

if [[ ! -f .env.example ]]; then
  cat > .env.example <<'EOF'
OPENAI_API_KEY=
DB_PATH=ofgem.db
SESSIONS_SECRET=change-me
EOF
else
  grep -q '^SESSIONS_SECRET=' .env.example || echo 'SESSIONS_SECRET=change-me' >> .env.example
  grep -q '^DB_PATH=' .env.example || echo 'DB_PATH=ofgem.db' >> .env.example
fi

# --- 1) api/server.py: imports, session middleware, helpers ----------------
srv="api/server.py"

# imports
insert_after_first_match "$srv" '^from fastapi import FastAPI' \
'from starlette.middleware.sessions import SessionMiddleware'
insert_after_first_match "$srv" '^from pydantic import BaseModel' \
'from passlib.hash import bcrypt'
insert_after_first_match "$srv" '^from pydantic import BaseModel' \
'from fastapi import Form'

# middleware (after app = FastAPI())
insert_after_first_match "$srv" '^app = FastAPI' \
$'app.add_middleware(\n    SessionMiddleware,\n    secret_key=os.getenv("SESSIONS_SECRET", "dev-secret"),\n    same_site="lax",\n    https_only=False,\n)\n'

# helpers (only if missing)
append_block_if_missing "$srv" "# --- auth helpers ---" $'# --- auth helpers ---\n\ndef get_user_id(request: Request) -> int | None:\n    return request.session.get("uid")\n\ndef require_user(request: Request) -> int:\n    uid = get_user_id(request)\n    if not uid:\n        raise HTTPException(401, "Login required")\n    return uid\n'

# call db.init_auth() after db = DB("ofgem.db")
insert_after_first_match "$srv" '^db = DB\(' \
'db.init_auth()  # initialize users/folders/saved_items tables'

# --- 2) storage/db.py: schema + helpers -----------------------------------
dbpy="storage/db.py"

append_block_if_missing "$dbpy" "CREATE TABLE IF NOT EXISTS users" $'def init_auth(self):\n    with self.conn:\n        self.conn.execute("""\n        CREATE TABLE IF NOT EXISTS users (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            email TEXT UNIQUE NOT NULL,\n            password_hash TEXT NOT NULL,\n            created_at TEXT DEFAULT CURRENT_TIMESTAMP\n        )""")\n        self.conn.execute("""\n        CREATE TABLE IF NOT EXISTS folders (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            user_id INTEGER NOT NULL,\n            name TEXT NOT NULL,\n            UNIQUE(user_id, name),\n            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE\n        )""")\n        self.conn.execute("""\n        CREATE TABLE IF NOT EXISTS saved_items (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            user_id INTEGER NOT NULL,\n            folder_id INTEGER,\n            guid TEXT NOT NULL,\n            created_at TEXT DEFAULT CURRENT_TIMESTAMP,\n            UNIQUE(user_id, guid),\n            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,\n            FOREIGN KEY(folder_id) REFERENCES folders(id) ON DELETE SET NULL\n        )""")\n'

append_block_if_missing "$dbpy" "def get_user_by_email" $'def get_user_by_email(self, email: str):\n    cur = self.conn.execute("SELECT * FROM users WHERE email=?", (email.lower().strip(),))\n    r = cur.fetchone()\n    return dict(r) if r else None\n\ndef create_user(self, email: str, password_hash: str) -> int:\n    cur = self.conn.execute("INSERT INTO users (email, password_hash) VALUES (?,?)", (email.lower().strip(), password_hash))\n    return cur.lastrowid\n\ndef get_user(self, uid: int):\n    cur = self.conn.execute("SELECT * FROM users WHERE id=?", (uid,))\n    r = cur.fetchone()\n    return dict(r) if r else None\n'

append_block_if_missing "$dbpy" "def list_folders" $'def list_folders(self, user_id: int):\n    cur = self.conn.execute("SELECT id, name FROM folders WHERE user_id=? ORDER BY name", (user_id,))\n    return [dict(r) for r in cur.fetchall()]\n\ndef create_folder(self, user_id: int, name: str) -> int:\n    name = name.strip()\n    if not name:\n        return 0\n    cur = self.conn.execute("INSERT OR IGNORE INTO folders (user_id, name) VALUES (?,?)", (user_id, name))\n    if cur.lastrowid:\n        return cur.lastrowid\n    cur = self.conn.execute("SELECT id FROM folders WHERE user_id=? AND name=?", (user_id, name))\n    row = cur.fetchone()\n    return row[0] if row else 0\n\ndef delete_folder(self, user_id: int, folder_id: int):\n    self.conn.execute("DELETE FROM folders WHERE id=? AND user_id=?", (folder_id, user_id))\n'

append_block_if_missing "$dbpy" "def save_item(self" $'def save_item(self, user_id: int, guid: str, folder_id: int | None):\n    self.conn.execute("INSERT OR IGNORE INTO saved_items (user_id, guid, folder_id) VALUES (?,?,?)", (user_id, guid, folder_id))\n\ndef unsave_item(self, user_id: int, guid: str):\n    self.conn.execute("DELETE FROM saved_items WHERE user_id=? AND guid=?", (user_id, guid))\n\ndef list_saved_items(self, user_id: int, folder_id: int | None = None, limit: int = 500):\n    if folder_id:\n        sql = """SELECT si.guid, i.title, i.link, i.published_at, f.name AS folder\n                 FROM saved_items si\n                 LEFT JOIN items i ON i.guid=si.guid\n                 LEFT JOIN folders f ON f.id=si.folder_id\n                 WHERE si.user_id=? AND si.folder_id=?\n                 ORDER BY si.created_at DESC LIMIT ?"""\n        args = (user_id, folder_id, limit)\n    else:\n        sql = """SELECT si.guid, i.title, i.link, i.published_at, f.name AS folder\n                 FROM saved_items si\n                 LEFT JOIN items i ON i.guid=si.guid\n                 LEFT JOIN folders f ON f.id=si.folder_id\n                 WHERE si.user_id=? ORDER BY si.created_at DESC LIMIT ?"""\n        args = (user_id, limit)\n    cur = self.conn.execute(sql, args)\n    return [dict(r) for r in cur.fetchall()]\n'

# --- 3) server routes: auth + folders + save API ---------------------------
auth_routes=$'# --- Auth routes (login/register/logout) ---\nfrom fastapi import Form\n\n@app.get("/account/login", response_class=HTMLResponse)\ndef login_page(request: Request):\n    return templates.TemplateResponse("login.html", {"request": request, "error": ""})\n\n@app.post("/account/login")\ndef login(request: Request, email: str = Form(...), password: str = Form(...)):\n    user = db.get_user_by_email(email)\n    if not user or not bcrypt.verify(password, user["password_hash"]):\n        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"}, status_code=400)\n    request.session["uid"] = user["id"]\n    return RedirectResponse(url="/summaries", status_code=302)\n\n@app.get("/account/register", response_class=HTMLResponse)\ndef register_page(request: Request):\n    return templates.TemplateResponse("register.html", {"request": request, "error": ""})\n\n@app.post("/account/register")\ndef register(request: Request, email: str = Form(...), password: str = Form(...), confirm: str = Form(...)):\n    if password != confirm:\n        return templates.TemplateResponse("register.html", {"request": request, "error": "Passwords do not match"}, status_code=400)\n    if db.get_user_by_email(email):\n        return templates.TemplateResponse("register.html", {"request": request, "error": "Email already registered"}, status_code=400)\n    uid = db.create_user(email, bcrypt.hash(password))\n    request.session["uid"] = uid\n    return RedirectResponse(url="/summaries", status_code=302)\n\n@app.post("/account/logout")\ndef logout(request: Request):\n    request.session.clear()\n    return RedirectResponse(url="/summaries", status_code=302)\n\n# --- Folder & Save APIs ---\nclass FolderIn(BaseModel):\n    name: str\n\n@app.get("/api/folders")\ndef api_folders(request: Request):\n    uid = require_user(request)\n    return {\"folders\": db.list_folders(uid)}\n\n@app.post("/api/folders")\ndef api_create_folder(request: Request, payload: FolderIn):\n    uid = require_user(request)\n    fid = db.create_folder(uid, payload.name)\n    if not fid:\n        raise HTTPException(400, \"Invalid folder name\")\n    return {\"ok\": True, \"id\": fid}\n\nclass SaveIn(BaseModel):\n    guid: str\n    folder_id: int | None = None\n\n@app.post(\"/api/save\")\ndef api_save(request: Request, payload: SaveIn):\n    uid = require_user(request)\n    db.save_item(uid, payload.guid, payload.folder_id)\n    return {\"ok\": True}\n\n@app.delete(\"/api/save/{guid}\")\ndef api_unsave(request: Request, guid: str):\n    uid = require_user(request)\n    db.unsave_item(uid, guid)\n    return {\"ok\": True}\n\n@app.get(\"/saved\", response_class=HTMLResponse)\ndef saved_page(request: Request, folder_id: int | None = None):\n    uid = require_user(request)\n    folders = db.list_folders(uid)\n    items = db.list_saved_items(uid, folder_id=folder_id)\n    return templates.TemplateResponse(\"saved.html\", {\"request\": request, \"folders\": folders, \"items\": items, \"active_folder\": folder_id})\n'
append_block_if_missing "$srv" "# --- Folder & Save APIs ---" "$auth_routes"

# --- 4) Templates: login/register/saved -----------------------------------
ensure_dir summariser/templates/summariser

login_tpl='<!doctype html>
<html><head><meta charset="utf-8"><title>Login</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:2rem}
.container{max-width:420px;margin:0 auto}
.muted{color:#666}
.btn{background:#fff;border:1px solid #ccc;border-radius:6px;padding:.5rem .8rem;cursor:pointer}
.btn.primary{border-color:#0a66c2;color:#0a66c2}
input{width:100%;padding:.6rem .7rem;border:1px solid #ccc;border-radius:6px;margin:.4rem 0}
</style></head>
<body><div class="container">
  <h2>Sign in</h2>
  {% if error %}<div class="muted" style="color:#b32d2e">{{ error }}</div>{% endif %}
  <form method="post">
    <input type="email" name="email" placeholder="you@company.com" required>
    <input type="password" name="password" placeholder="Password" required>
    <button class="btn primary" type="submit">Sign in</button>
  </form>
  <p class="muted">No account? <a href="/account/register">Register</a></p>
</div></body></html>
'

register_tpl='<!doctype html>
<html><head><meta charset="utf-8"><title>Register</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:2rem}
.container{max-width:420px;margin:0 auto}
.muted{color:#666}
.btn{background:#fff;border:1px solid #ccc;border-radius:6px;padding:.5rem .8rem;cursor:pointer}
.btn.primary{border-color:#0a66c2;color:#0a66c2}
input{width:100%;padding:.6rem .7rem;border:1px solid #ccc;border-radius:6px;margin:.4rem 0}
</style></head>
<body><div class="container">
  <h2>Create account</h2>
  {% if error %}<div class="muted" style="color:#b32d2e">{{ error }}</div>{% endif %}
  <form method="post">
    <input type="email" name="email" placeholder="you@company.com" required>
    <input type="password" name="password" placeholder="Password" required>
    <input type="password" name="confirm" placeholder="Confirm password" required>
    <button class="btn primary" type="submit">Register</button>
  </form>
  <p class="muted">Already have an account? <a href="/account/login">Sign in</a></p>
</div></body></html>
'

saved_tpl='<!doctype html>
<html><head><meta charset="utf-8"><title>My saved</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:1rem}
.wrap{display:grid;grid-template-columns:260px 1fr;gap:1rem}
aside{border:1px solid #eee;border-radius:12px;padding:1rem}
.cards{list-style:none;padding:0;display:grid;gap:.75rem}
.card{border:1px solid #eee;border-radius:12px;padding:1rem}
.btn{background:#fff;border:1px solid #ccc;border-radius:6px;padding:.35rem .6rem;cursor:pointer}
.btn.small{font-size:.85rem;padding:.25rem .5rem}
</style></head>
<body>
  <div class="wrap">
    <aside>
      <h3>Folders</h3>
      <ul style="list-style:none;padding:0">
        <li><a href="/saved">All saved</a></li>
        {% for f in folders %}
          <li><a href="/saved?folder_id={{ f.id }}">{{ f.name }}</a></li>
        {% endfor %}
      </ul>
      <form method="post" action="/account/logout"><button class="btn small">Logout</button></form>
    </aside>
    <main>
      <h2>Saved items{% if active_folder %} — folder {{ active_folder }}{% endif %}</h2>
      <ul class="cards">
        {% for it in items %}
          <li class="card">
            <div><a href="{{ it.link }}" target="_blank" rel="noopener">{{ it.title }}</a></div>
            <div class="muted">{{ it.published_at }}{% if it.folder %} · Folder: {{ it.folder }}{% endif %}</div>
            <div style="margin-top:.5rem;">
              <button class="btn small" onclick="fetch('/api/save/{{ it.guid }}',{method:'DELETE'}).then(()=>location.reload())">Remove</button>
            </div>
          </li>
        {% else %}
          <li class="card">No saved items yet.</li>
        {% endfor %}
      </ul>
    </main>
  </div>
</body></html>
'

printf "%s" "$login_tpl"    > summariser/templates/summariser/login.html
printf "%s" "$register_tpl" > summariser/templates/summariser/register.html
printf "%s" "$saved_tpl"    > summariser/templates/summariser/saved.html

# --- 5) summaries.html: add Save button + Save modal + JS -------------------
sum="summariser/templates/summariser/summaries.html"

# Add Save button before AI button (if not already present)
if ! grep -q 'class="btn save-btn"' "$sum"; then
  sed -i.bak 's/<button type="button" class="btn ai-btn"/<button type="button" class="btn save-btn" data-guid="{{ e.guid or e.link }}">Save<\/button>\n                <button type="button" class="btn ai-btn"/' "$sum"
fi

# Add Save modal block near end (before closing </body>) if missing
if ! grep -q 'id="saveModal"' "$sum"; then
  cat >> "$sum" <<'EOF'

  <!-- Save Modal -->
  <div id="saveModal" class="modal-backdrop" aria-hidden="true">
    <div class="modal" role="dialog" aria-modal="true" aria-labelledby="saveTitle">
      <h3 id="saveTitle">Save to folder</h3>
      <div class="muted" style="margin-bottom:.5rem;">Choose a folder or create one.</div>
      <select id="folderSelect" style="width:100%;margin:.25rem 0;"></select>
      <div class="row" style="margin-top:.5rem;">
        <input id="newFolderName" placeholder="New folder name" style="flex:1;">
        <button class="btn" id="createFolderBtn" type="button">Create</button>
      </div>
      <div class="actions">
        <button type="button" class="btn" id="saveCancel">Cancel</button>
        <button type="button" class="btn primary" id="saveConfirm">Save</button>
      </div>
    </div>
  </div>
EOF
fi

# Add JS wiring (separate <script> to avoid touching your existing block)
if ! grep -q 'openSaveModal' "$sum"; then
  cat >> "$sum" <<'EOF'

  <script>
  (function(){
    let pendingGuid = null;

    function openSaveModal(guid){
      pendingGuid = guid;
      const modal = document.getElementById('saveModal');
      modal.style.display = 'flex';
      modal.setAttribute('aria-hidden','false');
      loadFolders();
    }
    function closeSaveModal(){
      pendingGuid = null;
      const modal = document.getElementById('saveModal');
      modal.style.display = 'none';
      modal.setAttribute('aria-hidden','true');
    }
    async function loadFolders(){
      const sel = document.getElementById('folderSelect');
      sel.innerHTML = '<option value="">(No folder)</option>';
      try{
        const r = await fetch('/api/folders');
        if(r.status === 401){ alert('Please sign in to save.'); closeSaveModal(); return; }
        const data = await r.json();
        (data.folders || []).forEach(f=>{
          const o=document.createElement('option'); o.value=f.id; o.textContent=f.name; sel.appendChild(o);
        });
      }catch(e){ console.error(e); }
    }
    async function createFolder(){
      const name = (document.getElementById('newFolderName').value||'').trim();
      if(!name) return;
      const r = await fetch('/api/folders', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({name})});
      if(r.ok){ document.getElementById('newFolderName').value=''; loadFolders(); }
    }
    async function saveItem(){
      if(!pendingGuid) return;
      const folder_id = document.getElementById('folderSelect').value || null;
      const r = await fetch('/api/save', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({guid: pendingGuid, folder_id: folder_id ? Number(folder_id) : null})});
      if(r.status === 401){ alert('Please sign in to save.'); return; }
      if(!r.ok){ alert('Failed to save.'); return;}
      closeSaveModal();
    }

    document.querySelectorAll('.save-btn').forEach(b=>{
      b.addEventListener('click', ()=> openSaveModal(b.getAttribute('data-guid')));
    });
    document.getElementById('createFolderBtn')?.addEventListener('click', createFolder);
    document.getElementById('saveConfirm')?.addEventListener('click', saveItem);
    document.getElementById('saveCancel')?.addEventListener('click', closeSaveModal);
    document.getElementById('saveModal')?.addEventListener('click', (e)=>{ if(e.target.id==='saveModal') closeSaveModal(); });
  })();
  </script>
EOF
fi

echo "== Update complete =="
echo "Next: pip install -r requirements.txt ; restart uvicorn ; then register/login and test Save."
