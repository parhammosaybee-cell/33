import asyncio, sqlite3
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uuid

ROOT=Path(__file__).parent
DATA=ROOT/"data"
UPLOADS=DATA/"avatars"
DATA.mkdir(exist_ok=True)
UPLOADS.mkdir(parents=True,exist_ok=True)
DB=DATA/"messages.db"
app=FastAPI(title="PM Chat Public Room")
app.mount("/avatars",StaticFiles(directory=str(UPLOADS)),name="avatars")

def init_db():
    with sqlite3.connect(DB) as c:
        c.execute("""CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, text TEXT NOT NULL, created_at TEXT NOT NULL, avatar TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS profiles(username TEXT PRIMARY KEY, avatar TEXT)""")
        cols=[r[1] for r in c.execute("PRAGMA table_info(messages)").fetchall()]
        if "avatar" not in cols: c.execute("ALTER TABLE messages ADD COLUMN avatar TEXT")
def profile(username):
    with sqlite3.connect(DB) as c:
        r=c.execute("SELECT avatar FROM profiles WHERE username=?",(username,)).fetchone()
    return r[0] if r else None
def history(limit=100):
    with sqlite3.connect(DB) as c:
        c.row_factory=sqlite3.Row
        rows=c.execute("SELECT username,text,created_at,avatar FROM messages ORDER BY id DESC LIMIT ?",(limit,)).fetchall()
    return [dict(r) for r in reversed(rows)]
def save_message(username,text,created,avatar):
    with sqlite3.connect(DB) as c:
        c.execute("INSERT INTO messages(username,text,created_at,avatar) VALUES(?,?,?,?)",(username,text,created,avatar))
init_db()

class Hub:
    def __init__(self): self.clients=set(); self.lock=asyncio.Lock()
    async def connect(self,ws):
        await ws.accept()
        async with self.lock: self.clients.add(ws)
    async def remove(self,ws):
        async with self.lock: self.clients.discard(ws)
    async def broadcast(self,msg):
        async with self.lock: clients=list(self.clients)
        dead=[]
        for ws in clients:
            try: await ws.send_json(msg)
            except Exception: dead.append(ws)
        if dead:
            async with self.lock:
                for ws in dead: self.clients.discard(ws)
hub=Hub()

@app.get("/",response_class=HTMLResponse)
def home(): return (ROOT/"index.html").read_text(encoding="utf-8")
@app.get("/health")
def health(): return {"ok":True,"app":"PM Chat","room":"public"}
@app.get("/api/history")
def get_history(): return history()
@app.get("/api/profile/{username}")
def get_profile(username:str): return {"username":username,"avatar":profile(username)}
@app.post("/api/profile")
async def set_profile(username:str=Form(...), avatar:UploadFile=File(...)):
    username=username.strip()[:24]
    if not username: raise HTTPException(400,"نام کاربری لازم است")
    if avatar.content_type not in {"image/jpeg","image/png","image/webp","image/gif"}:
        raise HTTPException(400,"فقط تصویر JPG، PNG، WEBP یا GIF مجاز است")
    data=await avatar.read(3*1024*1024+1)
    if len(data)>3*1024*1024: raise HTTPException(413,"حداکثر حجم عکس ۳ مگابایت است")
    ext={"image/jpeg":".jpg","image/png":".png","image/webp":".webp","image/gif":".gif"}[avatar.content_type]
    filename=uuid.uuid4().hex+ext
    (UPLOADS/filename).write_bytes(data)
    url="/avatars/"+filename
    with sqlite3.connect(DB) as c:
        c.execute("INSERT INTO profiles(username,avatar) VALUES(?,?) ON CONFLICT(username) DO UPDATE SET avatar=excluded.avatar",(username,url))
    return {"ok":True,"username":username,"avatar":url}

@app.websocket("/ws")
async def websocket_endpoint(ws:WebSocket):
    await hub.connect(ws)
    try:
        await ws.send_json({"type":"history","messages":history()})
        while True:
            data=await ws.receive_json()
            username=str(data.get("username","کاربر")).strip()[:24] or "کاربر"
            text=str(data.get("text","")).strip()[:1000]
            if not text: continue
            created=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            avatar=profile(username)
            msg={"type":"message","username":username,"text":text,"created_at":created,"avatar":avatar}
            save_message(username,text,created,avatar)
            await hub.broadcast(msg)
    except WebSocketDisconnect: await hub.remove(ws)
    except Exception: await hub.remove(ws)
