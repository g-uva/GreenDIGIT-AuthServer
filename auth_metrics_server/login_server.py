from fastapi import FastAPI, Depends, HTTPException, status, Request, Body, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Any
from passlib.context import CryptContext
from jose import JWTError, jwt
from typing import Optional
import time
import os, json, zlib
from dotenv import load_dotenv
from metrics_store import store_metric, _col, _db, store_metrics_bulk
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from pymongo import InsertOne
from pymongo.errors import PyMongoError
from pymongo.write_concern import WriteConcern
from datetime import datetime, timezone
import traceback, uuid




load_dotenv()  # loads from .env in the current folder by default

tags_metadata = [
    {
        "name": "Auth",
        "description": "Login to obtain a JWT Bearer token. Use this token in `Authorization: Bearer <token>` on all protected endpoints.",
    },
    {
        "name": "Metrics",
        "description": "Submit and list metrics. **Requires** `Authorization: Bearer <token>`.",
    },
]

app = FastAPI(
    title="GreenDIGIT WP6.2 CIM Metrics API",
    description=(
        "API for publishing metrics.\n\n"
        "**Authentication**\n\n"
        "- Obtain a token via **POST /login** using form fields `email` and `password`. Your email must be registered beforehand. In case this does not work (wrong password/unknown), please contact goncalo.ferreira@student.uva.nl or a.tahir2@uva.nl.\n"
        "- Then include `Authorization: Bearer <token>` on all protected requests.\n"
        "- Tokens expire after 1 day—in which case you must simply repeat the process again.\n"
    ),
    version="1.0.0",
    openapi_tags=tags_metadata,
    swagger_ui_parameters={"persistAuthorization": True},
    root_path="/gd-cim-api"
)
app.mount("/static", StaticFiles(directory="static"), name="static")
security = HTTPBearer()

# Secret key for JWT
SECRET_KEY = os.environ["JWT_GEN_SEED_TOKEN"]
if not SECRET_KEY:
    raise RuntimeError("JWT_GEN_SEED_TOKEN not valid. You must generate a valid token on the server. :)")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_SECONDS = 86400 # 1 day
JWT_ISSUER = os.environ.get("JWT_ISSUER", "greendigit-login-uva")
BULK_MAX_OPS = int(os.getenv("BULK_MAX_OPS", "1000"))

# SQLite setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./users.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)

Base.metadata.create_all(bind=engine)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class SubmitData(BaseModel):
    field1: str
    field2: int

class GetTokenRequest(BaseModel):
    email: str
    password: str

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def load_allowed_emails():
    path = os.path.join(os.path.dirname(__file__), "allowed_emails.txt")
    if not os.path.exists(path):
        return set()
    with open(path, "r") as f:
        return set(line.strip().lower() for line in f if line.strip())

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)):
    token = credentials.credentials
    try:
        payload = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[ALGORITHM],
            options={"require": ["sub", "exp", "iat", "nbf", "iss"]},
            issuer=JWT_ISSUER
        )
        email: str = payload.get("sub")
        if email is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        user = db.query(User).filter(User.email == email).first()
        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")
        return email
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.middleware("http")
async def catch_all_errors(request: Request, call_next):
    req_id = str(uuid.uuid4())[:8]
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        # Log full traceback to stdout (docker logs / journalctl)
        print(f"[ERR {req_id}] {request.method} {request.url}\n{tb}", flush=True)
        # Return JSON instead of plain text
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(e).__name__}: {e}", "req_id": req_id}
        )

@app.post(
    "/login",
    tags=["Auth"],
    summary="Login and get a JWT access token",
    description=(
        "Use form fields `username` (email) and `password`.\n\n"
        "Returns a JWT for `Authorization: Bearer <token>`."
    ),
    response_class=HTMLResponse
)
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    email_lower = form_data.username.strip().lower()
    user = db.query(User).filter(User.email == email_lower).first()
    if not user:
        # First login: check if allowed, then register
        allowed_emails = load_allowed_emails()
        if email_lower not in allowed_emails:
            raise HTTPException(status_code=403, detail="Email not allowed")
        hashed_password = pwd_context.hash(form_data.password)
        db_user = User(email=email_lower, hashed_password=hashed_password)
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        user = db_user
    elif not pwd_context.verify(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect password. \n If you have forgotten your password please contact the GreenDIGIT team: goncalo.ferreira@student.uva.nl.")
    now = int(time.time())
    token_data = {
        "sub": user.email,
        "iss": JWT_ISSUER,
        "iat": now,
        "nbf": now,
        "exp": now + ACCESS_TOKEN_EXPIRE_SECONDS,
    }
    token = jwt.encode(token_data, SECRET_KEY, algorithm=ALGORITHM)
    return f"""
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>API Token Generated</title>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 20px;
                }}
                
                .container {{
                    background: white;
                    padding: 40px;
                    border-radius: 12px;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                    width: 100%;
                    max-width: 600px;
                }}
                
                h2 {{
                    color: #333;
                    margin-bottom: 30px;
                    text-align: center;
                    font-size: 24px;
                    font-weight: 600;
                }}
                
                .token-section {{
                    margin-bottom: 30px;
                }}
                
                .token-label {{
                    font-weight: 600;
                    color: #333;
                    margin-bottom: 8px;
                    font-size: 14px;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                }}
                
                .token-container {{
                    position: relative;
                    background: #f8f9fa;
                    border: 2px solid #e1e5e9;
                    border-radius: 8px;
                    padding: 16px;
                    margin-bottom: 20px;
                }}
                
                .token-value {{
                    font-family: 'Courier New', monospace;
                    font-size: 14px;
                    color: #333;
                    word-break: break-all;
                    line-height: 1.5;
                    margin: 0;
                    padding-right: 50px;
                }}
                
                .copy-btn {{
                    position: absolute;
                    top: 12px;
                    right: 12px;
                    background: #667eea;
                    color: white;
                    border: none;
                    padding: 8px 12px;
                    border-radius: 6px;
                    font-size: 12px;
                    cursor: pointer;
                    transition: background-color 0.3s ease;
                }}
                
                .copy-btn:hover {{
                    background: #5a6fd8;
                }}
                
                .copy-btn.copied {{
                    background: #28a745;
                }}
                
                .success-banner {{
                    background: linear-gradient(90deg, #28a745 0%, #20c997 100%);
                    color: white;
                    padding: 16px;
                    border-radius: 8px;
                    text-align: center;
                    margin-bottom: 30px;
                    font-weight: 500;
                }}
                
                .warning {{
                    background: #fff3cd;
                    border: 1px solid #ffeaa7;
                    color: #856404;
                    padding: 16px;
                    border-radius: 8px;
                    font-size: 14px;
                    text-align: center;
                }}
                
                .back-link {{
                    display: inline-block;
                    margin-top: 20px;
                    color: #667eea;
                    text-decoration: none;
                    font-size: 14px;
                    transition: color 0.3s ease;
                }}
                
                .back-link:hover {{
                    color: #5a6fd8;
                    text-decoration: underline;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="success-banner">
                    ✓ Token Generated Successfully
                </div>
                
                <h2>Your API Token</h2>
                
                <div class="token-section">
                    <div class="token-label">Access Token</div>
                    <div class="token-container">
                        <div class="token-value" id="access-token">
                            {token}
                        </div>
                        <button class="copy-btn" onclick="copyToken('access-token', this)">Copy</button>
                    </div>
                </div>
                
                <div class="token-section">
                    <div class="token-label">Token Type</div>
                    <div class="token-container">
                        <div class="token-value" id="token-type">
                            bearer
                        </div>
                        <button class="copy-btn" onclick="copyToken('token-type', this)">Copy</button>
                    </div>
                </div>
                
                <div class="warning">
                    ⚠️ This token expires in 24 hours. Store it securely and do not share it.
                </div>
                
                <a href="login.html" class="back-link">← Generate New Token</a>
            </div>
            
            <script>
                function copyToken(elementId, button) {{
                    const tokenElement = document.getElementById(elementId);
                    const tokenText = tokenElement.textContent.trim();
                    
                    navigator.clipboard.writeText(tokenText).then(function() {{
                        button.textContent = 'Copied!';
                        button.classList.add('copied');
                        
                        setTimeout(function() {{
                            button.textContent = 'Copy';
                            button.classList.remove('copied');
                        }}, 2000);
                    }});
                }}
                
                // You can populate the actual token values like this:
                // document.getElementById('access-token').textContent = json.access_token;
                // document.getElementById('token-type').textContent = json.token_type;
            </script>
        </body>
        </html>
    """

@app.get(
    "/token-ui",
    tags=["Auth"],
    summary="Simple HTML login to manually obtain a token",
    description="Convenience page that POSTs to `/login`.",
    response_class=HTMLResponse
)
def token_ui():
    return """
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>API Token Generator</title>
            <style>
                * {
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }
                
                body {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 20px;
                }
                
                .container {
                    background: white;
                    padding: 40px;
                    border-radius: 12px;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                    width: 100%;
                    max-width: 500px;
                    display: flex;
                    flex-direction: column;
                }
                
                h2 {
                    color: #333;
                    margin-bottom: 30px;
                    text-align: center;
                    font-size: 24px;
                    font-weight: 600;
                }
                
                form {
                    margin-bottom: 30px;
                }
                
                input {
                    width: 100%;
                    padding: 12px 16px;
                    margin-bottom: 16px;
                    border: 2px solid #e1e5e9;
                    border-radius: 8px;
                    font-size: 16px;
                    transition: border-color 0.3s ease;
                }
                
                input:focus {
                    outline: none;
                    border-color: #667eea;
                }
                
                button {
                    width: 100%;
                    padding: 14px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    border: none;
                    border-radius: 8px;
                    font-size: 16px;
                    font-weight: 600;
                    cursor: pointer;
                    transition: transform 0.2s ease;
                }
                
                button:hover {
                    transform: translateY(-2px);
                }
                
                .info {
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 8px;
                    border-left: 4px solid #ffc107;
                    margin-bottom: 20px;
                }
                
                .info p {
                    color: #666;
                    font-size: 14px;
                    line-height: 1.5;
                    margin-bottom: 0;
                }
                
                .contact {
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 8px;
                    border-left: 4px solid #17a2b8;
                    margin-bottom: 20px;
                }
                
                .contact p {
                    color: #666;
                    font-size: 14px;
                    margin-bottom: 10px;
                }
                
                .contact ul {
                    list-style: none;
                    margin: 0;
                    padding: 0;
                }
                
                .contact li {
                    color: #667eea;
                    font-size: 14px;
                    margin-bottom: 5px;
                }
                
                .contact li:last-child {
                    margin-bottom: 0;
                }

                /* Footer style */
                .footer {
                    font-size: 12px;
                    color: #555;
                    text-align: center;
                    margin-top: 30px;
                    line-height: 1.5;
                }

                .footer a {
                    color: #667eea;
                    text-decoration: none;
                }

                .footer a:hover {
                    text-decoration: underline;
                }

                .footer-logos {
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    gap: 20px;
                    margin-top: 15px;
                }

                .footer-logos img {
                    max-height: 50px;
                    object-fit: contain;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>GreenDIGIT WP6 CIM API Token</h1>
                <h2>Login to generate token</h2>
                <form action="login" method="post">
                    <input name="username" type="email" placeholder="Email" required>
                    <input name="password" type="password" placeholder="Password" required>
                    <button type="submit">Get Token</button>
                </form>
                
                <div class="info">
                    <p>The token is only valid for 1 day. You must regenerate in order to access.</p>
                </div>
                
                <div class="contact">
                    <p>If you have problems logging in, please contact:</p>
                    <ul>
                        <li>goncalo.ferreira@student.uva.nl</li>
                        <li>a.tahir2@uva.nl</li>
                    </ul>
                </div>

                <div class="footer">
                    This work is funded from the European Union’s Horizon Europe research and innovation programme through the 
                    <a href="https://greendigit-project.eu/" target="_blank">GreenDIGIT project</a>, under the grant agreement No. 
                    <a href="https://cordis.europa.eu/project/id/101131207" target="_blank">101131207</a>.
                    
                    <div class="footer-logos">
                        <img src="/static/cropped-GD_logo.png" alt="GreenDIGIT logo">
                        <img src="/static/EN-Funded-by-the-EU-POS-2.png" alt="Funded by the EU">
                    </div>
                </div>
            </div>
        </body>
        </html>
    """


@app.post(
    "/submit",
    tags=["Metrics"],
    summary="Submit a metrics JSON payload",
    description=(
        "Stores an arbitrary JSON document as a metric entry.\n\n"
        "**Requires:** `Authorization: Bearer <token>`.\n\n"
        "The `publisher_email` is derived from the token’s `sub` claim."
    ),
    responses={
        200: {"description": "Stored successfully"},
        400: {"description": "Invalid JSON body"},
        401: {"description": "Missing/invalid Bearer token"},
        500: {"description": "Database error"},
    },
)
async def submit(
    request: Request,
    publisher_email: str = Depends(verify_token),
    _example: Any = Body(
        default=None,
        examples={
            "sample": {
                "summary": "Example metric payload",
                "value": {
                    "cpu_watts": 11.2,
                    "mem_bytes": 734003200,
                    "labels": {"node": "compute-0", "job_id": "abc123"}
                },
            }
        },
    ),
):
    body = await request.json()
    ack = store_metric(publisher_email=publisher_email, body=body)
    if not ack.get("ok"):
        raise HTTPException(status_code=500, detail=f"DB error: {ack.get('error')}")
    return {"stored": ack}

@app.get(
    "/metrics/me",
    tags=["Metrics"],
    summary="List my published metrics",
    description=(
        "Returns all metrics published by the authenticated user.\n\n"
        "**Requires:** `Authorization: Bearer <token>`."
    ),
    responses={
        200: {"description": "List of metrics"},
        401: {"description": "Missing/invalid Bearer token"},
    },
)
def get_my_metrics(publisher_email: str = Depends(verify_token)):
    # Query all documents for this publisher
    docs = list(_col.find({"publisher_email": publisher_email}).sort("timestamp", -1))
    # Convert ObjectId and datetime to strings
    for d in docs:
        d["_id"] = str(d["_id"])
        if "timestamp" in d and not isinstance(d["timestamp"], str):
            d["timestamp"] = str(d["timestamp"])
    return docs


class PasswordResetRequest(BaseModel):
    new_password: str

@app.post("/reset-password", tags=["Auth"], summary="Reset my password")
def reset_password(
    data: PasswordResetRequest,
    publisher_email: str = Depends(verify_token),
    db: Session = Depends(get_db)
):
    """
    Reset the password for the currently logged-in user.
    Requires a valid Authorization: Bearer <token>.
    """
    user = db.query(User).filter(User.email == publisher_email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.hashed_password = pwd_context.hash(data.new_password)
    db.commit()
    return {"msg": "Password updated successfully"}

# @app.post(
#     "/submit/ndjson",
#     tags=["Metrics"],
#     summary="Stream NDJSON (optionally gzip) with safe bulk flush",
#     description="Ingests newline-delimited JSON via streaming; supports gzip; idempotent with Idempotency-Key + X-Batch-Seq.",
#     responses={
#         200: {"description": "OK; returns count of inserted lines"},
#         400: {"description": "Bad headers or body (content-type, JSON, UTF-8, gzip)"},
#         401: {"description": "Missing/invalid Bearer token"},
#         500: {"description": "Server/database error (rolled back for safe retry)"},
#     },
# )
# async def submit_ndjson(request: Request, publisher_email: str = Depends(verify_token)):
#     # Basic header checks (helpful when tools mis-set content-type)
#     ctype = (request.headers.get("Content-Type") or "").split(";")[0].strip().lower()
#     if ctype != "application/x-ndjson":
#         raise HTTPException(status_code=400, detail=f"Content-Type must be application/x-ndjson, got {ctype or '<missing>'}")

#     content_encoding = (request.headers.get("Content-Encoding") or "").lower()
#     try:
#         decoder = zlib.decompressobj(16 + zlib.MAX_WBITS) if content_encoding == "gzip" else None
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Invalid gzip stream: {e}")

#     # ---- Idempotency (only if both headers present) ----
#     idem = request.headers.get("Idempotency-Key")
#     seq_hdr = request.headers.get("X-Batch-Seq")
#     seq_int, sess = None, None
#     if idem is not None and seq_hdr is not None:
#         try:
#             seq_int = int(seq_hdr)
#         except ValueError:
#             raise HTTPException(status_code=400, detail="X-Batch-Seq must be an integer")

#         sess = _db["ingest_sessions"]
#         try:
#             sess.insert_one({
#                 "publisher_email": publisher_email,
#                 "idempotency_key": idem,
#                 "seq": seq_int,
#                 "status": "in_progress",
#             })
#             # fresh session => proceed to stream & insert
#         except Exception as e:
#             if "E11000" in str(e):
#                 # Already have a session row for this (pub, key, seq)
#                 existing = sess.find_one(
#                     {"publisher_email": publisher_email, "idempotency_key": idem, "seq": seq_int},
#                     {"status": 1, "_id": 0}
#                 )
#                 if existing and existing.get("status") == "done":
#                     # It really was finished earlier → just report duplicate
#                     return {"ok": True, "inserted": 0, "duplicate": True, "next_expected_seq": seq_int + 1}
#                 # else: it was "in_progress" (crashed earlier) → **resume**:
#                 # do NOT return; fall through and actually re-run the streaming ingest below,
#                 # then mark status="done" at the end.
#             else:
#                 raise

#     # ---- Streaming ingest with bulk flush ----
#     col = _db.get_collection(_col.name, write_concern=WriteConcern(w="majority", j=True))
#     buf = b""
#     ops, inserted, line_no = [], 0, 0

#     def _append_line(line_bytes: bytes):
#         nonlocal ops, inserted, line_no
#         line_no += 1
#         if not line_bytes.strip():
#             return
#         try:
#             body = json.loads(line_bytes)
#         except UnicodeDecodeError as e:
#             raise HTTPException(status_code=400, detail=f"Invalid UTF-8 at line {line_no}: {e}")
#         except json.JSONDecodeError as e:
#             raise HTTPException(status_code=400, detail=f"Invalid JSON at line {line_no}: {e.msg}")
#         ops.append(InsertOne({
#             "timestamp": datetime.now(timezone.utc).isoformat(),
#             "publisher_email": publisher_email,
#             "body": body
#         }))
#         if len(ops) >= BULK_MAX_OPS:
#             col.bulk_write(ops, ordered=False, bypass_document_validation=True)
#             inserted += len(ops)
#             ops = []

#     try:
#         async for chunk in request.stream():
#             if decoder:
#                 chunk = decoder.decompress(chunk)
#             buf += chunk
#             *lines, buf = buf.split(b"\n")
#             for line in lines:
#                 _append_line(line)

#         # flush any gzip tail and process the remainder
#         if decoder:
#             tail = decoder.flush()
#             if tail:
#                 buf += tail
#         if buf:
#             for line in filter(None, buf.split(b"\n")):
#                 _append_line(line)

#         if ops:
#             col.bulk_write(ops, ordered=False, bypass_document_validation=True)
#             inserted += len(ops)

#     except PyMongoError as e:
#         # rollback idempotency marker so client can retry
#         if ((sess is not None) is not None) and (seq_int is not None):
#             sess.delete_one({"publisher_email": publisher_email, "idempotency_key": idem, "seq": seq_int})
#         raise HTTPException(status_code=500, detail=f"Mongo bulk_write error after {inserted} inserts (line {line_no}): {e}")
#     except HTTPException:
#         # re-raise structured 4xx errors from parsing
#         if ((sess is not None) is not None) and (seq_int is not None):
#             sess.delete_one({"publisher_email": publisher_email, "idempotency_key": idem, "seq": seq_int})
#         raise
#     except Exception as e:
#         # catch-all: always return JSON (not a blank 500)
#         if ((sess is not None) is not None) and (seq_int is not None):
#             sess.delete_one({"publisher_email": publisher_email, "idempotency_key": idem, "seq": seq_int})
#         raise HTTPException(status_code=500, detail=f"Unhandled error at line {line_no}: {type(e).__name__}: {e}")

#     # ---- Finalise idempotency session ----
#     if ((sess is not None) is not None) and (seq_int is not None):
#         sess.update_one(
#             {"publisher_email": publisher_email, "idempotency_key": idem, "seq": seq_int},
#             {"$set": {"status": "done", "inserted": inserted, "ts_done": datetime.now(timezone.utc).isoformat()}}
#         )
#         return {"ok": True, "inserted": inserted, "next_expected_seq": seq_int + 1}

#     return {"ok": True, "inserted": inserted}


# @app.post(
#     "/submit/batch",
#     summary="Bulk insert (JSON array) with idempotency",
#     description=(
#         "Accepts a **JSON array** of metric objects and performs a single Mongo bulk write.\n\n"
#         "Headers:\n"
#         "- **Authorization**: `Bearer <token>` (required)\n"
#         "- **Content-Type**: `application/json` (required)\n"
#         "- **Idempotency-Key**: UUID for the **array batch** (required)\n"
#         "- **X-Batch-Seq**: integer sequence for this batch (required)\n\n"
#         "Safeguards:\n"
#         "- Writes use **w=majority, j=true** for durability.\n"
#         "- Unique key on `(publisher_email, idempotency_key, seq)` prevents duplicates; safe to retry the same request.\n\n"
#         "Request example:\n"
#         "```http\n"
#         "POST /gd-cim-api/submit/batch\n"
#         "Authorization: Bearer <jwt>\n"
#         "Content-Type: application/json\n"
#         "Idempotency-Key: 11111111-1111-1111-1111-111111111111\n"
#         "X-Batch-Seq: 0\n"
#         "\n"
#         "[\n"
#         "  {\"metric\":\"cpu.util\",\"value\":0.73,\"ts\":\"2025-09-01T10:02:03Z\",\"node\":\"compute-0\"},\n"
#         "  {\"metric\":\"mem.used\",\"value\":2154,\"ts\":\"2025-09-01T10:02:04Z\",\"node\":\"compute-0\"}\n"
#         "]\n"
#         "```\n"
#         "Shell example:\n"
#         "```bash\n"
#         "curl -X POST $URL/gd-cim-api/submit/batch \\\n"
#         "  -H \"Authorization: Bearer $TOKEN\" \\\n"
#         "  -H \"Content-Type: application/json\" \\\n"
#         "  -H \"Idempotency-Key: $IDEM\" \\\n"
#         "  -H \"X-Batch-Seq: 0\" \\\n"
#         "  --data-binary @input.json\n"
#         "```\n"
#     ),
#     responses={
#         200: {"description": "OK; returns number inserted and next seq"},
#         400: {"description": "Missing/invalid headers"},
#         401: {"description": "Missing/invalid Bearer token"},
#         422: {"description": "Body was not a JSON array"},
#         500: {"description": "Database error"},
#     },
# )
# async def submit_batch(
#     request: Request,
#     body = Body(...),  # must be a JSON array
#     publisher_email: str = Depends(verify_token),
# ):
#     # Idempotency headers
#     idem = request.headers.get("Idempotency-Key")
#     seq  = request.headers.get("X-Batch-Seq")
    
#     if not idem or seq is None:
#         raise HTTPException(status_code=400, detail="Missing Idempotency-Key or X-Batch-Seq")

#     try:
#         seq_int = int(seq)
#     except ValueError:
#         raise HTTPException(status_code=400, detail="X-Batch-Seq must be an integer")

#     if not isinstance(body, list):
#         raise HTTPException(status_code=422, detail="Body must be a JSON array of objects")

#     # 1) try to register this (publisher,idempotency_key,seq)
#     sess = _db["ingest_sessions"] if (idem and seq is not None) else None
#     if (sess is not None) is not None:
#         seq = int(seq)
#         try:
#             sess.insert_one({
#                 "publisher_email": publisher_email,
#                 "idempotency_key": idem,
#                 "seq": seq_int,
#                 "status": "in_progress",
#             })
#         except Exception as e:
#             if "E11000" in str(e):
#                 return {"ok": True, "inserted": 0, "duplicate": True, "next_expected_seq": seq_int + 1}
#             raise

#     # 2) bulk insert
#     r = store_metrics_bulk(publisher_email, body)

#     # 3) finalise
#     if r.get("ok"):
#         sess.update_one(
#             {"publisher_email": publisher_email, "idempotency_key": idem, "seq": seq_int},
#             {"$set": {"status": "done", "inserted": r["inserted"]}}
#         )
#         return {"ok": True, "inserted": r["inserted"], "next_expected_seq": seq_int + 1}
#     else:
#         # roll back the session marker so you can retry
#         sess.delete_one({"publisher_email": publisher_email, "idempotency_key": idem, "seq": seq_int})
#         raise HTTPException(status_code=500, detail=r.get("error", "bulk insert failed"))


# @app.get("/ingest/status", tags=["Metrics"], summary="Report next expected seq for an idempotency key")
# def ingest_status(
#     idem_key: str = Query(..., alias="idempotency_key"),
#     publisher_email: str = Depends(verify_token),
# ):
#     sess = _db["ingest_sessions"]
#     cur = sess.find(
#         {"publisher_email": publisher_email, "idempotency_key": idem_key},
#         {"_id": 0, "seq": 1, "status": 1}
#     )
#     records = list(cur)
#     if not records:
#         return {"next_expected_seq": 0, "processed": [], "in_progress": [], "missing": []}

#     done = sorted(r["seq"] for r in records if r.get("status") == "done")
#     in_prog = sorted(r["seq"] for r in records if r.get("status") == "in_progress")

#     next_seq = (max(done) + 1) if done else (min(in_prog) if in_prog else 0)

#     # optional: gaps to help diagnose partial uploads
#     max_seq = max(done) if done else -1
#     missing = [s for s in range(max_seq + 1) if s not in set(done)]

#     return {
#         "next_expected_seq": next_seq,
#         "processed": done,
#         "in_progress": in_prog,
#         "missing": missing
#     }
    
@app.get("/verify_token", tags=["Auth"], summary=["Validate GreenDIGIT JWT based token."])
def verify_token_endpoint(email: str = Depends(verify_token)):
    return { "valid": True, "sub": email }


@app.post(
    "/get-token",
    tags=["Auth"],
    summary="Get JWT via JSON body (email and password).",
    description="Returns JSON: {access_token, token_type, expires_in}."
)
def get_token(body: GetTokenRequest, db: Session = Depends(get_db)):
    email_lower = body.email.strip().lower()
    user = db.query(User).filter(User.email == email_lower).first()
    if not user:
        allowed_emails = load_allowed_emails()
        if email_lower not in allowed_emails:
            raise HTTPException(status_code=403, detail="Email not allowed")
        hashed_password = pwd_context.hash(body.password)
        user = User(email=email_lower, hashed_password=hashed_password)
        db.add(user); db.commit(); db.refresh(user)
    elif not pwd_context.verify(body.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect password. \n If you have forgotten your password please contact the GreenDIGIT team: goncalo.ferreira@student.uva.nl.")

    now = int(time.time())
    token_data = {
        "sub": user.email,
        "iss": JWT_ISSUER,
        "iat": now,
        "nbf": now,
        "exp": now + ACCESS_TOKEN_EXPIRE_SECONDS,
    }
    token = jwt.encode(token_data, SECRET_KEY, algorithm=ALGORITHM)
    return {"access_token": token, "token_type": "bearer", "expires_in": ACCESS_TOKEN_EXPIRE_SECONDS}