from fastapi import FastAPI, Depends, HTTPException, status, Request, Body, Query, APIRouter
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import List, Dict, Any
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
from pathlib import Path




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
    title="GreenDIGIT WP6 CIM Metrics API",
    version="1.0.0",
    openapi_tags=tags_metadata,
    swagger_ui_parameters={"persistAuthorization": True},
    root_path="/gd-cim-api",
    docs_url="/v1/docs",
    openapi_url="/v1/openapi.json",
)
router = APIRouter(prefix="/v1")
prefix = app.root_path or ""
app.description = (
    "API for publishing metrics for GreenDIGIT WP6 partners (IFcA, DIRAC, and UTH).\n\n"
    "**Authentication**\n\n"
    "- Obtain a token via **POST /v1/login** using form fields `email` and `password`, "
    "or via **GET /v1/token** with query parameters `email` and `password`. "
    "Your email must be registered beforehand. If it fails (wrong password/unknown), "
    "please contact goncalo.ferreira@student.uva.nl or a.tahir2@uva.nl.\n"
    "- Then include `Authorization: Bearer <token>` on all protected requests.\n"
    "- Tokens expire after 1 day — regenerate when needed.\n\n"
    "### Funding and acknowledgements\n"
    "This work is funded from the European Union's Horizon Europe research and innovation programme "
    "through the [GreenDIGIT project](https://greendigit-project.eu/), under the grant agreement "
    "No. [101131207](https://cordis.europa.eu/project/id/101131207).\n\n"
    # GitHub badge (Markdown)
    "[![GitHub Repo](https://img.shields.io/badge/github-GreenDIGIT--AuthServer-blue?logo=github)]"
    "(https://github.com/g-uva/GreenDIGIT-AuthServer)\n\n"
    # Logos (HTML so we can size them)
    f'<p><img src="{prefix}/static/EN-Funded-by-the-EU-POS-2.png" alt="Funded by the EU" width="160"> '
    f'<img src="{prefix}/static/cropped-GD_logo.png" alt="GreenDIGIT" width="120"></p>'
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
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
    
class MetricItem(BaseModel):
    node: str
    metric: str
    value: float
    timestamp: str
    cfp_ci_service: Dict[str, Any] = Field(..., description="Embedded CI service response")

class PostCimJsonRequest(BaseModel):
    publisher_email: str
    job_id: str
    metrics: List[MetricItem]

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

@router.post(
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
                    align-items: center;
                }}
                
                h1 {{
                    text-align: center;
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

def static_url(request: Request, filename: str) -> str:
    # Prefer proxy header; fall back to ASGI root_path; finally no prefix
    prefix = request.headers.get("x-forwarded-prefix") or request.scope.get("root_path") or ""
    if prefix.endswith("/"):
        prefix = prefix[:-1]
    return f"{prefix}/static/{filename}"

@router.get(
    "/token-ui",
    tags=["Auth"],
    summary="Simple HTML login to manually obtain a token",
    description="Convenience page that POSTs to `/v1/login`.",
    response_class=HTMLResponse
)
def token_ui(request: Request):
    gd_logo = static_url(request, "cropped-GD_logo.png")
    eu_logo = static_url(request, "EN-Funded-by-the-EU-POS-2.png")

    return f"""
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>API Token Generator</title>
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
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                    background: white;
                    padding: 40px;
                    border-radius: 12px;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                    width: 100%;
                    max-width: 500px;
                }}
                
                h2 {{
                    color: #333;
                    margin-bottom: 30px;
                    text-align: center;
                    font-size: 24px;
                    font-weight: 600;
                }}
                
                form {{
                    margin-bottom: 30px;
                }}
                
                input {{
                    width: 100%;
                    padding: 12px 16px;
                    margin-bottom: 16px;
                    border: 2px solid #e1e5e9;
                    border-radius: 8px;
                    font-size: 16px;
                    transition: border-color 0.3s ease;
                }}
                
                input:focus {{
                    outline: none;
                    border-color: #667eea;
                }}
                
                button {{
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
                }}
                
                button:hover {{
                    transform: translateY(-2px);
                }}
                
                .info {{
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 8px;
                    border-left: 4px solid #ffc107;
                    margin-bottom: 20px;
                }}
                
                .info p {{
                    color: #666;
                    font-size: 14px;
                    line-height: 1.5;
                    margin-bottom: 0;
                }}
                
                .contact {{
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 8px;
                    border-left: 4px solid #17a2b8;
                    margin-bottom: 20px;
                    width: 100%;
                }}
                
                .contact p {{
                    color: #666;
                    font-size: 14px;
                    margin-bottom: 10px;
                }}
                
                .contact ul {{
                    list-style: none;
                    margin: 0;
                    padding: 0;
                }}
                
                .contact li {{
                    color: #667eea;
                    font-size: 14px;
                    margin-bottom: 5px;
                }}
                
                .contact li:last-child {{
                    margin-bottom: 0;
                }}

                /* Footer style */
                .footer {{
                    font-size: 12px;
                    color: #555;
                    text-align: center;
                    margin-top: 30px;
                    line-height: 1.5;
                }}

                .footer a {{
                    color: #667eea;
                    text-decoration: none;
                }}

                .footer a:hover {{
                    text-decoration: underline;
                }}

                .footer-logos {{
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    gap: 20px;
                    margin-top: 15px;
                }}

                .footer-logos img {{
                    max-height: 50px;
                    object-fit: contain;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>GreenDIGIT WP6 CIM API</h1>
                <h2 style="margin-top:15px;">Login to generate token</h2>
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
                    This work is funded from the European Union's Horizon Europe research and innovation programme through the 
                    <a href="https://greendigit-project.eu/" target="_blank">GreenDIGIT project</a>, under the grant agreement No. 
                    <a href="https://cordis.europa.eu/project/id/101131207" target="_blank">101131207</a>.
                    
                    <div class="footer-logos">
                        <img src="{gd_logo}" alt="GreenDIGIT logo">
                        <img src="{eu_logo}" alt="Funded by the EU">
                    </div>
                </div>
            </div>
        </body>
        </html>
    """

@router.post(
    "/submit",
    tags=["Metrics"],
    summary="Submit a metrics JSON payload",
    description=(
        "Stores an arbitrary JSON document as a metric entry.\n\n"
        "**Requires:** `Authorization: Bearer <token>`.\n\n"
        "The `publisher_email` is derived from the token's `sub` claim."
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

@router.get(
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

@router.post("/reset-password", tags=["Auth"], summary="Reset my password")
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

@router.get("/verify-token", tags=["Auth"], summary=["Validate GreenDIGIT JWT based token."])
def verify_token_endpoint(email: str = Depends(verify_token)):
    return { "valid": True, "sub": email }


@router.get(
    "/token",
    tags=["Auth"],
    summary="Get JWT via query string (email and password).",
    description="Returns JSON: {access_token, token_type, expires_in}. Accepts `email` and `password` as query parameters."
)
def get_token(
    email: str = Query(..., description="User email"),
    password: str = Query(..., description="User password"),
    db: Session = Depends(get_db)
):
    email_lower = email.strip().lower()
    user = db.query(User).filter(User.email == email_lower).first()
    if not user:
        allowed_emails = load_allowed_emails()
        if email_lower not in allowed_emails:
            raise HTTPException(status_code=403, detail="Email not allowed")
        hashed_password = pwd_context.hash(password)
        user = User(email=email_lower, hashed_password=hashed_password)
        db.add(user); db.commit(); db.refresh(user)
    elif not pwd_context.verify(password, user.hashed_password):
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

@router.post(
    "/cim-json",
    tags=["Metrics"],
    summary="Submit JSON metrics for conversion to SQL.",
    description="Converts JSON metrics with CFP calculated into namespaces to be submitted to SQL-compatible endpoint Databases."
)
def digest_cim_json(body: PostCimJsonRequest):
    # For now just print for debugging
    print("Received /cim-json submission:")
    print("Publisher:", body.publisher_email)
    print("Job ID:", body.job_id)
    for m in body.metrics:
        print(f"  - Metric {m.metric} @ {m.timestamp}: {m.value} (node={m.node})")
        print("    CFP:", m.cfp_ci_service)

    # Mock SQL mapping (to later adapt cnr_db_connect.py)
    mock_sql = [
        {
            "table": "metrics_table",
            "publisher_email": body.publisher_email,
            "job_id": body.job_id,
            "metric": m.metric,
            "value": m.value,
            "timestamp": m.timestamp,
            "cfp": m.cfp_ci_service.get("cfp_g")
        }
        for m in body.metrics
    ]

    print("Mock SQL mapping:")
    for row in mock_sql:
        print(row)

    return {"ok": True, "rows_prepared": len(mock_sql)}

app.include_router(router)
