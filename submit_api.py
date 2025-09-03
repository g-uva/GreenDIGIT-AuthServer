from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

app = FastAPI()
security = HTTPBearer()

# Replace with your actual token
VALID_TOKEN = "mysecrettoken"

class SubmitData(BaseModel):
    field1: str
    field2: int

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != VALID_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing token"
        )

@app.get("/submit")
async def submit(
    request: Request,
    token: None = Depends(verify_token)
):
    data = await request.json()
    # Optionally, validate data with SubmitData(**data)
    return {"received": data}