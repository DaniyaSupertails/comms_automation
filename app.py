from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import mysql.connector as sql
import os

from google.oauth2 import service_account
from google.cloud import bigquery

app = FastAPI()

# ---------------------------
# HEALTH CHECK
# ---------------------------
@app.get("/")
def home():
    return {"status": "running"}


# ---------------------------
# REQUEST MODEL
# ---------------------------
class AudienceRequest(BaseModel):
    event_type: str
    days: int
    purchase_days: int
    daily_target: int


# ---------------------------
# DB CONNECTION (MYSQL)
# ---------------------------
def get_db_connection():
    return sql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )


# ---------------------------
# BQ CONNECTION
# ---------------------------
def get_bq_connection():
    credentials_path = os.getenv("BQ_CREDENTIALS_PATH")

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )

    return bigquery.Client(credentials=credentials)


# ---------------------------
# MAIN ENDPOINT (TEST)
# ---------------------------
@app.post("/get-audience")
def get_audience(req: AudienceRequest):

    return {
        "message": "API working",
        "received": req.dict()
    }
