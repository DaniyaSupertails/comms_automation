from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import pandas as pd
import mysql.connector as sql
import os
from typing import Literal

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
# REQUEST MODEL (STRICT VALIDATION)
# ---------------------------
class AudienceRequest(BaseModel):
    event_type: Literal["sent", "delivered", "failed"]
    days: int = Field(gt=0, le=365)
    purchase_days: int = Field(gt=0, le=3650)
    daily_target: int = Field(gt=0)


# ---------------------------
# DB CONNECTION (MYSQL)
# ---------------------------
def get_db_connection():
    try:
        return sql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            connection_timeout=10
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB connection failed: {str(e)}")


# ---------------------------
# BQ CONNECTION
# ---------------------------
def get_bq_connection():
    try:
        credentials_path = os.getenv("BQ_CREDENTIALS_PATH")

        if not credentials_path:
            raise Exception("BQ_CREDENTIALS_PATH not set")

        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )

        return bigquery.Client(credentials=credentials)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BQ connection failed: {str(e)}")


# ---------------------------
# MAIN ENDPOINT (SAFE)
# ---------------------------
@app.post("/get-audience")
def get_audience(req: AudienceRequest):

    cnx = None
    cursor = None

    try:
        # ---------------------------
        # DB CHECK
        # ---------------------------
        cnx = get_db_connection()
        cursor = cnx.cursor()

        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        # ---------------------------
        # RESPONSE (TEMP)
        # ---------------------------
        return {
            "status": "success",
            "db_check": result[0],
            "input": req.dict()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # ---------------------------
        # CLEANUP (CRITICAL)
        # ---------------------------
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()
