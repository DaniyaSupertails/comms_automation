"""
Comms audience export API — matches `70. comms_backend (2).ipynb` step-for-step:

1) comms_base: two-stage collapse (priority, then segment_priority + total_inventory).
2) BQ: full-table QUALIFY queries on `cx_signal_ah` for recency_score and behavior_score
   (same SQL strings as the notebook).
3) cust: `cx_identifier` LEFT JOIN `cx_profile_attributes` (customer_id, email, payment_card_issuer);
   merge onto BQ rows, then build recent_signal / behavior_signal and outer-merge signals on email.
4) base: DOA-eligible customers from cx_identifier + profile.
5) base_enriched = base.merge(comms, on="email", how="left"); fill segment NA with Generic;
   final = base_enriched.merge(signals, on="email", how="left").
6) comms_label / cohort / monthly cohort / Telle / purchase / daily_target / enrichment chain.

Configure via environment: DB_*, BQ_PRIVATE_KEY, BQ_PRIVATE_KEY_ID (optional SHOW_ERROR_DETAILS).
BQ_SIGNAL_FULL_TABLE=1 enables notebook full-table BQ pulls (default off — use eligible-scoped
chunked BQ on Render to avoid 502 from timeouts/OOM).

Performance (defaults tuned for speed): PIPELINE_PARALLEL_MYSQL=1 runs comms/cust/base on
separate DB connections; BQ_CHUNK_PARALLEL (default 4) runs combined BQ chunks concurrently;
BQ_CUSTOMER_CHUNK_SIZE (default 12000) trades memory for fewer BQ jobs. Install pyarrow +
google-cloud-bigquery-storage for faster BigQuery downloads.

BigQuery non-secret fields are defined in _BQ_SERVICE_ACCOUNT_PUBLIC below.
"""

from __future__ import annotations

import io
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Literal

import mysql.connector as sql
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from google.cloud import bigquery
from google.oauth2 import service_account
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

app = FastAPI(title="Comms CSV Export")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Export-Row-Count"],
)

# ---------------------------------------------------------------------------
# BigQuery service account (public fields only — secrets via env on Render)
# ---------------------------------------------------------------------------
_BQ_SERVICE_ACCOUNT_PUBLIC: dict[str, str] = {
    "type": "service_account",
    "project_id": "ga4-data-api-1681899023728",
    "client_email": "daniya@ga4-data-api-1681899023728.iam.gserviceaccount.com",
    "client_id": "101663420454472117352",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": (
        "https://www.googleapis.com/robot/v1/metadata/x509/"
        "daniya%40ga4-data-api-1681899023728.iam.gserviceaccount.com"
    ),
    "universe_domain": "googleapis.com",
}


def _is_truthy_env(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _bq_private_key_from_env() -> str:
    """Render often stores PEM with literal \\n — normalize to real newlines."""
    raw = (os.getenv("BQ_PRIVATE_KEY") or os.getenv("GOOGLE_PRIVATE_KEY") or "").strip()
    if not raw:
        return ""
    return raw.replace("\\n", "\n").strip()


# ---------------------------------------------------------------------------
# Request model
# ---------------------------------------------------------------------------
class AudienceRequest(BaseModel):
    event_type: Literal["sent", "delivered", "failed"]
    days: int = Field(gt=0, le=365)
    purchase_days: int = Field(gt=0, le=3650)
    daily_target: int = Field(gt=0)
    selected_templates: list[str] = []


# ---------------------------------------------------------------------------
# Constants (notebook-aligned)
# ---------------------------------------------------------------------------
RAW_SEGMENT_PRIORITY = {
    "Abandoned Product": 1,
    "Price Drop Sku": 2,
    "Recommended Product": 4,
    "Major Sku": 5,
    "Affinity Product": 6,
}

COMMS_LABEL_PRIORITY = {
    "Abandoned Product": 1,
    "Price Drop Sku": 2,
    "Recency": 3,
    "Recommended Product": 4,
    "Major Sku": 5,
    "Affinity Product": 6,
    "Behavior": 7,
    "Generic": 8,
}

COMMS_LABEL1_MAP = {
    "Abandoned Product": "abc",
    "Price Drop Sku": "pd",
    "Recommended Product": "reco",
    "Major Sku": "sku",
    "Affinity Product": "affinity",
    "Generic": "gen",
    "SUPPLEMENTS": "supplements",
    "WET FOOD": "wet_food",
    "DRY FOOD": "dry_food",
    "CLOTHING": "clothing",
    "DEWORMING": "deworming",
    "LIVER CARE": "liver",
    "TOYS": "toys",
    "BEDDING, MATS & TRAVEL SUPPLIES": "bmt",
    "DIGESTIVE CARE": "digestive",
    "LITTER": "litter",
    "COLLAR, LEASHES, HARNESSES": "clh",
    "TREATS": "treats",
    "GROOMING": "grooming",
    "SKIN CARE": "skin",
    "BOWLS & FEEDERS": "bowls",
    "TICK & FLEA": "tick",
    "JOINT CARE": "joint",
    "CARDIAC CARE": "cardiac",
    "KIDNEY CARE": "kidney",
}

COLLECTION_MAP = {
    "DRY FOOD": "collections/dry-food",
    "DEWORMING": "collections/pet-deworming-medicine",
    "SUPPLEMENTS": "collections/supplements",
    "SKIN CARE": "collections/pet-skin-care",
    "CLOTHING": "collections/clothing",
    "TICK & FLEA": "collections/tick-and-flea",
    "WET FOOD": "collections/wet-food",
    "TOYS": "collections/pet-toys",
    "GROOMING": "collections/grooming",
    "COLLAR, LEASHES, HARNESSES": "collections/collars-leashes-harnesses",
    "TREATS": "collections/pet-treats",
    "BEDDING, MATS & TRAVEL SUPPLIES": "collections/beds-and-travel-supplies",
    "BOWLS & FEEDERS": "collections/bowls-feeders",
    "JOINT CARE": "collections/joint-pain-medicine-for-dogs-cats",
    "LITTER": "collections/cat-litter",
    "KIDNEY CARE": "collections/kidney-medicine-for-dogs-cats",
    "CARDIAC CARE": "collections/cardiac-medicine-for-dogs-cats",
    "LIVER CARE": "collections/liver-medications-for-dogs-cats",
    "DIGESTIVE CARE": "collections/pet-digestive-care-medicine",
}

CAMPAIGN_MAP = {
    "abc": 1772845356,
    "affinity": 1772846825,
    "bmt": 1772846916,
    "bowls": 1772847017,
    "cardiac": 1772847155,
    "clh": 1772847252,
    "clothing": 1772847341,
    "deworming": 1772847421,
    "digestive": 1772847525,
    "dry_food": 1772847592,
    "gen": 1772847676,
    "grooming": 1772847784,
    "joint": 1772847854,
    "kidney": 1772847933,
    "litter": 1772848012,
    "liver": 1772848093,
    "pd": 1772848125,
    "reco": 1772848260,
    "skin": 1772848341,
    "sku": 1772848439,
    "supplements": 1772848533,
    "tick": 1772848614,
    "toys": 1772848696,
    "treats": 1772848764,
    "wet_food": 1772848840,
}

DISCOUNT_MAP = {
    "SAVE100": "100 OFF",
    "MISSEDYOU": "300 OFF",
    "STSECOND": "150 OFF",
    "STTHIRD": "150 OFF",
}

ALLOWED_TELLE_TEMPLATES = {
    "13dec_wa_pf_cs_v2",
    "19apr_henlo_ntb_atc_viewed_bbb",
    "19apr_henlo_pdt_viewed",
    "abc_update_v1",
    "abc_update_v5",
    "ibnod_collection_viewed_23mar",
    "ibnod_collection_viewed_23mar_blr",
    "ibnod_product_added_23mar",
    "ibnod_product_viewed_23mar",
    "pharmacy_abc_v2",
    "ucj_update_conv_v2",
    "19apr_henlo_pdt_viewed_4",
    "ibnod_pet_tag_wa1",
    "bday_gift_msg1",
    "clinic_conversion",
    "predicted_replenishment_v2",
    "ibnod_goat_deals_4_days",
    "pettag_2",
    "ibnod_product_viewed_23mar_blr",
    "clinic_lead_gen_submit_v3",
    "26march_bday_followup_clinic_v2",
    "scoopy_journey_wa1",
    "scoopy_journey_wa2",
    "scoopy_journey_wa3",
    "ibnod_welcome_23mar",
    "ibnod_product_viewed_blr_v3",
    "19apr_henlo_pdt_viewed_2",
    "19apr_henlo_pdt_viewed_3",
    "ibnod_goat_deals_4_days_v2",
    "ibnod_welcome_23mar_v2",
}

DAILY_TARGET_PRIORITY_ORDER = [
    "Abandoned Product",
    "Price Drop Sku",
    "Recency",
    "Recommended Product",
    "Major Sku",
    "Affinity Product",
    "Behavior",
    "Generic",
]

DELIVERY_COPY_MAP = {
    "same_day": "in same day",
    "30_min": "in 30 mins",
    "120_min": "in 120 mins",
    "next_day": "next day",
}

FALLBACK_PRODUCT_COHORTS = {"Recency", "Behavior", "Generic"}

IN_CHUNK_SIZE = int(os.getenv("SQL_IN_CHUNK_SIZE", "3000"))
BQ_CUSTOMER_CHUNK_SIZE = int(os.getenv("BQ_CUSTOMER_CHUNK_SIZE", "12000"))
BQ_CHUNK_PARALLEL = max(1, int(os.getenv("BQ_CHUNK_PARALLEL", "4")))


def _is_pipeline_parallel_mysql() -> bool:
    return (os.getenv("PIPELINE_PARALLEL_MYSQL", "1").strip().lower() in ("1", "true", "yes"))

# Full-table BQ pulls match the notebook but often OOM or exceed Render’s proxy timeout → 502.
# Default: eligible-scoped chunked BQ (same QUALIFY per customer_id, only for DOA customers).
# Set BQ_SIGNAL_FULL_TABLE=1 only when you need exact notebook full scans (e.g. local batch).
# ---------------------------------------------------------------------------
# Notebook `70. comms_backend (2).ipynb` — full-table signal extracts (verbatim SQL).
RECENCY_SIGNAL_SQL = """
SELECT
    customer_id,
    ah_category,
    recency_score
FROM `ga4-data-api-1681899023728.cx_signal_final.cx_signal_ah`
WHERE ah_category != 'CLINIC & AHS' AND ah_category != 'OTHERS'
  AND recency_score IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY recency_score DESC
) = 1
"""

BEHAVIOR_SIGNAL_SQL = """
SELECT
    customer_id,
    ah_category,
    behavior_score
FROM `ga4-data-api-1681899023728.cx_signal_final.cx_signal_ah`
WHERE ah_category != 'CLINIC & AHS' AND ah_category != 'OTHERS'
  AND behavior_score IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY behavior_score DESC
) = 1
"""


# ---------------------------------------------------------------------------
# Helpers: BQ ↔ MySQL customer_id and email normalization
# ---------------------------------------------------------------------------
def _normalize_customer_id(val) -> str:
    """
    BigQuery often returns numeric customer_id; str(float) becomes '12345.0' while MySQL uses '12345'.
    Without this, merge with cx_identifier fails and signals lose all emails.
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    try:
        if isinstance(val, (int, np.integer)):
            return str(int(val))
        if isinstance(val, (float, np.floating)):
            f = float(val)
            if np.isnan(f):
                return ""
            if f == int(f):
                return str(int(f))
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    if len(s) > 2 and s.endswith(".0") and s[:-2].lstrip("-").isdigit():
        return s[:-2]
    return s


def normalize_email(s):
    if pd.isna(s):
        return None
    return str(s).strip().lower()


# ---------------------------------------------------------------------------
# DB / BQ
# ---------------------------------------------------------------------------
def _resolve_db_host_and_port(raw_host: str, raw_port: str) -> tuple[str, int]:
    host = raw_host.strip()
    port = int((raw_port or "3306").strip())
    if host.count(":") == 1:
        maybe_host, maybe_port = host.rsplit(":", 1)
        if maybe_port.isdigit():
            host = maybe_host.strip()
            port = int(maybe_port)
    return host, port


def get_db_connection():
    """
    MySQL must be reachable from the Render host. Use your cloud DB hostname in DB_HOST
    (not localhost — on Render there is no MySQL on localhost unless you add a private service).
    """
    raw_host = os.getenv("DB_HOST") or ""
    raw_port = os.getenv("DB_PORT") or ""
    user = (os.getenv("DB_USER") or "").strip()
    password = os.getenv("DB_PASSWORD") or ""
    database = (os.getenv("DB_NAME") or "").strip()
    host, port = _resolve_db_host_and_port(raw_host, raw_port)
    if not host or not user or not database:
        raise HTTPException(
            status_code=503,
            detail=(
                "Database not configured: set DB_HOST, DB_USER, DB_PASSWORD, DB_NAME "
                "(and DB_PORT if needed) "
                "in Render Environment. Use your managed MySQL hostname (e.g. AWS RDS, "
                "PlanetScale, or Render Private Service), not localhost."
            ),
        )
    if host in ("localhost", "127.0.0.1", "::1"):
        logger.warning(
            "DB_HOST is %s — connections from Render web services to localhost will fail "
            "unless MySQL runs in the same container.",
            host,
        )
    try:
        connect_kwargs = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
            "connection_timeout": int((os.getenv("DB_CONNECT_TIMEOUT") or "15").strip()),
            "autocommit": True,
        }
        if _is_truthy_env("DB_SSL_DISABLED"):
            connect_kwargs["ssl_disabled"] = True
        ssl_ca = (os.getenv("DB_SSL_CA") or "").strip()
        if ssl_ca:
            connect_kwargs["ssl_ca"] = ssl_ca
        if _is_truthy_env("DB_SSL_VERIFY_CERT"):
            connect_kwargs["ssl_verify_cert"] = True
        if _is_truthy_env("DB_SSL_VERIFY_IDENTITY"):
            connect_kwargs["ssl_verify_identity"] = True
        return sql.connect(**connect_kwargs)
    except Exception as e:
        logger.exception("MySQL connection failed")
        raise HTTPException(status_code=500, detail=f"DB connection failed: {e!s}") from e


def get_bq_client():
    """
    Builds credentials from _BQ_SERVICE_ACCOUNT_PUBLIC in this file plus:
    - BQ_PRIVATE_KEY (or GOOGLE_PRIVATE_KEY): PEM including BEGIN/END lines
    - BQ_PRIVATE_KEY_ID (or GOOGLE_PRIVATE_KEY_ID): key id string
    """
    try:
        private_key = _bq_private_key_from_env()
        private_key_id = (
            os.getenv("BQ_PRIVATE_KEY_ID") or os.getenv("GOOGLE_PRIVATE_KEY_ID") or ""
        ).strip()
        if not private_key or not private_key_id:
            raise ValueError(
                "Set BQ_PRIVATE_KEY and BQ_PRIVATE_KEY_ID in the environment (Render)."
            )
        info = {
            **_BQ_SERVICE_ACCOUNT_PUBLIC,
            "private_key_id": private_key_id,
            "private_key": private_key,
        }
        creds = service_account.Credentials.from_service_account_info(info)
        project = info.get("project_id")
        return bigquery.Client(credentials=creds, project=project)
    except Exception as e:
        logger.exception("BigQuery client init failed")
        raise HTTPException(status_code=500, detail=f"BQ connection failed: {e!s}") from e


def http_detail(exc: BaseException) -> str:
    if os.getenv("SHOW_ERROR_DETAILS", "").lower() in ("1", "true", "yes"):
        return str(exc)
    return "Internal server error"


def _dependency_health_details() -> dict[str, object]:
    missing_env = [
        key
        for key in (
            "DB_HOST",
            "DB_USER",
            "DB_PASSWORD",
            "DB_NAME",
            "BQ_PRIVATE_KEY",
            "BQ_PRIVATE_KEY_ID",
        )
        if not (os.getenv(key) or "").strip()
    ]
    details: dict[str, object] = {
        "status": "ok",
        "missing_env": missing_env,
        "database": {"status": "unknown"},
        "bigquery": {"status": "unknown"},
    }

    cnx = None
    try:
        cnx = get_db_connection()
        cur = cnx.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        details["database"] = {"status": "ok"}
    except HTTPException as exc:
        details["status"] = "error"
        details["database"] = {"status": "error", "detail": str(exc.detail)}
    except Exception as exc:
        details["status"] = "error"
        details["database"] = {"status": "error", "detail": str(exc)}
    finally:
        if cnx is not None:
            cnx.close()

    try:
        bq = get_bq_client()
        list(bq.query("SELECT 1 AS ok").result(max_results=1))
        details["bigquery"] = {"status": "ok"}
    except HTTPException as exc:
        details["status"] = "error"
        details["bigquery"] = {"status": "error", "detail": str(exc.detail)}
    except Exception as exc:
        details["status"] = "error"
        details["bigquery"] = {"status": "error", "detail": str(exc)}

    if missing_env:
        details["status"] = "error"
    return details


# ---------------------------------------------------------------------------
# Step 0: Eligible base (DOA filter) — notebook cx_identifier + profile
# ---------------------------------------------------------------------------
def get_eligible_customers(cnx) -> pd.DataFrame:
    query = f"""
        SELECT id.customer_id, id.email
        FROM cx_identifier id
        LEFT JOIN cx_profile_attributes cx ON id.customer_id = cx.customer_id
        WHERE cx.customer_doa IS NOT NULL
    """
    df = pd.read_sql(query, cnx)
    df["customer_id"] = df["customer_id"].astype(str)
    df["email"] = df["email"].str.strip().str.lower()
    df = df.dropna(subset=["email"]).drop_duplicates("email").reset_index(drop=True)
    if not df["email"].is_unique:
        raise ValueError("Eligible customer emails are not unique")
    return df


# ---------------------------------------------------------------------------
# Step 1: Collapse comms_base to one row per email (notebook df)
# ---------------------------------------------------------------------------
def build_comms_collapsed(cnx) -> pd.DataFrame:
    """Notebook: SELECT * semantics; comms_base is email-keyed (no customer_id column)."""
    query = """
        SELECT email, segment, sku, total_inventory
        FROM retentionTeam.comms_base
    """
    df = pd.read_sql(query, cnx)
    df = df.drop_duplicates()
    df = df[df["email"].notna()].copy()
    df["email"] = df["email"].str.strip().str.lower()

    df["segment"] = (
        df["segment"]
        .str.strip()
        .str.replace("_", " ", regex=False)
        .str.title()
    )

    df["priority"] = df["segment"].map(RAW_SEGMENT_PRIORITY)
    unmapped = df.loc[df["priority"].isna(), "segment"].unique()
    if len(unmapped) > 0:
        raise ValueError(f"Unmapped segments in comms_base: {unmapped}")

    # STEP 1 (notebook): collapse on segment priority only
    df = (
        df.sort_values("priority")
        .drop_duplicates("email", keep="first")
        .reset_index(drop=True)
    )

    # STEP 2 (notebook): segment_priority + total_inventory, then drop segment_priority
    df["segment_priority"] = df["segment"].map(RAW_SEGMENT_PRIORITY)
    df = (
        df.sort_values(by=["segment_priority", "total_inventory"], ascending=[True, False])
        .drop_duplicates("email", keep="first")
        .drop(columns="segment_priority")
        .reset_index(drop=True)
    )
    if not df["email"].is_unique:
        raise ValueError("Email deduplication failed in comms_base")
    return df[["email", "segment", "sku", "total_inventory"]]


def load_cust_for_signals(cnx) -> pd.DataFrame:
    """Notebook `cust`: cx_identifier + profile (used to map BQ customer_id → email)."""
    query = """
        SELECT id.customer_id, id.email, cx.payment_card_issuer
        FROM cx_identifier id
        LEFT JOIN cx_profile_attributes cx ON id.customer_id = cx.customer_id
    """
    df = pd.read_sql(query, cnx)
    df["customer_id"] = df["customer_id"].astype(str)
    df = df.drop_duplicates(subset=["customer_id"], keep="first")
    return df


def _bq_query_to_dataframe(bq_client, query: str) -> pd.DataFrame:
    try:
        return bq_client.query(query).to_dataframe(create_bqstorage_client=True)
    except Exception:
        logger.info("BQ Storage API path failed; retrying default to_dataframe()")
        return bq_client.query(query).to_dataframe()


def get_signals_notebook(bq_client, cust: pd.DataFrame) -> pd.DataFrame:
    """Notebook: full BQ pulls → merge cust → recent_signal / behavior_signal → outer merge."""
    recent = _bq_query_to_dataframe(bq_client, RECENCY_SIGNAL_SQL)
    behavior = _bq_query_to_dataframe(bq_client, BEHAVIOR_SIGNAL_SQL)

    if recent.empty and behavior.empty:
        return pd.DataFrame(columns=["email", "recency_category", "behavior_category"])

    cust_sub = cust[["customer_id", "email"]].copy()
    cust_sub["customer_id"] = cust_sub["customer_id"].astype(str)

    if not recent.empty:
        recent["customer_id"] = recent["customer_id"].map(_normalize_customer_id)
        recent1 = recent.merge(
            cust_sub,
            on="customer_id",
            how="left",
            validate="one_to_one",
        )
        recent1["email"] = recent1["email"].apply(normalize_email)
        recent_signal = (
            recent1.sort_values("recency_score", ascending=False)
            .drop_duplicates("email", keep="first")
            [["email", "ah_category"]]
            .rename(columns={"ah_category": "recency_category"})
        )
    else:
        recent_signal = pd.DataFrame(columns=["email", "recency_category"])

    if not behavior.empty:
        behavior["customer_id"] = behavior["customer_id"].map(_normalize_customer_id)
        behavior1 = behavior.merge(
            cust_sub,
            on="customer_id",
            how="left",
            validate="one_to_one",
        )
        behavior1["email"] = behavior1["email"].apply(normalize_email)
        behavior_signal = (
            behavior1.sort_values("behavior_score", ascending=False)
            .drop_duplicates("email", keep="first")
            [["email", "ah_category"]]
            .rename(columns={"ah_category": "behavior_category"})
        )
    else:
        behavior_signal = pd.DataFrame(columns=["email", "behavior_category"])

    if not recent_signal.empty:
        assert recent_signal["email"].is_unique
    if not behavior_signal.empty:
        assert behavior_signal["email"].is_unique

    signals = recent_signal.merge(
        behavior_signal,
        on="email",
        how="outer",
        validate="one_to_one",
    )
    signals = signals.drop_duplicates()
    signals = signals.dropna(subset=["email"])
    assert signals["email"].notna().all()
    assert signals["email"].is_unique
    return signals


def _use_full_table_bq_signals() -> bool:
    return (os.getenv("BQ_SIGNAL_FULL_TABLE", "0").strip().lower() in ("1", "true", "yes"))


def _get_combined_signal_chunk(bq_client, customer_ids: list[str]) -> pd.DataFrame:
    """
    One BigQuery job per chunk: best recency row + best behavior row per customer_id,
    then FULL OUTER JOIN. Replaces two separate BQ round-trips per chunk (~2x faster BQ phase).
    """
    if not customer_ids:
        return pd.DataFrame(
            columns=[
                "customer_id",
                "recency_category",
                "recency_score",
                "behavior_category",
                "behavior_score",
            ]
        )
    query = """
    WITH target_customers AS (
        SELECT customer_id
        FROM UNNEST(@customer_ids) AS customer_id
    ),
    rec AS (
        SELECT
            CAST(src.customer_id AS STRING) AS customer_id,
            src.ah_category AS recency_category,
            src.recency_score,
            src.behavior_score
        FROM (
            SELECT
                CAST(src.customer_id AS STRING) AS customer_id,
                src.ah_category,
                src.recency_score,
                src.behavior_score,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(src.customer_id AS STRING)
                    ORDER BY src.recency_score DESC
                ) AS rn
            FROM `ga4-data-api-1681899023728.cx_signal_final.cx_signal_ah` src
            INNER JOIN target_customers t
                ON CAST(src.customer_id AS STRING) = t.customer_id
            WHERE src.ah_category NOT IN ('CLINIC & AHS', 'OTHERS')
              AND src.recency_score IS NOT NULL
        ) src
        WHERE src.rn = 1
    ),
    beh AS (
        SELECT
            CAST(src.customer_id AS STRING) AS customer_id,
            src.ah_category AS behavior_category,
            src.recency_score,
            src.behavior_score
        FROM (
            SELECT
                CAST(src.customer_id AS STRING) AS customer_id,
                src.ah_category,
                src.recency_score,
                src.behavior_score,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(src.customer_id AS STRING)
                    ORDER BY src.behavior_score DESC
                ) AS rn
            FROM `ga4-data-api-1681899023728.cx_signal_final.cx_signal_ah` src
            INNER JOIN target_customers t
                ON CAST(src.customer_id AS STRING) = t.customer_id
            WHERE src.ah_category NOT IN ('CLINIC & AHS', 'OTHERS')
              AND src.behavior_score IS NOT NULL
        ) src
        WHERE src.rn = 1
    )
    SELECT
        COALESCE(rec.customer_id, beh.customer_id) AS customer_id,
        rec.recency_category,
        rec.recency_score,
        beh.behavior_category,
        beh.behavior_score
    FROM rec
    FULL OUTER JOIN beh
        ON rec.customer_id = beh.customer_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("customer_ids", "STRING", customer_ids)
        ]
    )
    try:
        df = bq_client.query(query, job_config=job_config).to_dataframe(
            create_bqstorage_client=True
        )
    except Exception:
        logger.info("BQ Storage API path failed; retrying default to_dataframe()")
        df = bq_client.query(query, job_config=job_config).to_dataframe()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "customer_id",
                "recency_category",
                "recency_score",
                "behavior_category",
                "behavior_score",
            ]
        )
    df["customer_id"] = df["customer_id"].map(_normalize_customer_id)
    return df


def _build_signal_table(
    raw_df: pd.DataFrame, cust_map: pd.DataFrame, *, score_column: str, out_column: str
) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=["email", out_column])
    merged = raw_df.merge(
        cust_map[["customer_id", "email"]],
        on="customer_id",
        how="left",
        validate="many_to_one",
    )
    merged["email"] = merged["email"].apply(normalize_email)
    signal_df = (
        merged.sort_values(score_column, ascending=False)
        .drop_duplicates("email", keep="first")
        [["email", out_column]]
        .dropna(subset=["email"])
        .reset_index(drop=True)
    )
    if not signal_df["email"].is_unique:
        raise ValueError(f"{out_column} emails are not unique")
    return signal_df


def get_signals_eligible_scoped(
    bq_client, cust_map: pd.DataFrame, customer_ids: list[str]
) -> pd.DataFrame:
    """Notebook merge semantics; BQ restricted to eligible customer_ids (avoids Render 502)."""
    customer_ids = [cid for cid in map(_normalize_customer_id, customer_ids) if cid]
    if not customer_ids:
        return pd.DataFrame(columns=["email", "recency_category", "behavior_category"])

    chunks: list[list[str]] = [
        customer_ids[i : i + BQ_CUSTOMER_CHUNK_SIZE]
        for i in range(0, len(customer_ids), BQ_CUSTOMER_CHUNK_SIZE)
    ]
    logger.info(
        "BQ combined signal chunks: count=%s parallel=%s chunk_size_cap=%s",
        len(chunks),
        BQ_CHUNK_PARALLEL,
        BQ_CUSTOMER_CHUNK_SIZE,
    )

    def _fetch_combined(ch: list[str]) -> pd.DataFrame:
        return _get_combined_signal_chunk(bq_client, ch)

    if BQ_CHUNK_PARALLEL <= 1:
        combined_frames = [_fetch_combined(ch) for ch in chunks]
    else:
        with ThreadPoolExecutor(max_workers=BQ_CHUNK_PARALLEL) as ex:
            combined_frames = list(ex.map(_fetch_combined, chunks))

    combined = (
        pd.concat(combined_frames, ignore_index=True) if combined_frames else pd.DataFrame()
    )

    if combined.empty:
        recent_raw = pd.DataFrame(
            columns=["customer_id", "recency_category", "recency_score"]
        )
        behavior_raw = pd.DataFrame(
            columns=["customer_id", "behavior_category", "behavior_score"]
        )
    else:
        recent_raw = combined[
            ["customer_id", "recency_category", "recency_score"]
        ].dropna(subset=["recency_score"])
        behavior_raw = combined[
            ["customer_id", "behavior_category", "behavior_score"]
        ].dropna(subset=["behavior_score"])

    recent_signal = _build_signal_table(
        recent_raw,
        cust_map,
        score_column="recency_score",
        out_column="recency_category",
    )
    behavior_signal = _build_signal_table(
        behavior_raw,
        cust_map,
        score_column="behavior_score",
        out_column="behavior_category",
    )

    signals = recent_signal.merge(
        behavior_signal,
        on="email",
        how="outer",
        validate="one_to_one",
    )
    signals = signals.drop_duplicates().dropna(subset=["email"]).reset_index(drop=True)
    if not signals.empty and not signals["email"].is_unique:
        raise ValueError("Signals emails are not unique")
    return signals


# ---------------------------------------------------------------------------
# Step 3: Comms logic + dedupe
# ---------------------------------------------------------------------------
def apply_comms_logic(df: pd.DataFrame) -> pd.DataFrame:
    conditions = [
        df["segment"].eq("Abandoned Product"),
        df["segment"].eq("Price Drop Sku"),
        df["recency_category"].notna(),
        df["segment"].eq("Recommended Product"),
        df["segment"].eq("Major Sku"),
        df["segment"].eq("Affinity Product"),
        df["behavior_category"].notna(),
    ]
    choices = [
        "Abandoned Product",
        "Price Drop Sku",
        "Recency",
        "Recommended Product",
        "Major Sku",
        "Affinity Product",
        "Behavior",
    ]
    df = df.copy()
    df["comms_label"] = np.select(conditions, choices, default="Generic")
    df["comms_cohort"] = np.select(
        [df["comms_label"].eq("Recency"), df["comms_label"].eq("Behavior")],
        [df["recency_category"], df["behavior_category"]],
        default=df["comms_label"],
    )
    df["priority_rank"] = df["comms_label"].map(COMMS_LABEL_PRIORITY)
    if df["priority_rank"].isna().any():
        raise ValueError("Unknown comms_label in priority map")
    df = (
        df.sort_values("priority_rank")
        .drop_duplicates("email", keep="first")
        .drop(columns="priority_rank")
        .reset_index(drop=True)
    )
    if not df["email"].is_unique:
        raise ValueError("Duplicate emails after comms logic")
    return df


# ---------------------------------------------------------------------------
# Monthly cohort (notebook monthly_customer_cohorts_v2)
# ---------------------------------------------------------------------------
def enrich_monthly_customer_cohort(df: pd.DataFrame, cnx) -> pd.DataFrame:
    month_key = datetime.now().strftime("%Y%m")
    query = """
        SELECT email, cohort
        FROM retentionTeam.monthly_customer_cohorts_v2
        WHERE month = %s
    """
    coh = pd.read_sql(query, cnx, params=[month_key])
    coh = coh.drop_duplicates("email")
    coh["email"] = coh["email"].str.strip().str.lower()
    m = coh.set_index("email")["cohort"]
    out = df.copy()
    out["customer_cohort"] = out["email"].map(m).fillna("BF")
    return out


# ---------------------------------------------------------------------------
# Step 4: Telle log exclusions
# ---------------------------------------------------------------------------
def apply_comms_filter(final_df: pd.DataFrame, req: AudienceRequest, cnx) -> pd.DataFrame:
    if req.event_type == "sent":
        time_col = "sent_at"
    elif req.event_type == "delivered":
        time_col = "delivered_at"
    elif req.event_type == "failed":
        time_col = "failed_at"
    else:
        raise ValueError("Invalid event_type")

    query = f"""
        SELECT customer_id, template_name
        FROM comms_telle_logs
        WHERE {time_col} >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
    """
    logs = pd.read_sql(query, cnx, params=[req.days])
    logs["customer_id"] = logs["customer_id"].astype(str)

    if req.event_type in ("sent", "delivered"):
        disallowed = logs[~logs["template_name"].isin(ALLOWED_TELLE_TEMPLATES)]
        excluded = set(disallowed["customer_id"])
    else:
        if req.selected_templates:
            logs = logs[logs["template_name"].isin(req.selected_templates)]
        excluded = set(logs["customer_id"])

    out = final_df.copy()
    out["customer_id"] = out["customer_id"].astype(str)
    return out[~out["customer_id"].isin(excluded)]


# ---------------------------------------------------------------------------
# Step 5: Recent purchasers only (notebook semantics)
# ---------------------------------------------------------------------------
def _read_sql_in_chunks(cnx, base_query: str, ids: list[str], chunk_size: int = IN_CHUNK_SIZE):
    if not ids:
        return pd.DataFrame()
    frames = []
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        placeholders = ",".join(["%s"] * len(chunk))
        q = base_query.format(placeholders=placeholders)
        frames.append(pd.read_sql(q, cnx, params=chunk))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def apply_purchase_filter(final_df: pd.DataFrame, req: AudienceRequest, cnx) -> pd.DataFrame:
    customer_ids = final_df["customer_id"].astype(str).unique().tolist()
    base_q = """
        SELECT customer_id, MAX(created_at) AS last_purchase_date
        FROM shopifyBase.cleaned_shopify_orders_lineitems
        WHERE line_item_status = 'Valid'
          AND customer_id IN ({placeholders})
        GROUP BY customer_id
    """
    purchase_df = _read_sql_in_chunks(cnx, base_q, customer_ids)
    purchase_df["customer_id"] = purchase_df["customer_id"].astype(str)

    merged = final_df.copy()
    merged["customer_id"] = merged["customer_id"].astype(str)
    merged = merged.merge(purchase_df, on="customer_id", how="left")

    cutoff = pd.Timestamp.now(tz=None).normalize() - pd.Timedelta(days=req.purchase_days)
    filtered = merged[
        merged["last_purchase_date"].notna() & (merged["last_purchase_date"] >= cutoff)
    ].copy()
    return filtered.drop(columns=["last_purchase_date"])


# ---------------------------------------------------------------------------
# Step 6: Daily cap by comms_label priority
# ---------------------------------------------------------------------------
def apply_daily_target(df: pd.DataFrame, req: AudienceRequest) -> pd.DataFrame:
    selected = []
    remaining = req.daily_target
    for cohort in DAILY_TARGET_PRIORITY_ORDER:
        cohort_df = df[df["comms_label"] == cohort]
        if cohort_df.empty:
            continue
        take_n = min(len(cohort_df), remaining)
        selected.append(cohort_df.head(take_n))
        remaining -= take_n
        if remaining == 0:
            break
    out = (
        pd.concat(selected, ignore_index=True)
        if selected
        else pd.DataFrame(columns=df.columns)
    )
    if len(out) > req.daily_target:
        raise ValueError("Daily target exceeded")
    return out


# ---------------------------------------------------------------------------
# Campaign key resolution (notebook COMMS_LABEL1_MAP)
# ---------------------------------------------------------------------------
def resolve_comms_cohort1(comms_cohort) -> str:
    if pd.isna(comms_cohort):
        return "gen"
    key = str(comms_cohort).strip()
    if not key:
        return "gen"
    mapped = COMMS_LABEL1_MAP.get(key)
    if mapped:
        return mapped
    return "gen"


def assign_comms_cohort1(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["comms_cohort1"] = out["comms_cohort"].apply(resolve_comms_cohort1)
    return out


# ---------------------------------------------------------------------------
# Step 7: Enrichment chain (notebook order)
# ---------------------------------------------------------------------------
def enrich_product_urls(df: pd.DataFrame, cnx) -> pd.DataFrame:
    final_df = df.copy()
    sku_list = final_df["sku"].dropna().unique().tolist()

    if sku_list:
        placeholders = ",".join(["%s"] * len(sku_list))
        query = f"""
            SELECT v.sku, p.title, p.handle
            FROM shopifyBase.shopify_products p
            LEFT JOIN shopifyBase.shopify_productsVariants v
                ON p.product_id = v.product_id
            WHERE v.sku IN ({placeholders})
        """
        pmap = pd.read_sql(query, cnx, params=sku_list).drop_duplicates("sku")
        final_df = final_df.merge(pmap, on="sku", how="left")
        final_df["product_name"] = final_df["title"].fillna("your pet's favourite")
        final_df = final_df.drop(columns=["title"])
    else:
        final_df["product_name"] = "your pet's favourite"
        final_df["handle"] = np.nan

    final_df["product_name"] = final_df["product_name"].fillna("your pet's favourite")
    final_df.loc[final_df["comms_label"].isin(FALLBACK_PRODUCT_COHORTS), "product_name"] = (
        "your pet's favourite"
    )

    base_url = "https://supertails.com/products/"
    if "handle" in final_df.columns:
        final_df["handle"] = np.where(
            final_df["handle"].notna(),
            base_url + final_df["handle"].astype(str).str.strip(),
            np.nan,
        )

    final_df["comms_cohort_clean"] = (
        final_df["comms_cohort"].astype(str).str.strip().str.upper()
    )
    final_df["handle"] = final_df["handle"].fillna(
        final_df["comms_cohort_clean"].map(COLLECTION_MAP)
    )
    final_df["handle"] = final_df["handle"].fillna("https://supertails.com/")
    final_df["handle"] = final_df["handle"].apply(
        lambda x: x if str(x).startswith("http") else f"https://supertails.com/{x}"
    )
    return final_df.drop(columns=["comms_cohort_clean"], errors="ignore")


def enrich_shopify_coupon_tags(df: pd.DataFrame, cnx) -> pd.DataFrame:
    final_df = df.copy()
    customer_ids = final_df["customer_id"].astype(str).unique().tolist()
    if not customer_ids:
        final_df["coupon"] = "SAVE100"
        return final_df
    placeholders = ",".join(["%s"] * len(customer_ids))
    tags_q = f"""
        SELECT customer_id, tags
        FROM shopifyBase.shopify_customers
        WHERE customer_id IN ({placeholders})
    """
    tags_df = pd.read_sql(tags_q, cnx, params=customer_ids)
    tags_df["customer_id"] = tags_df["customer_id"].astype(str)
    tags_df["coupon"] = np.select(
        [
            tags_df["tags"].str.contains("Order_Count:1", na=False),
            tags_df["tags"].str.contains("Order_Count:2", na=False),
            tags_df["tags"].str.contains("LOD:90Days", na=False),
        ],
        ["STSECOND", "STTHIRD", "MISSEDYOU"],
        default="SAVE100",
    )
    final_df["customer_id"] = final_df["customer_id"].astype(str)
    return final_df.merge(tags_df[["customer_id", "coupon"]], on="customer_id", how="left")


def enrich_cx_customer_bundle(df: pd.DataFrame, cnx) -> pd.DataFrame:
    """Single MySQL query: phone, delivery copy, pet name, card issuer (replaces 4 round-trips)."""
    final_df = df.copy()
    customer_ids = final_df["customer_id"].astype(str).unique().tolist()
    if not customer_ids:
        final_df["final_phone"] = np.nan
        final_df["copy_3"] = "at your doorstep"
        final_df["payment_card_issuer"] = np.nan
        final_df["pet_name"] = "your pet"
        return final_df

    placeholders = ",".join(["%s"] * len(customer_ids))
    query = f"""
        SELECT
            id.customer_id,
            COALESCE(id.clevertap_phone, id.phone) AS final_phone,
            id.delivery_flag,
            cx.payment_card_issuer,
            pp.pet_name
        FROM cx_identifier id
        LEFT JOIN cx_profile_attributes cx ON id.customer_id = cx.customer_id
        LEFT JOIN cx_pet_profile pp
            ON id.customer_id = pp.customer_id AND pp.pet_number = 1
        WHERE id.customer_id IN ({placeholders})
    """
    meta = pd.read_sql(query, cnx, params=customer_ids)
    meta["customer_id"] = meta["customer_id"].astype(str)
    out = final_df.merge(meta, on="customer_id", how="left")
    out["pet_name"] = out["pet_name"].fillna("your pet")

    valid = out["delivery_flag"].isin(["120_min", "30_min", "same_day", "next_day"])
    out["copy_3"] = np.where(
        valid & out["delivery_flag"].notna(),
        out["delivery_flag"].map(DELIVERY_COPY_MAP),
        "at your doorstep",
    )
    return out.drop(columns=["delivery_flag"], errors="ignore")


def _apply_coupon_discount_bank_copy_2(final_df: pd.DataFrame) -> pd.DataFrame:
    final_df["coupon"] = final_df["coupon"].fillna("SAVE100")
    final_df["coupon_discount"] = final_df["coupon"].map(DISCOUNT_MAP).fillna("100 OFF")

    issuer = final_df.get("payment_card_issuer", pd.Series("", index=final_df.index)).fillna("")
    conditions = [
        issuer.str.contains("ICIC", regex=False),
        issuer.str.contains("HDFC", regex=False),
        issuer.str.contains("UTIB", regex=False),
    ]
    choices = ["ICIC", "HDFC", "UTIB"]
    final_df["bank"] = np.select(conditions, choices, default=None)

    bank_missing = final_df["bank"].isna()
    final_df["copy_2"] = np.where(
        bank_missing,
        "*"
        + final_df["coupon"].astype(str)
        + "* & Get *"
        + final_df["coupon_discount"].astype(str)
        + "* + *Extra Bank Discounts on*",
        "*"
        + final_df["coupon"].astype(str)
        + "* & Get *"
        + final_df["coupon_discount"].astype(str)
        + "* + *Extra 5% OFF* on "
        + final_df["bank"].astype(str),
    )
    return final_df


def enrich_campaign_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = assign_comms_cohort1(df)
    out["campaign_id"] = out["comms_cohort1"].map(CAMPAIGN_MAP)
    return out


def enrich_final_payload(selected: pd.DataFrame, cnx) -> pd.DataFrame:
    """
    Notebook order: SKU/title/handle + fallbacks → comms_cohort1 + campaign_id →
    Shopify coupon tags → cx bundle (phone, delivery, pet, issuer) → copy_2.
    """
    df = selected.copy()
    df = enrich_product_urls(df, cnx)
    df = enrich_campaign_ids(df)
    df = enrich_shopify_coupon_tags(df, cnx)
    df = enrich_cx_customer_bundle(df, cnx)
    df = _apply_coupon_discount_bank_copy_2(df)
    return df


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def _preview_rows_from_df(df: pd.DataFrame, limit: int = 50) -> tuple[int, list[dict]]:
    """Row count and first N rows as JSON-serializable dicts (for /audience-preview)."""
    n = int(len(df))
    if n == 0 or df.empty:
        return 0, []
    sub = df.head(limit)
    preferred = [
        "email",
        "customer_id",
        "pet_name",
        "final_phone",
        "comms_label",
        "comms_cohort",
        "product_name",
    ]
    cols = [c for c in preferred if c in sub.columns]
    if not cols:
        cols = list(sub.columns)[:12]
    export = sub[cols]
    records = json.loads(export.to_json(orient="records", date_format="iso"))
    return n, records


def _mysql_disable_max_execution_time(cnx) -> None:
    """Notebook runs SET SESSION MAX_EXECUTION_TIME=0 before heavy reads."""
    try:
        cur = cnx.cursor()
        cur.execute("SET SESSION MAX_EXECUTION_TIME=0")
        cur.close()
    except Exception as e:
        logger.warning("Could not SET SESSION MAX_EXECUTION_TIME=0: %s", e)


def _mysql_run_isolated(fn):
    """Run fn(cnx) on a fresh connection (thread-safe; used for parallel MySQL bootstrap)."""
    cnx = get_db_connection()
    try:
        _mysql_disable_max_execution_time(cnx)
        return fn(cnx)
    finally:
        cnx.close()


def run_pipeline(req: AudienceRequest, cnx, bq) -> pd.DataFrame:
    _mysql_disable_max_execution_time(cnx)
    # Notebook order: comms_base → BQ signals + cust → DOA base → merges.
    # Default BQ path is eligible-scoped (chunked) so Render does not return 502 on full scans.
    if _use_full_table_bq_signals():
        logger.warning(
            "BQ_SIGNAL_FULL_TABLE is on: full-table BQ scans (slow; may still timeout on small workers)."
        )
        if _is_pipeline_parallel_mysql():
            with ThreadPoolExecutor(max_workers=2) as ex:
                f_comms = ex.submit(_mysql_run_isolated, build_comms_collapsed)
                f_cust = ex.submit(_mysql_run_isolated, load_cust_for_signals)
                comms, cust = f_comms.result(), f_cust.result()
        else:
            comms = build_comms_collapsed(cnx)
            cust = load_cust_for_signals(cnx)
        signals = get_signals_notebook(bq, cust)
        logger.info("pipeline signal rows (full BQ + cust merge)=%s", len(signals))
        base = get_eligible_customers(cnx)
    else:
        if _is_pipeline_parallel_mysql():
            with ThreadPoolExecutor(max_workers=3) as ex:
                f_comms = ex.submit(_mysql_run_isolated, build_comms_collapsed)
                f_cust = ex.submit(_mysql_run_isolated, load_cust_for_signals)
                f_base = ex.submit(_mysql_run_isolated, get_eligible_customers)
                comms, cust, base = f_comms.result(), f_cust.result(), f_base.result()
        else:
            comms = build_comms_collapsed(cnx)
            cust = load_cust_for_signals(cnx)
            base = get_eligible_customers(cnx)
        signals = get_signals_eligible_scoped(
            bq,
            cust,
            base["customer_id"].astype(str).unique().tolist(),
        )
        logger.info("pipeline signal rows (eligible-scoped BQ + cust merge)=%s", len(signals))
    base_enriched = base.merge(comms, on="email", how="left", validate="one_to_one")
    base_enriched["segment"] = base_enriched["segment"].fillna("Generic")

    if signals.empty:
        logger.warning("BQ signal tables returned no rows; recency/behavior will be empty.")
        df = base_enriched.copy()
        df["recency_category"] = np.nan
        df["behavior_category"] = np.nan
    else:
        df = base_enriched.merge(
            signals[["email", "recency_category", "behavior_category"]].drop_duplicates("email"),
            on="email",
            how="left",
            validate="one_to_one",
        )

    logger.info("pipeline rows after base + comms + signals=%s", len(df))

    df = apply_comms_logic(df)
    logger.info("pipeline rows after comms logic=%s", len(df))
    df = enrich_monthly_customer_cohort(df, cnx)
    logger.info("pipeline rows after monthly cohort=%s", len(df))
    df = apply_comms_filter(df, req, cnx)
    logger.info("pipeline rows after comms filter=%s", len(df))
    df = apply_purchase_filter(df, req, cnx)
    logger.info("pipeline rows after purchase filter=%s", len(df))
    df = apply_daily_target(df, req)
    logger.info("pipeline rows after daily target=%s", len(df))
    # Notebook has one enrichment path; preview uses the same logic as export (first N rows only in API).
    return enrich_final_payload(df, cnx)


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
def _path_to_index_html() -> str | None:
    """Same directory as app.py (Render layout), then static/index.html fallback."""
    base = os.path.dirname(os.path.abspath(__file__))
    for name in ("index.html", os.path.join("static", "index.html")):
        p = os.path.join(base, name)
        if os.path.isfile(p):
            return p
    return None


@app.get("/health")
def health():
    """JSON health check for Render / uptime monitors."""
    return {"status": "running"}


@app.get("/dependency-health")
def dependency_health():
    """Lightweight dependency checks so the frontend can show a real failure reason."""
    return _dependency_health_details()


@app.get("/")
def serve_ui():
    """Serve the Comms Base Builder UI (index.html next to app.py, or static/index.html)."""
    path = _path_to_index_html()
    if path:
        return FileResponse(path, media_type="text/html")
    return {
        "status": "running",
        "hint": "Place index.html next to app.py (or static/index.html). GET /health for JSON.",
    }


@app.get("/get-templates")
def get_templates(days: int = Query(7, gt=0, le=365)):
    """Distinct template names from failed sends in the last `days` days (for Failed event UI)."""
    cnx = None
    try:
        cnx = get_db_connection()
        query = """
            SELECT DISTINCT template_name
            FROM comms_telle_logs
            WHERE failed_at >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
              AND template_name IS NOT NULL
              AND TRIM(template_name) != ''
            ORDER BY template_name
        """
        df = pd.read_sql(query, cnx, params=[days])
        templates = df["template_name"].dropna().astype(str).tolist()
        return {"templates": templates}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get-templates failed")
        raise HTTPException(status_code=500, detail=http_detail(e)) from e
    finally:
        if cnx is not None:
            cnx.close()


@app.post("/audience-preview")
def audience_preview(req: AudienceRequest):
    """
    Same audience logic as export: returns total match count and the first 50 rows
    for quick QA in the builder UI.
    """
    cnx = None
    try:
        cnx = get_db_connection()
        bq = get_bq_client()
        df = run_pipeline(req, cnx, bq)
        count, rows = _preview_rows_from_df(df, limit=50)
        return {"count": count, "preview": rows}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("audience-preview failed")
        raise HTTPException(status_code=500, detail=http_detail(e)) from e
    finally:
        if cnx is not None:
            cnx.close()


@app.post("/export-csv")
def export_csv(req: AudienceRequest):
    cnx = None
    try:
        cnx = get_db_connection()
        bq = get_bq_client()
        logger.info(
            "export-csv start event_type=%s days=%s purchase_days=%s daily_target=%s",
            req.event_type,
            req.days,
            req.purchase_days,
            req.daily_target,
        )
        df = run_pipeline(req, cnx, bq)
        n = len(df)
        logger.info("export-csv finished rows=%s cols=%s", n, len(df.columns))
        if n == 0:
            logger.warning(
                "export-csv: 0 rows — common causes: purchase filter window too tight, "
                "Telle exclusions removed everyone, or daily_target cohorts empty."
            )
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=comms_output.csv",
                "X-Export-Row-Count": str(n),
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("export-csv failed")
        raise HTTPException(status_code=500, detail=http_detail(e)) from e
    finally:
        if cnx is not None:
            cnx.close()
