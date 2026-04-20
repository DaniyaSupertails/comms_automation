"""
Comms audience export API — logic aligned with comms_backend notebook.
Configure via environment: DB_*, BQ_PRIVATE_KEY, BQ_PRIVATE_KEY_ID (optional SHOW_ERROR_DETAILS).
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


# ---------------------------------------------------------------------------
# DB / BQ
# ---------------------------------------------------------------------------
def get_db_connection():
    """
    MySQL must be reachable from the Render host. Use your cloud DB hostname in DB_HOST
    (not localhost — on Render there is no MySQL on localhost unless you add a private service).
    """
    host = (os.getenv("DB_HOST") or "").strip()
    user = (os.getenv("DB_USER") or "").strip()
    password = os.getenv("DB_PASSWORD") or ""
    database = (os.getenv("DB_NAME") or "").strip()
    if not host or not user or not database:
        raise HTTPException(
            status_code=503,
            detail=(
                "Database not configured: set DB_HOST, DB_USER, DB_PASSWORD, and DB_NAME "
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
        return sql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            connection_timeout=10,
        )
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


# ---------------------------------------------------------------------------
# Step 0: Eligible base (DOA filter) — notebook cx_identifier + profile
# ---------------------------------------------------------------------------
def get_eligible_customers(cnx) -> pd.DataFrame:
    query = """
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
    query = """
        SELECT customer_id, email, segment, sku, total_inventory
        FROM retentionTeam.comms_base
    """
    df = pd.read_sql(query, cnx)
    df = df.drop_duplicates()
    df = df[df["email"].notna()].copy()
    df["email"] = df["email"].str.strip().str.lower()

    df["segment"] = (
        df["segment"]
        .fillna("Generic")
        .astype(str)
        .str.strip()
        .str.replace("_", " ", regex=False)
        .str.title()
    )

    df["priority"] = df["segment"].map(RAW_SEGMENT_PRIORITY)
    unmapped = df.loc[df["priority"].isna(), "segment"].unique()
    if len(unmapped) > 0:
        raise ValueError(f"Unmapped segments in comms_base: {unmapped}")

    df = (
        df.sort_values(by=["priority", "total_inventory"], ascending=[True, False])
        .drop_duplicates("email", keep="first")
        .reset_index(drop=True)
    )
    if not df["email"].is_unique:
        raise ValueError("Email deduplication failed in comms_base")
    return df[["email", "segment", "sku", "total_inventory"]]


def merge_eligible_with_comms(eligible: pd.DataFrame, comms: pd.DataFrame) -> pd.DataFrame:
    out = eligible.merge(comms, on="email", how="left", validate="one_to_one")
    out["segment"] = out["segment"].fillna("Generic")
    return out


# ---------------------------------------------------------------------------
# Step 2: Signals + customer map (BQ + MySQL)
# ---------------------------------------------------------------------------
def _normalize_customer_id(val) -> str:
    """
    BigQuery often returns numeric customer_id; str(float) becomes '12345.0' while MySQL uses '12345'.
    Without this, merge with cx_identifier fails and signals lose all emails → broken / empty audience.
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


def get_signals(bq_client):
    query = """
    WITH recency AS (
        SELECT
            customer_id,
            ah_category AS recency_category,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY recency_score DESC
            ) AS rn
        FROM `ga4-data-api-1681899023728.cx_signal_final.cx_signal_ah`
        WHERE ah_category NOT IN ('CLINIC & AHS', 'OTHERS')
          AND recency_score IS NOT NULL
    ),
    behavior AS (
        SELECT
            customer_id,
            ah_category AS behavior_category,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY behavior_score DESC
            ) AS rn
        FROM `ga4-data-api-1681899023728.cx_signal_final.cx_signal_ah`
        WHERE ah_category NOT IN ('CLINIC & AHS', 'OTHERS')
          AND behavior_score IS NOT NULL
    )
    SELECT
        COALESCE(r.customer_id, b.customer_id) AS customer_id,
        r.recency_category,
        b.behavior_category
    FROM recency r
    FULL OUTER JOIN behavior b
        ON r.customer_id = b.customer_id
    WHERE r.rn = 1 OR b.rn = 1
    """
    df = bq_client.query(query).to_dataframe()
    df["customer_id"] = df["customer_id"].map(_normalize_customer_id)
    return df


def get_customer_email_map(cnx):
    query = """
        SELECT customer_id, email
        FROM cx_identifier
    """
    df = pd.read_sql(query, cnx)
    df["customer_id"] = df["customer_id"].astype(str)
    df["email"] = df["email"].str.strip().str.lower()
    df = df.dropna(subset=["email"]).drop_duplicates("customer_id")
    return df


def _mysql_with_fresh_connection(fn):
    """Run fn(cnx) on a new connection and close it (for parallel bootstrap)."""
    cnx = get_db_connection()
    try:
        return fn(cnx)
    finally:
        cnx.close()


def _parallel_load_signals_and_mysql(bq_client):
    """
    Run BigQuery signals + three independent MySQL reads in parallel.
    Cuts wall-clock time vs sequential when DB/BQ are the bottleneck.
    """
    with ThreadPoolExecutor(max_workers=4) as ex:
        f_sig = ex.submit(get_signals, bq_client)
        f_el = ex.submit(_mysql_with_fresh_connection, get_eligible_customers)
        f_co = ex.submit(_mysql_with_fresh_connection, build_comms_collapsed)
        f_cm = ex.submit(_mysql_with_fresh_connection, get_customer_email_map)
        signals = f_sig.result()
        eligible = f_el.result()
        comms = f_co.result()
        cust_map = f_cm.result()
    return signals, eligible, comms, cust_map


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


def enrich_coupon_and_bank(df: pd.DataFrame, cnx) -> pd.DataFrame:
    final_df = df.copy()
    customer_ids = final_df["customer_id"].astype(str).unique().tolist()

    if customer_ids:
        placeholders = ",".join(["%s"] * len(customer_ids))
        tags_q = f"""
            SELECT customer_id, tags
            FROM shopifyBase.shopify_customers
            WHERE customer_id IN ({placeholders})
        """
        tags_df = pd.read_sql(tags_q, cnx, params=customer_ids)
        tags_df["coupon"] = np.select(
            [
                tags_df["tags"].str.contains("Order_Count:1", na=False),
                tags_df["tags"].str.contains("Order_Count:2", na=False),
                tags_df["tags"].str.contains("LOD:90Days", na=False),
            ],
            ["STSECOND", "STTHIRD", "MISSEDYOU"],
            default="SAVE100",
        )
        final_df = final_df.merge(tags_df[["customer_id", "coupon"]], on="customer_id", how="left")

        issuer_q = f"""
            SELECT id.customer_id, cx.payment_card_issuer
            FROM cx_identifier id
            LEFT JOIN cx_profile_attributes cx ON id.customer_id = cx.customer_id
            WHERE id.customer_id IN ({placeholders})
        """
        issuer_df = pd.read_sql(issuer_q, cnx, params=customer_ids)
        issuer_df["customer_id"] = issuer_df["customer_id"].astype(str)
        final_df = final_df.merge(issuer_df, on="customer_id", how="left")
    else:
        final_df["coupon"] = "SAVE100"

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


def enrich_delivery(df: pd.DataFrame, cnx) -> pd.DataFrame:
    final_df = df.copy()
    customer_ids = final_df["customer_id"].astype(str).unique().tolist()
    if not customer_ids:
        final_df["copy_3"] = "at your doorstep"
        return final_df

    placeholders = ",".join(["%s"] * len(customer_ids))
    query = f"""
        SELECT customer_id, delivery_flag
        FROM cx_identifier
        WHERE customer_id IN ({placeholders})
          AND delivery_flag IN ('120_min', '30_min', 'same_day', 'next_day')
    """
    edd = pd.read_sql(query, cnx, params=customer_ids)
    edd["customer_id"] = edd["customer_id"].astype(str)
    edd["copy_3"] = edd["delivery_flag"].map(DELIVERY_COPY_MAP)
    out = final_df.merge(edd[["customer_id", "copy_3"]], on="customer_id", how="left")
    out["copy_3"] = out["copy_3"].fillna("at your doorstep")
    return out


def enrich_campaign_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = assign_comms_cohort1(df)
    out["campaign_id"] = out["comms_cohort1"].map(CAMPAIGN_MAP)
    return out


def enrich_phone(df: pd.DataFrame, cnx) -> pd.DataFrame:
    customer_ids = df["customer_id"].astype(str).unique().tolist()
    if not customer_ids:
        df["final_phone"] = np.nan
        return df

    placeholders = ",".join(["%s"] * len(customer_ids))
    query = f"""
        SELECT customer_id,
               COALESCE(clevertap_phone, phone) AS final_phone
        FROM cx_identifier
        WHERE customer_id IN ({placeholders})
    """
    phones = pd.read_sql(query, cnx, params=customer_ids)
    phones["customer_id"] = phones["customer_id"].astype(str)
    return df.merge(phones, on="customer_id", how="left")


def enrich_pet_name(df: pd.DataFrame, cnx) -> pd.DataFrame:
    customer_ids = df["customer_id"].astype(str).unique().tolist()
    if not customer_ids:
        df["pet_name"] = "your pet"
        return df

    placeholders = ",".join(["%s"] * len(customer_ids))
    query = f"""
        SELECT customer_id, pet_name
        FROM cx_pet_profile
        WHERE customer_id IN ({placeholders})
          AND pet_number = 1
    """
    pets = pd.read_sql(query, cnx, params=customer_ids)
    pets["customer_id"] = pets["customer_id"].astype(str)
    out = df.merge(pets, on="customer_id", how="left")
    out["pet_name"] = out["pet_name"].fillna("your pet")
    return out


def enrich_final_payload(selected: pd.DataFrame, cnx) -> pd.DataFrame:
    df = selected.copy()
    df = enrich_product_urls(df, cnx)
    df = enrich_coupon_and_bank(df, cnx)
    df = enrich_delivery(df, cnx)
    df = enrich_campaign_ids(df)
    df = enrich_phone(df, cnx)
    df = enrich_pet_name(df, cnx)
    return df


def enrich_final_payload_light(selected: pd.DataFrame, cnx) -> pd.DataFrame:
    """
    Faster path for /audience-preview only: skips heavy SKU / tags / delivery lookups.
    Export still uses full enrich_final_payload.
    """
    df = selected.copy()
    df = enrich_campaign_ids(df)
    df = enrich_phone(df, cnx)
    df = enrich_pet_name(df, cnx)
    if "product_name" not in df.columns:
        df["product_name"] = "your pet's favourite"
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


def run_pipeline(req: AudienceRequest, cnx, bq, *, preview: bool = False) -> pd.DataFrame:
    parallel = os.getenv("PIPELINE_PARALLEL_BOOTSTRAP", "1").lower() in ("1", "true", "yes")
    if parallel:
        signals, eligible, comms, cust_map = _parallel_load_signals_and_mysql(bq)
    else:
        eligible = get_eligible_customers(cnx)
        comms = build_comms_collapsed(cnx)
        signals = get_signals(bq)
        cust_map = get_customer_email_map(cnx)

    df = merge_eligible_with_comms(eligible, comms)

    signals = signals.merge(cust_map, on="customer_id", how="left")
    signals = signals.dropna(subset=["email"]).drop_duplicates("email")

    if signals.empty:
        logger.warning(
            "No BQ signals matched cx_identifier emails after customer_id join — "
            "recency/behavior will be empty; check _normalize_customer_id / BQ data."
        )
        df = df.copy()
        df["recency_category"] = np.nan
        df["behavior_category"] = np.nan
    else:
        df = df.merge(
            signals[["email", "recency_category", "behavior_category"]],
            on="email",
            how="left",
            validate="one_to_one",
        )

    df = apply_comms_logic(df)
    df = enrich_monthly_customer_cohort(df, cnx)
    df = apply_comms_filter(df, req, cnx)
    df = apply_purchase_filter(df, req, cnx)
    df = apply_daily_target(df, req)
    if preview:
        return enrich_final_payload_light(df, cnx)
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
        df = run_pipeline(req, cnx, bq, preview=True)
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
