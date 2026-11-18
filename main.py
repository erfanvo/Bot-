# -*- coding: utf-8 -*-
import logging
import json
import os
import re
import csv
import uuid
import base64
import asyncio
import redis
import io
import urllib.request
import urllib.error
import ssl
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# --- لاگ‌ها ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- متغیرهای محیطی (پشتیبانی از هر دو نام متغیر در Railway) ---
TELEGRAM_BOT_TOKEN = (
    os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
    or "توکن_ربات_اسنپ_را_اینجا_بگذارید"
).strip()

allowed_users_env = os.getenv("ALLOWED_USER_IDS") or os.getenv("ADMIN_IDS") or ""
ALLOWED_USER_IDS = [
    int(x.strip()) for x in re.split(r"[,\s]+", allowed_users_env) if x.strip().isdigit()
]

REDIS_URL = os.getenv("REDIS_URL")
PORT = int(os.getenv("PORT", 8000))
REDIS_PREFIX = "snapp:license:"
PARTNER_PREFIX = "snapp:partner:"

# --- اندپوینت‌های رسمی وب‌اپلیکیشن مسافر اسنپ (مطابق لاگ شبکه PWA) ---
SNAPP_CONFIG_URL = "https://app.snapp.taxi/api/api-base/v2/passenger/config"
SNAPP_BALANCE_URL = "https://app.snapp.taxi/api/api-base/v2/passenger/balance"
SNAPP_UNSEEN_VOUCHERS_URL = "https://app.snapp.taxi/api/v1/promotion-center/unseen"
SNAPP_VOUCHER_LIST_URL = "https://app.snapp.taxi/api/v1/promotion-center/list"
SNAPP_HOMEPAGE_URL = "https://app.snapp.taxi/api/superapp/homepage/personalized/v2/content"

HTTP_EXECUTOR = ThreadPoolExecutor(max_workers=10)

# --- اتصال به ردیس ---
redis_client = None


def get_redis():
    global redis_client
    if redis_client is not None:
        try:
            redis_client.ping()
            return redis_client
        except Exception:
            redis_client = None

    if REDIS_URL:
        try:
            redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            redis_client.ping()
            logger.info("✅ اتصال به ردیس برقرار شد.")
            return redis_client
        except Exception as e:
            logger.error(f"❌ خطا در اتصال به ردیس: {e}")
    return None


get_redis()


# --- تولید لایسنس با پیشوند دقیق Snapp و کد اشتراک همکار ---
def generate_snapp_license() -> str:
    return f"Snapp-{str(uuid.uuid4())[:8].upper()}-{str(uuid.uuid4())[:8].upper()}"


def generate_partner_code() -> str:
    return f"SUB-{str(uuid.uuid4())[:6].upper()}"


def normalize_license_key(raw_key: str) -> str:
    cleaned = (raw_key or "").strip()
    if cleaned.lower().startswith("snapp-"):
        suffix = cleaned[6:].upper()
        return f"Snapp-{suffix}"
    if cleaned.lower().startswith("tapsi-"):
        suffix = cleaned[6:].upper()
        return f"TAPSI-{suffix}"
    return cleaned


def find_redis_key_for_license(raw_key: str):
    db = get_redis()
    if not db:
        return None, None
    norm_key = normalize_license_key(raw_key)
    candidates = [
        f"{REDIS_PREFIX}{norm_key}",
        f"{REDIS_PREFIX}{raw_key.strip()}",
        f"{REDIS_PREFIX}{raw_key.strip().upper()}",
    ]
    for rk in candidates:
        val = db.get(rk)
        if val:
            return rk, val
    return None, None


def get_partner_record(partner_code: str):
    db = get_redis()
    if not db or not partner_code:
        return None, None
    clean_code = partner_code.strip().upper()
    rk = f"{PARTNER_PREFIX}{clean_code}"
    raw = db.get(rk)
    if not raw:
        return None, None
    try:
        return rk, json.loads(raw)
    except Exception:
        return None, None


def fetch_all_partners() -> list:
    db = get_redis()
    if not db:
        return []
    keys = db.keys(f"{PARTNER_PREFIX}*")
    if not keys:
        return []
    partners = []
    try:
        for raw in db.mget(keys):
            if raw:
                try:
                    partners.append(json.loads(raw))
                except Exception:
                    pass
    except Exception:
        pass
    partners.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return partners


def normalize_iran_phone(raw_phone: str) -> str:
    if not raw_phone:
        return ""
    digits = re.sub(r"\D", "", str(raw_phone))
    if digits.startswith("989") and len(digits) == 12:
        return "0" + digits[2:]
    if digits.startswith("00989") and len(digits) == 14:
        return "0" + digits[4:]
    if digits.startswith("9") and len(digits) == 10:
        return "0" + digits
    if digits.startswith("09") and len(digits) == 11:
        return digits
    return ""


def extract_phone_from_local_storage(local_storage_str: str, provided_phone: str = "unknown") -> str:
    norm_provided = normalize_iran_phone(provided_phone)
    if norm_provided:
        return norm_provided

    if not local_storage_str:
        return "نامشخص"

    priority_keys = ("cellphone", "phone", "mobile", "phone_number", "phoneNumber", "username", "msisdn", "user_phone")
    try:
        parsed = json.loads(local_storage_str)
        if isinstance(parsed, dict):
            for k, v in parsed.items():
                if any(pk in str(k).lower() for pk in priority_keys):
                    candidate = normalize_iran_phone(str(v))
                    if candidate:
                        return candidate
                if isinstance(v, str) and (v.startswith("{") or v.startswith("[")):
                    try:
                        inner = json.loads(v)
                        if isinstance(inner, dict):
                            for ik, iv in inner.items():
                                if any(pk in str(ik).lower() for pk in priority_keys):
                                    candidate = normalize_iran_phone(str(iv))
                                    if candidate:
                                        return candidate
                    except Exception:
                        pass
    except Exception:
        pass

    matches = re.findall(r"(?:\+98|0098|98|0)?(9\d{9})\b", local_storage_str)
    if matches:
        return "0" + matches[0]

    return provided_phone if provided_phone and provided_phone != "unknown" else "نامشخص"


# ======================== استخراج هوشمند توکن و بررسی کوپن‌ها و امتیازات اسنپ ========================
def decode_jwt_payload(jwt_token: str) -> dict:
    """دیکد کردن بخش Payload توکن JWT برای بررسی تاریخ انقضا و مشخصات نشست."""
    try:
        parts = jwt_token.split(".")
        if len(parts) != 3:
            return {}
        payload_b64 = parts[1]
        padding = "=" * ((4 - len(payload_b64) % 4) % 4)
        raw_bytes = base64.urlsafe_b64decode(payload_b64 + padding)
        return json.loads(raw_bytes.decode("utf-8", errors="ignore"))
    except Exception:
        return {}


def extract_auth_info_from_storage(local_storage_str: str) -> dict:
    """
    استخراج هوشمند Bearer Access Token، Refresh Token، نام کاربر و امتیازهای ذخیره‌شده
    از درون رشته JSON مربوط به window.localStorage اسنپ.
    """
    info = {
        "access_token": "",
        "refresh_token": "",
        "fullname": "",
        "stored_points": None,
        "jwt_exp": 0,
        "is_jwt_expired": False,
    }
    if not local_storage_str:
        return info

    # ۱. جستجوی تمام توکن‌های JWT در کل رشته LocalStorage
    jwt_candidates = re.findall(
        r"eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}",
        local_storage_str,
    )

    best_token = ""
    best_exp = -1
    for cand in jwt_candidates:
        payload = decode_jwt_payload(cand)
        aud = payload.get("aud", [])
        tenant = payload.get("tenant_id", "")
        exp = int(payload.get("exp", 0) or 0)
        if "passenger" in str(aud) or tenant == "snapp-ir" or exp > best_exp:
            best_token = cand
            best_exp = exp

    if best_token:
        info["access_token"] = best_token
        if best_exp > 0:
            info["jwt_exp"] = best_exp
            info["is_jwt_expired"] = datetime.now().timestamp() > best_exp

    # ۲. جستجوی Refresh Token (فرمت v2.xxxx)
    rt_match = re.search(r"v2\.[A-Za-z0-9_\-+=]{20,}", local_storage_str)
    if rt_match:
        info["refresh_token"] = rt_match.group(0)

    # ۳. پیمایش ساختار JSON برای یافتن کلیدهای صریح توکن، نام یا امتیاز باشگاه
    def _walk_obj(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                kl = str(k).lower()
                if isinstance(v, str):
                    v_clean = v.strip()
                    if v_clean.lower().startswith("bearer "):
                        v_clean = v_clean[7:].strip()
                    if not info["access_token"] and kl in ("access_token", "accesstoken", "token", "id_token", "auth_token"):
                        if len(v_clean) > 25:
                            info["access_token"] = v_clean
                    elif not info["refresh_token"] and kl in ("refresh_token", "refreshtoken"):
                        info["refresh_token"] = v_clean
                    elif not info["fullname"] and kl in ("fullname", "full_name", " passenger_name"):
                        info["fullname"] = v_clean
                    elif (v_clean.startswith("{") or v_clean.startswith("[")) and len(v_clean) > 2:
                        try:
                            _walk_obj(json.loads(v_clean))
                        except Exception:
                            pass
                elif isinstance(v, (int, float)) and not isinstance(v, bool):
                    if kl in ("points", "club_points", "loyalty_points", "user_points", "score", "club_score"):
                        info["stored_points"] = int(v)
                elif isinstance(v, (dict, list)):
                    _walk_obj(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk_obj(item)

    try:
        parsed = json.loads(local_storage_str)
        _walk_obj(parsed)
    except Exception:
        pass

    return info


def _http_json_request(url: str, method: str = "GET", headers: dict = None, body_dict: dict = None, timeout: int = 10):
    """ارسال درخواست HTTP بدون نیاز به کتابخانه خارجی و بازگرداندن کد وضعیت و دیکشنری پاسخ."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "locale": "fa-IR",
    }
    if headers:
        req_headers.update(headers)

    data_bytes = None
    if body_dict is not None:
        data_bytes = json.dumps(body_dict).encode("utf-8")
        req_headers.setdefault("Content-Type", "application/json")

    req = urllib.request.Request(url, data=data_bytes, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            raw_text = resp.read().decode("utf-8", errors="replace")
            try:
                return resp.status, json.loads(raw_text)
            except Exception:
                return resp.status, {}
    except urllib.error.HTTPError as he:
        try:
            err_text = he.read().decode("utf-8", errors="replace")
            return he.code, json.loads(err_text)
        except Exception:
            return he.code, {}
    except Exception as e:
        logger.debug(f"HTTP request error for {url}: {e}")
        return 0, {}


def _format_duration_days(seconds_val) -> str:
    try:
        sec = int(seconds_val or 0)
        if sec <= 0:
            return ""
        days = sec // 86400
        if days > 0:
            return f"{days} روز"
        hours = max(1, sec // 3600)
        return f"{hours} ساعت"
    except Exception:
        return ""


def parse_voucher_list_payload(vouchers_raw: list, promo_messages_raw: list = None) -> dict:
    """
    پردازش ساختار دقیق پاسخ اندپوینت `/api/v1/promotion-center/list` اسنپ
    و تفکیک کوپن‌های سفر (تاکسی/باکس/بایک)، غذا، فروشگاه، اسنپ‌کلاب و سایر سرویس‌ها.
    """
    parsed_vouchers = []
    cab_count = 0
    food_count = 0
    shop_count = 0
    club_vouchers_count = 0

    for v in vouchers_raw or []:
        if not isinstance(v, dict):
            continue
        if not v.get("is_active", True):
            continue

        title = (v.get("title") or "").strip()
        content = (v.get("content") or "").strip()
        extra_info = (v.get("extra_info") or "").strip()
        code = (v.get("promotion_code") or "").strip()
        venture_title = (v.get("venture_title") or "سرویس اسنپ").strip()
        discount_price = int(v.get("discount_price") or 0)
        remain_uses = int(v.get("remain_use_times") or 1)
        max_uses = int(v.get("max_use_times") or 1)
        is_cab = bool(v.get("is_cab", False))
        categories = v.get("categories") or []
        v_type = int(v.get("type") or 0)

        badges = []
        expiry_badge = ""
        is_club = (10 in categories) or (v_type == 12)
        is_new = False

        for b in v.get("badges") or []:
            if isinstance(b, dict):
                b_txt = (b.get("text") or "").strip()
                if b_txt:
                    badges.append(b_txt)
                    if "قابل استفاده" in b_txt or "روز" in b_txt or "مهلت" in b_txt:
                        expiry_badge = b_txt
                    if "کلاب" in b_txt:
                        is_club = True
                    if "جدید" in b_txt:
                        is_new = True

        if not expiry_badge:
            dur_str = _format_duration_days(v.get("remain_date_duration"))
            if dur_str:
                expiry_badge = f"تا {dur_str} دیگر"

        if is_cab or 1 in categories or 9 in categories:
            cab_count += 1
        elif 2 in categories:
            food_count += 1
        elif 6 in categories or 4 in categories:
            shop_count += 1

        if is_club:
            club_vouchers_count += 1

        parsed_vouchers.append({
            "id": v.get("id"),
            "title": title,
            "content": content,
            "extra_info": extra_info,
            "promotion_code": code,
            "venture_title": venture_title,
            "discount_price": discount_price,
            "remain_use_times": remain_uses,
            "max_use_times": max_uses,
            "expiry_text": expiry_badge or "نامشخص",
            "is_cab": is_cab,
            "is_club": is_club,
            "is_new": is_new,
            "badges": badges,
        })

    parsed_promos = []
    for pm in promo_messages_raw or []:
        if not isinstance(pm, dict):
            continue
        parsed_promos.append({
            "title": (pm.get("title") or "").strip(),
            "discount": (pm.get("discount") or "").strip(),
            "usable_time": (pm.get("usableTime") or "").strip(),
            "venture": (pm.get("deep_link", {}).get("venture_name") or "").strip(),
        })

    return {
        "vouchers": parsed_vouchers,
        "total_vouchers": len(parsed_vouchers),
        "cab_vouchers_count": cab_count,
        "food_vouchers_count": food_count,
        "shop_vouchers_count": shop_count,
        "club_vouchers_count": club_vouchers_count,
        "promotion_messages": parsed_promos,
    }


def _sync_inspect_snapp_account(local_storage_str: str) -> dict:
    """
    بررسی آنلاین وضعیت اکانت اسنپ با استفاده از توکن موجود در LocalStorage:
    - دریافت پروفایل، اعتبار کیف پول، وضعیت اشتراک اسنپ‌پرو و مسافت طی‌شده از `/api/api-base/v2/passenger/config`
    - دریافت موجودی دقیق از `/api/api-base/v2/passenger/balance`
    - دریافت تعداد کوپن‌های دیده‌نشده از `/api/v1/promotion-center/unseen`
    - دریافت لیست کامل کوپن‌های تخفیف و جوایز اسنپ‌کلاب از `/api/v1/promotion-center/list`
    """
    auth_info = extract_auth_info_from_storage(local_storage_str)
    token = auth_info.get("access_token", "")
    checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    result = {
        "checked_at": checked_at,
        "has_token": bool(token),
        "token_valid": False,
        "status_text": "بدون توکن",
        "fullname": auth_info.get("fullname") or "نامشخص",
        "cellphone": "",
        "user_id": "",
        "universal_id": "",
        "referral_code": "",
        "credit_rial": 0,
        "credit_toman": 0,
        "credit_display": "۰ ریال",
        "snapp_pay_eligible": False,
        "snapp_pay_registered": False,
        "is_pro": False,
        "pro_expire_date": "",
        "total_distance_km": 0.0,
        "total_duration_min": 0,
        "club_points": auth_info.get("stored_points"),
        "club_enabled": False,
        "unseen_vouchers_count": 0,
        "total_vouchers": 0,
        "cab_vouchers_count": 0,
        "food_vouchers_count": 0,
        "shop_vouchers_count": 0,
        "club_vouchers_count": 0,
        "vouchers": [],
        "promotion_messages": [],
    }

    if not token:
        result["status_text"] = "❌ فاقد توکن Bearer در LocalStorage"
        return result

    if auth_info.get("is_jwt_expired"):
        result["status_text"] = "⚠️ توکن منقضی شده (نیاز به ورود مجدد)"

    pwa_headers = {
        "locale": "fa-IR",
        "App-Version": "pwa",
        "x-app-version": "v18.49.0",
        "x-app-name": "passenger-pwa",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    voucher_headers = {
        "Accept": "application/json, text/plain, */*",
        "X-App-Version": "5.8.0",
        "X-App-Name": "passenger-pwa",
        "locale": "fa-IR",
        "Authorization": f"Bearer {token}",
    }

    # ۱. استعلام کانفیگ و پروفایل مسافر
    cfg_status, cfg_json = _http_json_request(
        SNAPP_CONFIG_URL,
        method="POST",
        headers=pwa_headers,
        body_dict={
            "locale": "fa-IR",
            "device_type": 6,
            "version_code": 2,
            "os_version": "Android",
            "device_name": "Chrome",
            "referrer": 0,
        },
    )

    if cfg_status == 200 and isinstance(cfg_json.get("data"), dict):
        result["token_valid"] = True
        result["status_text"] = "🟢 فعال و معتبر"
        cdata = cfg_json["data"]
        prof = cdata.get("profile") or {}
        if prof.get("fullname"):
            result["fullname"] = prof.get("fullname")
        if prof.get("cellphone"):
            result["cellphone"] = normalize_iran_phone(prof.get("cellphone")) or prof.get("cellphone")
        result["user_id"] = str(prof.get("user_id") or "")
        result["universal_id"] = str(prof.get("universal_id") or "")
        result["referral_code"] = str(prof.get("referral_code") or "")

        credit_val = int(prof.get("credit") or 0)
        result["credit_rial"] = credit_val
        result["credit_toman"] = credit_val // 10
        result["credit_display"] = f"{credit_val:,} ریال ({credit_val // 10:,} تومان)"

        dist_m = int(prof.get("total_distance") or 0)
        result["total_distance_km"] = round(dist_m / 1000.0, 1)
        result["total_duration_min"] = int(prof.get("total_duration") or 0)

        # بررسی امتیاز احتمالی در پروفایل یا متا
        for pk in ("points", "club_points", "loyalty_points", "score"):
            if prof.get(pk) is not None:
                result["club_points"] = prof.get(pk)
            elif isinstance(prof.get("meta"), dict) and prof["meta"].get(pk) is not None:
                result["club_points"] = prof["meta"].get(pk)

        cw = cdata.get("credit_wallet_info") or {}
        result["snapp_pay_eligible"] = bool(cw.get("eligible", False))
        result["snapp_pay_registered"] = bool(cw.get("registered", False))

        pro_sub = (cdata.get("subscriptions") or {}).get("pro") or {}
        result["is_pro"] = bool(pro_sub.get("is_pro", False))
        exp_ts = int(pro_sub.get("expiration_date") or 0)
        if exp_ts > 0:
            try:
                result["pro_expire_date"] = datetime.fromtimestamp(exp_ts).strftime("%Y-%m-%d")
            except Exception:
                result["pro_expire_date"] = str(exp_ts)

        camps = cdata.get("campaigns") or {}
        if camps.get("backend.loyalty.missionbased.rollout") or camps.get("client.club.productsearch"):
            result["club_enabled"] = True

    elif cfg_status in (401, 403):
        result["status_text"] = "🔴 توکن منقضی یا نامعتبر (401)"
        return result

    # ۲. استعلام موجودی کیف پول
    bal_status, bal_json = _http_json_request(
        SNAPP_BALANCE_URL,
        method="POST",
        headers=pwa_headers,
        body_dict={"place": "sidemenu-topup"},
    )
    if bal_status == 200 and isinstance(bal_json.get("data"), dict):
        result["token_valid"] = True
        result["status_text"] = "🟢 فعال و معتبر"
        bdata = bal_json["data"]
        b_rial = int(bdata.get("balance") or result["credit_rial"] or 0)
        result["credit_rial"] = b_rial
        result["credit_toman"] = b_rial // 10
        result["credit_display"] = f"{b_rial:,} ریال ({b_rial // 10:,} تومان)"

    # ۳. استعلام تعداد کوپن‌های دیده‌نشده
    unseen_status, unseen_json = _http_json_request(
        SNAPP_UNSEEN_VOUCHERS_URL,
        method="GET",
        headers=pwa_headers,
    )
    if unseen_status == 200 and isinstance(unseen_json.get("data"), dict):
        vc = unseen_json["data"].get("voucher_center") or {}
        result["unseen_vouchers_count"] = int(vc.get("count") or 0)

    # ۴. استعلام لیست کامل کوپن‌های تخفیف و جوایز اسنپ‌کلاب
    vlist_status, vlist_json = _http_json_request(
        SNAPP_VOUCHER_LIST_URL,
        method="GET",
        headers=voucher_headers,
    )
    if vlist_status == 200 and isinstance(vlist_json.get("data"), dict):
        result["token_valid"] = True
        result["status_text"] = "🟢 فعال و معتبر"
        vdata = vlist_json["data"]
        parsed_v = parse_voucher_list_payload(
            vdata.get("vouchers") or [],
            vdata.get("promotion_message") or [],
        )
        result.update(parsed_v)

    if not result["token_valid"] and cfg_status == 0 and vlist_status == 0:
        result["status_text"] = "⚠️ عدم پاسخگویی سرور اسنپ (خطای شبکه/آی‌پی سرور)"

    return result


async def inspect_snapp_account_rewards(local_storage_str: str) -> dict:
    """اجرای غیرمسدودکننده (Async) بررسی آنلاین امتیازات و کوپن‌های تخفیف."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(HTTP_EXECUTOR, _sync_inspect_snapp_account, local_storage_str)


def format_single_account_rewards_text(record: dict) -> str:
    """فرمت‌بندی گزارش کامل کوپن‌ها، امتیاز و اعتبار یک لایسنس برای نمایش در تلگرام."""
    lic = record.get("license_key", "-")
    phone = record.get("phone_number", "نامشخص")
    pname = record.get("partner_name", "نامشخص")
    created = record.get("created_at", "-")
    rw = record.get("rewards") or {}

    fullname = rw.get("fullname") or "نامشخص"
    status_text = rw.get("status_text") or "بررسی نشده"
    credit_disp = rw.get("credit_display") or "۰ ریال"
    is_pro = "🌟 فعال" + (f" (تا {rw.get('pro_expire_date')})" if rw.get("pro_expire_date") else "") if rw.get("is_pro") else "ندارد"
    snapp_pay = "✅ فعال" if rw.get("snapp_pay_registered") else ("🟡 واجد شرایط" if rw.get("snapp_pay_eligible") else "غیرفعال")
    dist_km = rw.get("total_distance_km", 0)
    club_pts = rw.get("club_points")
    club_v_cnt = rw.get("club_vouchers_count", 0)
    pts_str = f"{club_pts:,} امتیاز" if club_pts is not None else f"فعال ({club_v_cnt} جایزه کلاب | {dist_km} کیلومتر سفر)"

    total_v = rw.get("total_vouchers", 0)
    cab_v = rw.get("cab_vouchers_count", 0)
    food_v = rw.get("food_vouchers_count", 0)
    shop_v = rw.get("shop_vouchers_count", 0)
    unseen_v = rw.get("unseen_vouchers_count", 0)
    checked_at = rw.get("checked_at", "-")

    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━",
        "🎁 *گزارش امتیاز، کیف‌پول و کوپن‌های تخفیف اسنپ*",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"🔑 لایسنس: `{lic}`",
        f"📱 شماره: `{phone}`",
        f"👤 نام صاحب حساب: `{fullname}`",
        f"📡 وضعیت نشست: *{status_text}*",
        f"💰 موجودی کیف‌پول: `{credit_disp}`",
        f"👑 اشتراک اسنپ‌پرو: `{is_pro}`",
        f"💳 سرویس اعتباری اسنپ‌پی: `{snapp_pay}`",
        f"⭐️ اسنپ‌کلاب / امتیاز: `{pts_str}`",
        f"🎫 مجموع کوپن‌های فعال: `{total_v}` عدد (دیده‌نشده: `{unseen_v}`)",
        f"   ├ 🚕 سفر و باکس: `{cab_v}` | 🍔 غذا: `{food_v}` | 🛍 فروشگاه: `{shop_v}`",
        f"   └ 🏆 جوایز اسنپ‌کلاب: `{club_v_cnt}`",
        f"🕒 آخرین استعلام: `{checked_at}`",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]

    vouchers = rw.get("vouchers") or []
    if not vouchers:
        lines.append("📭 هیچ کوپن تخفیف فعالی در این حساب یافت نشد.")
    else:
        lines.append(f"📋 *لیست کوپن‌های تخفیف فعال ({len(vouchers)} مورد):*\n")
        for idx, v in enumerate(vouchers[:15], start=1):
            v_title = v.get("title", "کوپن تخفیف")
            v_code = v.get("promotion_code") or "خودکار"
            v_venture = v.get("venture_title", "اسنپ")
            v_exp = v.get("expiry_text", "-")
            v_uses = f"{v.get('remain_use_times', 1)} بار"
            v_price = int(v.get("discount_price") or 0)
            price_tag = f" | سقف: {v_price:,} ت" if v_price > 0 else ""
            club_tag = " 🏆[اسنپ‌کلاب]" if v.get("is_club") else (" 🚕[سفر]" if v.get("is_cab") else "")
            new_tag = " 🆕" if v.get("is_new") else ""

            lines.append(
                f"{idx}. 🏷 *{v_venture}*{club_tag}{new_tag}\n"
                f"   📝 {v_title}\n"
                f"   🎟 کد: `{v_code}`{price_tag} | 🔄 `{v_uses}` | ⏳ {v_exp}"
            )
        if len(vouchers) > 15:
            lines.append(f"\n_... و {len(vouchers) - 15} کوپن دیگر (در فایل گزارش کامل قابل مشاهده است)._")

    lines.append(f"\n👤 ثبت‌کننده: `{pname}` | 📅 ثبت: `{created}`")
    return "\n".join(lines)


def fetch_all_snapp_records() -> list:
    db = get_redis()
    if not db:
        return []
    keys = db.keys(f"{REDIS_PREFIX}*")
    if not keys:
        return []

    records = []
    try:
        raw_values = db.mget(keys)
        for raw in raw_values:
            if not raw:
                continue
            try:
                item = json.loads(raw)
                records.append(item)
            except Exception:
                continue
    except Exception as e:
        logger.error(f"Error fetching records: {e}")

    records.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return records


async def scan_all_database_rewards(records: list) -> list:
    """اسکن همزمان (با محدودیت همزمانی ۵ درخواست) تمامی لایسنس‌های موجود در دیتابیس ردیس."""
    db = get_redis()
    sem = asyncio.Semaphore(5)

    async def _worker(rec: dict):
        async with sem:
            storage_str = rec.get("local_storage", "{}")
            rw = await inspect_snapp_account_rewards(storage_str)
            rec["rewards"] = rw
            if rw.get("cellphone") and rec.get("phone_number") in ("", "unknown", "نامشخص", None):
                rec["phone_number"] = rw["cellphone"]
            lic_key = rec.get("license_key")
            if db and lic_key:
                try:
                    db.set(f"{REDIS_PREFIX}{lic_key}", json.dumps(rec, ensure_ascii=False))
                except Exception as e:
                    logger.error(f"Error updating rewards in Redis for {lic_key}: {e}")
            return rec

    tasks = [_worker(r) for r in records]
    return await asyncio.gather(*tasks)


def build_rewards_txt_report(records: list, only_cab: bool = False) -> str:
    """تولید گزارش متنی جامع از امتیازها، اعتبار کیف‌پول و کوپن‌های تخفیف تمام اکانت‌های دیتابیس."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filtered = []
    for r in records:
        rw = r.get("rewards") or {}
        if only_cab:
            if rw.get("cab_vouchers_count", 0) > 0:
                filtered.append(r)
        else:
            filtered.append(r)

    total_vouchers_all = sum((r.get("rewards") or {}).get("total_vouchers", 0) for r in filtered)
    total_cab_all = sum((r.get("rewards") or {}).get("cab_vouchers_count", 0) for r in filtered)
    pro_accounts = sum(1 for r in filtered if (r.get("rewards") or {}).get("is_pro"))
    active_tokens = sum(1 for r in filtered if (r.get("rewards") or {}).get("token_valid"))

    title_header = (
        "گزارش اکانت‌های دارای کوپن تخفیف سفر/باکس اسنپ"
        if only_cab
        else "گزارش جامع امتیازات، کیف‌پول و کوپن‌های تخفیف دیتابیس اسنپ"
    )

    lines = [
        "======================================================================",
        f"   {title_header}",
        f"   تاریخ گزارش: {now_str}",
        f"   تعداد اکانت‌ها: {len(filtered)} | نشست‌های فعال: {active_tokens} | اسنپ‌پرو: {pro_accounts}",
        f"   مجموع کل کوپن‌ها: {total_vouchers_all} عدد (کوپن‌های سفر/باکس: {total_cab_all} عدد)",
        "======================================================================\n",
    ]

    for idx, r in enumerate(filtered, start=1):
        lic = r.get("license_key", "-")
        phone = r.get("phone_number", "نامشخص")
        pname = r.get("partner_name", "-")
        rw = r.get("rewards") or {}
        fullname = rw.get("fullname", "نامشخص")
        status_txt = rw.get("status_text", "بررسی نشده")
        credit_disp = rw.get("credit_display", "۰ ریال")
        is_pro = "بله" if rw.get("is_pro") else "خیر"
        snapp_pay = "فعال" if rw.get("snapp_pay_registered") else "غیرفعال"
        dist_km = rw.get("total_distance_km", 0)
        pts = rw.get("club_points")
        pts_txt = f"{pts:,}" if pts is not None else f"جوایز کلاب: {rw.get('club_vouchers_count', 0)} | مسافت: {dist_km}km"
        vouchers = rw.get("vouchers") or []

        lines.append(f"[{idx}] لایسنس: {lic} | شماره: {phone} | نام: {fullname}")
        lines.append(f"    وضعیت: {status_txt} | موجودی: {credit_disp} | اسنپ‌پرو: {is_pro} | اسنپ‌پی: {snapp_pay}")
        lines.append(f"    امتیاز/کلاب: {pts_txt} | کل کوپن‌ها: {len(vouchers)} (سفر/باکس: {rw.get('cab_vouchers_count', 0)}) | سازنده: {pname}")

        if vouchers:
            lines.append("    --- لیست کوپن‌های تخفیف ---")
            for v_idx, v in enumerate(vouchers, start=1):
                if only_cab and not v.get("is_cab"):
                    continue
                v_venture = v.get("venture_title", "اسنپ")
                v_title = v.get("title", "")
                v_code = v.get("promotion_code") or "خودکار"
                v_price = int(v.get("discount_price") or 0)
                v_uses = v.get("remain_use_times", 1)
                v_exp = v.get("expiry_text", "-")
                club_badge = " [اسنپ‌کلاب]" if v.get("is_club") else ""
                cab_badge = " [سفر/باکس]" if v.get("is_cab") else ""
                lines.append(
                    f"      {v_idx}) [{v_venture}]{cab_badge}{club_badge} {v_title}\n"
                    f"         کد تخفیف: {v_code} | سقف تخفیف: {v_price:,} | دفعات مجاز: {v_uses} | مهلت: {v_exp}"
                )
        else:
            lines.append("    --- فاقد کوپن تخفیف فعال ---")
        lines.append("-" * 70)

    return "\n".join(lines)


# ======================== کیبوردها و هندلرهای ربات تلگرام ========================
def kb_snapp_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎁 بررسی امتیاز و کوپن‌های دیتابیس", callback_data="snapp_rewards_menu"),
        ],
        [
            InlineKeyboardButton("📊 آمار و وضعیت دیتابیس", callback_data="snapp_stats"),
            InlineKeyboardButton("👥 مدیریت سهمیه دوستان", callback_data="snapp_partners"),
        ],
        [
            InlineKeyboardButton("📋 لیست لایسنس و شماره‌ها", callback_data="snapp_list_inline"),
            InlineKeyboardButton("📄 گزارش لایسنس + شماره (TXT)", callback_data="snapp_export_pairs"),
        ],
        [
            InlineKeyboardButton("📊 خروجی اکسل کامل (CSV)", callback_data="snapp_export_csv"),
            InlineKeyboardButton("📦 بکاپ کامل LocalStorage (JSON)", callback_data="snapp_export_json"),
        ],
        [
            InlineKeyboardButton("🔑 خروجی فقط لایسنس‌ها", callback_data="snapp_export_keys"),
            InlineKeyboardButton("📱 خروجی فقط شماره‌ها", callback_data="snapp_export_phones"),
        ],
        [
            InlineKeyboardButton("🔍 راهنمای دستورات و سهمیه", callback_data="snapp_help"),
            InlineKeyboardButton("🗑 پاک‌سازی کل دیتابیس", callback_data="snapp_clear_confirm"),
        ],
    ])


def kb_rewards_submenu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 اسکن آنلاین همه اکانت‌ها از سرور اسنپ", callback_data="snapp_scan_all_rewards"),
        ],
        [
            InlineKeyboardButton("🎫 مشاهده اکانت‌های دارای کوپن", callback_data="snapp_view_coupon_accounts"),
            InlineKeyboardButton("🚕 گزارش ویژه کوپن‌های سفر (TXT)", callback_data="snapp_export_cab_rewards"),
        ],
        [
            InlineKeyboardButton("📄 گزارش کامل کوپن و امتیاز (TXT)", callback_data="snapp_export_rewards_txt"),
            InlineKeyboardButton("📊 خروجی اکسل کوپن و امتیاز (CSV)", callback_data="snapp_export_rewards_csv"),
        ],
        [
            InlineKeyboardButton("🔙 بازگشت به پنل مدیریت", callback_data="snapp_home"),
        ],
    ])


def kb_back_to_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 بازگشت به پنل مدیریت", callback_data="snapp_home")]
    ])


def is_admin(user_id: int) -> bool:
    if not ALLOWED_USER_IDS:
        return True
    return user_id in ALLOWED_USER_IDS


def get_welcome_text() -> str:
    db = get_redis()
    records = fetch_all_snapp_records()
    partners = fetch_all_partners()
    with_coupons = sum(1 for r in records if (r.get("rewards") or {}).get("total_vouchers", 0) > 0)
    total_coupons = sum((r.get("rewards") or {}).get("total_vouchers", 0) for r in records)
    db_badge = "🟢 متصل" if db else "🔴 قطع (متغیر REDIS_URL را بررسی کنید)"
    return (
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🚀 *پنل مدیریت حرفه‌ای لایسنس، کوپن و سهمیه اسنپ*\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📡 وضعیت دیتابیس ردیس: *{db_badge}*\n"
        f"🗄 مجموع لایسنس‌های ثبت‌شده: `{len(records)}`\n"
        f"🎁 اکانت‌های دارای کوپن تخفیف: `{with_coupons}` (`{total_coupons}` کوپن فعال)\n"
        f"👥 تعداد کدهای اشتراک دوستان: `{len(partners)}`\n\n"
        "از منوی زیر گزارش کوپن‌ها، امتیازات یا مدیریت سهمیه را انتخاب کنید:"
    )


def get_rewards_overview_text(records: list) -> str:
    total = len(records)
    scanned = sum(1 for r in records if r.get("rewards"))
    valid_tokens = sum(1 for r in records if (r.get("rewards") or {}).get("token_valid"))
    with_any_voucher = sum(1 for r in records if (r.get("rewards") or {}).get("total_vouchers", 0) > 0)
    with_cab_voucher = sum(1 for r in records if (r.get("rewards") or {}).get("cab_vouchers_count", 0) > 0)
    with_club_voucher = sum(1 for r in records if (r.get("rewards") or {}).get("club_vouchers_count", 0) > 0)
    pro_users = sum(1 for r in records if (r.get("rewards") or {}).get("is_pro"))
    total_vouchers = sum((r.get("rewards") or {}).get("total_vouchers", 0) for r in records)
    total_cab_vouchers = sum((r.get("rewards") or {}).get("cab_vouchers_count", 0) for r in records)
    total_balance_toman = sum((r.get("rewards") or {}).get("credit_toman", 0) for r in records)

    return (
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🎁 *مرکز بررسی امتیازات، کیف‌پول و کوپن‌های تخفیف اسنپ*\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🗂 کل اکانت‌های دیتابیس: `{total}` (بررسی‌شده: `{scanned}`)\n"
        f"🟢 نشست‌های معتبر و فعال: `{valid_tokens}`\n"
        f"👑 اکانت‌های دارای اشتراک اسنپ‌پرو: `{pro_users}`\n"
        f"💰 مجموع موجودی کیف‌پول اکانت‌ها: `{total_balance_toman:,} تومان`\n"
        "──────────────────────\n"
        f"🎫 اکانت‌های دارای کوپن تخفیف: `{with_any_voucher}` اکانت\n"
        f"🚕 اکانت‌های دارای تخفیف سفر/باکس: `{with_cab_voucher}` اکانت\n"
        f"🏆 اکانت‌های دارای جایزه اسنپ‌کلاب: `{with_club_voucher}` اکانت\n"
        f"🔢 مجموع کل کوپن‌های فعال کشف‌شده: `{total_vouchers}` عدد (سفر: `{total_cab_vouchers}`)\n\n"
        "💡 *راهنما:* با زدن دکمه «اسکن آنلاین همه اکانت‌ها»، ربات به کمک توکن داخل `LocalStorage` تمام اکانت‌ها را از سرور اسنپ استعلام گرفته و لیست دقیق کدهای تخفیف، موجودی و امتیازات را بروزرسانی می‌کند.\n"
        "🔎 برای بررسی تکی هر اکانت نیز می‌توانید دستور زیر را بفرستید:\n"
        "`/check Snapp-XXXX-XXXX` یا `/check 09123456789`"
    )


async def start_snapp_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_message or not update.effective_user:
        return
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.effective_message.reply_text(
            f"⛔️ شما دسترسی ادمین ندارید.\nآیدی عددی شما جهت ثبت در متغیر `ALLOWED_USER_IDS`:\n`{uid}`",
            parse_mode="Markdown",
        )
        return
    await update.effective_message.reply_text(
        get_welcome_text(),
        reply_markup=kb_snapp_admin(),
        parse_mode="Markdown",
    )


async def add_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ساخت کد اشتراک با محدودیت تعداد لایسنس برای دوست/همکار: /add_user Ali 50"""
    if not update.effective_message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        return

    db = get_redis()
    if not db:
        await update.effective_message.reply_text("❌ دیتابیس ردیس متصل نیست!", parse_mode="Markdown")
        return

    if len(context.args) < 2:
        await update.effective_message.reply_text(
            "⚠️ *راهنمای ساخت کد اشتراک با محدودیت تعداد:*\n\n"
            "`/add_user نام_دوست تعداد_مجاز`\n\n"
            "📌 *مثال (سهمیه ۵۰ لایسنس برای علی):*\n"
            "`/add_user Ali 50`",
            parse_mode="Markdown",
        )
        return

    name = context.args[0].strip()
    try:
        limit = int(context.args[1].strip())
        if limit <= 0:
            raise ValueError()
    except ValueError:
        await update.effective_message.reply_text("⚠️ تعداد مجاز باید یک عدد صحیح مثبت باشد.", parse_mode="Markdown")
        return

    code = generate_partner_code()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    partner_data = {
        "code": code,
        "name": name,
        "limit": limit,
        "used": 0,
        "active": True,
        "created_at": now_str,
    }
    db.set(f"{PARTNER_PREFIX}{code}", json.dumps(partner_data, ensure_ascii=False))

    msg = (
        "✅ *کد اشتراک اختصاصی با موفقیت ساخته شد!*\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 نام کاربر: `{name}`\n"
        f"🔑 کد اشتراک: `{code}`\n"
        f"🎯 سقف مجاز ساخت لایسنس: `{limit}` عدد\n"
        f"📊 مصرف شده: `0 / {limit}`\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 بدون وارد کردن این کد در برنامه، امکان صدور هیچ لایسنسی وجود ندارد."
    )
    await update.effective_message.reply_text(msg, parse_mode="Markdown")


async def set_limit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تغییر یا شارژ سقف مجاز یک کد اشتراک: /set_limit SUB-XXXXXX 100"""
    if not update.effective_message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        return

    db = get_redis()
    if len(context.args) < 2:
        await update.effective_message.reply_text(
            "⚠️ *راهنمای تغییر سقف سهمیه:*\n`/set_limit SUB-XXXXXX 100`",
            parse_mode="Markdown",
        )
        return

    code = context.args[0].strip().upper()
    try:
        new_limit = int(context.args[1].strip())
        if new_limit < 0:
            raise ValueError()
    except ValueError:
        await update.effective_message.reply_text("⚠️ سقف جدید باید یک عدد صحیح باشد.", parse_mode="Markdown")
        return

    rk, pdata = get_partner_record(code)
    if not rk or not pdata:
        await update.effective_message.reply_text(f"❌ کد اشتراک `{code}` یافت نشد.", parse_mode="Markdown")
        return

    pdata["limit"] = new_limit
    pdata["active"] = True
    db.set(rk, json.dumps(pdata, ensure_ascii=False))
    await update.effective_message.reply_text(
        f"✅ سقف مجاز کاربر `{pdata.get('name')}` (`{code}`) به `{new_limit}` لایسنس تغییر یافت.\n"
        f"📊 وضعیت فعلی: `{pdata.get('used', 0)} / {new_limit}`",
        parse_mode="Markdown",
    )


async def block_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """مسدود کردن کد اشتراک دوست: /block_user SUB-XXXXXX"""
    if not update.effective_message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        return

    db = get_redis()
    if not context.args:
        await update.effective_message.reply_text(
            "⚠️ *راهنمای مسدودسازی کد اشتراک:*\n`/block_user SUB-XXXXXX`",
            parse_mode="Markdown",
        )
        return

    code = context.args[0].strip().upper()
    rk, pdata = get_partner_record(code)
    if not rk or not pdata:
        await update.effective_message.reply_text(f"❌ کد اشتراک `{code}` یافت نشد.", parse_mode="Markdown")
        return

    pdata["active"] = False
    db.set(rk, json.dumps(pdata, ensure_ascii=False))
    await update.effective_message.reply_text(
        f"🚫 دسترسی کد اشتراک `{code}` متعلق به `{pdata.get('name')}` مسدود شد.\n"
        f"برای فعال‌سازی مجدد می‌توانید از دستور `/set_limit {code} {pdata.get('limit', 50)}` استفاده کنید.",
        parse_mode="Markdown",
    )


async def check_rewards_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """استعلام زنده امتیازات، موجودی کیف پول و کوپن‌های تخفیف یک لایسنس یا شماره: /check Snapp-XXXX-XXXX"""
    if not update.effective_message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.effective_message.reply_text(
            "⚠️ *راهنمای بررسی آنلاین کوپن‌ها و امتیازات اکانت:*\n\n"
            "🔹 بررسی با کلید لایسنس:\n"
            "`/check Snapp-XXXX-XXXX`\n\n"
            "🔹 بررسی با شماره موبایل:\n"
            "`/check 09123456789`\n\n"
            "🔹 اسکن یکجای کل دیتابیس:\n"
            "`/check_all`",
            parse_mode="Markdown",
        )
        return

    query_str = context.args[0].strip()
    norm_q_phone = normalize_iran_phone(query_str)
    records = fetch_all_snapp_records()
    matched = [
        r for r in records
        if r.get("license_key", "").lower() == query_str.lower()
        or r.get("phone_number", "") == query_str
        or (norm_q_phone and r.get("phone_number", "") == norm_q_phone)
    ]

    if not matched:
        await update.effective_message.reply_text(
            f"❌ هیچ لایسنسی با مشخصات `{query_str}` در دیتابیس یافت نشد.",
            parse_mode="Markdown",
        )
        return

    wait_msg = await update.effective_message.reply_text(
        f"⏳ در حال استعلام آنلاین امتیازها و کوپن‌های تخفیف از سرور اسنپ ({len(matched[:3])} اکانت)...",
        parse_mode="Markdown",
    )

    db = get_redis()
    for item in matched[:3]:
        rw = await inspect_snapp_account_rewards(item.get("local_storage", "{}"))
        item["rewards"] = rw
        if rw.get("cellphone") and item.get("phone_number") in ("", "unknown", "نامشخص", None):
            item["phone_number"] = rw["cellphone"]
        lic = item.get("license_key")
        if db and lic:
            db.set(f"{REDIS_PREFIX}{lic}", json.dumps(item, ensure_ascii=False))

        report_text = format_single_account_rewards_text(item)
        await update.effective_message.reply_text(report_text, parse_mode="Markdown")

    try:
        await wait_msg.delete()
    except Exception:
        pass


async def check_all_rewards_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """اسکن دسته‌جمعی کل اکانت‌های دیتابیس و ارسال فایل گزارش کوپن‌ها و امتیازات: /check_all"""
    if not update.effective_message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        return

    records = fetch_all_snapp_records()
    if not records:
        await update.effective_message.reply_text("⚠️ دیتابیس خالی است!", parse_mode="Markdown")
        return

    wait_msg = await update.effective_message.reply_text(
        f"⏳ در حال اسکن آنلاین `{len(records)}` اکانت در دیتابیس از سرور اسنپ...\n"
        "لطفاً چند ثانیه صبر کنید.",
        parse_mode="Markdown",
    )

    updated_records = await scan_all_database_rewards(records)
    now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_report = build_rewards_txt_report(updated_records, only_cab=False)
    doc = io.BytesIO(txt_report.encode("utf-8"))
    doc.name = f"Snapp_Rewards_Coupons_{now_tag}.txt"

    overview = get_rewards_overview_text(updated_records)
    try:
        await wait_msg.edit_text(overview, reply_markup=kb_rewards_submenu(), parse_mode="Markdown")
    except Exception:
        await update.effective_message.reply_text(overview, reply_markup=kb_rewards_submenu(), parse_mode="Markdown")

    await update.effective_message.reply_document(
        doc,
        caption="🎁 *گزارش کامل امتیازات، موجودی کیف‌پول و کوپن‌های تخفیف کل دیتابیس اسنپ*",
        parse_mode="Markdown",
    )


async def get_license_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.effective_message.reply_text(
            "⚠️ *راهنمای جستجو:*\n"
            "`/get Snapp-XXXX-XXXX`\n"
            "یا جستجو با شماره موبایل:\n"
            "`/get 09123456789`",
            parse_mode="Markdown",
        )
        return

    query_str = context.args[0].strip()
    norm_q_phone = normalize_iran_phone(query_str)
    records = fetch_all_snapp_records()
    matched = [
        r for r in records
        if r.get("license_key", "").lower() == query_str.lower()
        or r.get("phone_number", "") == query_str
        or (norm_q_phone and r.get("phone_number", "") == norm_q_phone)
        or r.get("partner_code", "").lower() == query_str.lower()
    ]

    if not matched:
        await update.effective_message.reply_text(f"❌ هیچ رکوردی برای `{query_str}` یافت نشد.", parse_mode="Markdown")
        return

    for item in matched[:5]:
        lic = item.get("license_key", "-")
        phone = item.get("phone_number", "نامشخص")
        created = item.get("created_at", "-")
        partner_name = item.get("partner_name", "نامشخص")
        storage_str = item.get("local_storage", "{}")
        rw = item.get("rewards") or {}
        v_cnt = rw.get("total_vouchers", 0)
        credit_disp = rw.get("credit_display", "بررسی نشده")

        doc = io.BytesIO(storage_str.encode("utf-8"))
        doc.name = f"{lic}_{phone}.json"
        caption = (
            "🔍 *اطلاعات لایسنس یافت‌شده:*\n\n"
            f"🔑 لایسنس: `{lic}`\n"
            f"📱 شماره: `{phone}`\n"
            f"👤 نام حساب: `{rw.get('fullname', 'نامشخص')}`\n"
            f"💰 اعتبار کیف‌پول: `{credit_disp}`\n"
            f"🎫 تعداد کوپن فعال: `{v_cnt}` عدد\n"
            f"👤 سازنده: `{partner_name}`\n"
            f"⏰ زمان ثبت: `{created}`\n"
            f"🎁 استعلام زنده کوپن‌ها: `/check {lic}`\n"
            f"🗑 حذف سریع: `/delete {lic}`"
        )
        await update.effective_message.reply_document(doc, caption=caption, parse_mode="Markdown")


async def delete_snapp_license(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.effective_message.reply_text(
            "⚠️ *راهنمای حذف:*\n`/delete Snapp-XXXX-XXXX`",
            parse_mode="Markdown",
        )
        return

    target = context.args[0].strip()
    db = get_redis()
    rk, _ = find_redis_key_for_license(target)
    if rk and db and db.delete(rk):
        await update.effective_message.reply_text(f"✅ لایسنس `{target}` با موفقیت از دیتابیس حذف شد.", parse_mode="Markdown")
    else:
        await update.effective_message.reply_text(f"⚠️ لایسنس `{target}` در دیتابیس یافت نشد.", parse_mode="Markdown")


async def snapp_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not is_admin(query.from_user.id):
        if query:
            await query.answer(f"⛔️ دسترسی غیرمجاز. آیدی شما: {query.from_user.id}", show_alert=True)
        return

    db = get_redis()
    if not db:
        await query.answer("❌ دیتابیس ردیس متصل نیست! متغیر REDIS_URL را بررسی کنید.", show_alert=True)
        return

    data_cb = query.data
    records = fetch_all_snapp_records()
    now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")

    if data_cb == "snapp_home":
        await query.answer()
        await query.edit_message_text(
            get_welcome_text(),
            reply_markup=kb_snapp_admin(),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_rewards_menu":
        await query.answer()
        await query.edit_message_text(
            get_rewards_overview_text(records),
            reply_markup=kb_rewards_submenu(),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_scan_all_rewards":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        await query.answer("⏳ اسکن آنلاین کوپن‌ها و امتیازات آغاز شد...")
        await query.edit_message_text(
            f"⏳ *در حال استعلام آنلاین `{len(records)}` اکانت از سرورهای اسنپ...*\n\n"
            "ربات در حال بررسی پروفایل، موجودی کیف‌پول، اشتراک اسنپ‌پرو، امتیازات و لیست کوپن‌های تخفیف تمامی اکانت‌هاست. لطفاً چند لحظه صبر کنید...",
            parse_mode="Markdown",
        )

        updated_records = await scan_all_database_rewards(records)
        await query.edit_message_text(
            get_rewards_overview_text(updated_records),
            reply_markup=kb_rewards_submenu(),
            parse_mode="Markdown",
        )

        txt_report = build_rewards_txt_report(updated_records, only_cab=False)
        doc = io.BytesIO(txt_report.encode("utf-8"))
        doc.name = f"Snapp_Full_Rewards_Report_{now_tag}.txt"
        await query.message.reply_document(
            doc,
            caption=(
                "✅ *اسکن آنلاین دیتابیس به پایان رسید!*\n"
                "📄 گزارش کامل امتیازات، موجودی کیف‌پول و کدهای تخفیف تمام اکانت‌ها به پیوست ارسال شد."
            ),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_view_coupon_accounts":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        # اگر هیچ اکانتی قبلاً اسکن نشده باشد، به صورت خودکار اسکن می‌کنیم
        if not any(r.get("rewards") for r in records):
            await query.answer("⏳ در حال استعلام اولیه اکانت‌ها از اسنپ...")
            records = await scan_all_database_rewards(records)
        else:
            await query.answer()

        coupon_accounts = [
            r for r in records if (r.get("rewards") or {}).get("total_vouchers", 0) > 0
        ]
        coupon_accounts.sort(
            key=lambda x: (
                (x.get("rewards") or {}).get("cab_vouchers_count", 0),
                (x.get("rewards") or {}).get("total_vouchers", 0),
            ),
            reverse=True,
        )

        lines = [
            "━━━━━━━━━━━━━━━━━━━━━━",
            f"🎫 *اکانت‌های دارای کوپن تخفیف فعال ({len(coupon_accounts)} از {len(records)} اکانت)*",
            "━━━━━━━━━━━━━━━━━━━━━━\n",
        ]

        if not coupon_accounts:
            lines.append("📭 هیچ اکانتی با کوپن تخفیف فعال یافت نشد.\n💡 ابتدا دکمه «اسکن آنلاین همه اکانت‌ها» را بزنید.")
        else:
            for idx, r in enumerate(coupon_accounts[:20], start=1):
                lic = r.get("license_key", "-")
                phone = r.get("phone_number", "نامشخص")
                rw = r.get("rewards") or {}
                fname = rw.get("fullname", "نامشخص")
                tot_v = rw.get("total_vouchers", 0)
                cab_v = rw.get("cab_vouchers_count", 0)
                club_v = rw.get("club_vouchers_count", 0)
                cred = rw.get("credit_toman", 0)
                pro_badge = "👑پرو | " if rw.get("is_pro") else ""

                top_titles = []
                for v in (rw.get("vouchers") or [])[:3]:
                    vt = v.get("venture_title", "")
                    vc = v.get("promotion_code") or "خودکار"
                    top_titles.append(f"{vt}(`{vc}`)")
                v_preview = " ، ".join(top_titles)

                lines.append(
                    f"{idx}. 📱 `{phone}` ({fname}) ➔ `{lic}`\n"
                    f"   ├ {pro_badge}💰 `{cred:,} ت` | 🎫 کل کوپن: `{tot_v}` (🚕 سفر: `{cab_v}` | 🏆 کلاب: `{club_v}`)\n"
                    f"   └ 🏷 نمونه: {v_preview}\n"
                )

            if len(coupon_accounts) > 20:
                lines.append(f"💡 _برای مشاهده هر {len(coupon_accounts)} اکانت، دکمه گزارش TXT یا CSV را بزنید._")

        await query.edit_message_text(
            "\n".join(lines),
            reply_markup=kb_rewards_submenu(),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_export_cab_rewards":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        if not any(r.get("rewards") for r in records):
            await query.answer("⏳ در حال اسکن آنلاین اکانت‌ها...")
            records = await scan_all_database_rewards(records)
        else:
            await query.answer("⏳ در حال ساخت گزارش کوپن‌های سفر/باکس...")

        txt_report = build_rewards_txt_report(records, only_cab=True)
        doc = io.BytesIO(txt_report.encode("utf-8"))
        doc.name = f"Snapp_Cab_Coupons_{now_tag}.txt"
        cab_acc_count = sum(1 for r in records if (r.get("rewards") or {}).get("cab_vouchers_count", 0) > 0)
        await query.message.reply_document(
            doc,
            caption=(
                "🚕 *گزارش اختصاصی اکانت‌های دارای کوپن تخفیف سفر، بایک و باکس اسنپ*\n"
                f"🔢 تعداد اکانت‌های دارای تخفیف سفر: `{cab_acc_count}`"
            ),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_export_rewards_txt":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        if not any(r.get("rewards") for r in records):
            await query.answer("⏳ در حال اسکن آنلاین اکانت‌ها...")
            records = await scan_all_database_rewards(records)
        else:
            await query.answer("⏳ در حال ساخت گزارش جامع کوپن‌ها و امتیازات...")

        txt_report = build_rewards_txt_report(records, only_cab=False)
        doc = io.BytesIO(txt_report.encode("utf-8"))
        doc.name = f"Snapp_All_Rewards_Coupons_{now_tag}.txt"
        await query.message.reply_document(
            doc,
            caption=(
                "📄 *گزارش کامل امتیازات، موجودی کیف‌پول و تمام کوپن‌های تخفیف دیتابیس*\n"
                f"🔢 تعداد کل اکانت‌ها: `{len(records)}`"
            ),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_export_rewards_csv":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        if not any(r.get("rewards") for r in records):
            await query.answer("⏳ در حال اسکن آنلاین اکانت‌ها...")
            records = await scan_all_database_rewards(records)
        else:
            await query.answer("⏳ در حال تولید فایل اکسل (CSV) کوپن‌ها و امتیازات...")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "ردیف",
            "کلید لایسنس",
            "شماره موبایل",
            "نام صاحب حساب",
            "وضعیت توکن",
            "موجودی کیف‌پول (تومان)",
            "اشتراک اسنپ‌پرو",
            "اسنپ‌پی",
            "امتیاز کلاب / مسافت (km)",
            "کل کوپن‌های فعال",
            "کوپن سفر/باکس",
            "کوپن غذا",
            "کوپن فروشگاه",
            "جایزه اسنپ‌کلاب",
            "کوپن دیده‌نشده",
            "جزئیات و کدهای تخفیف",
            "سازنده (همکار)",
            "زمان آخرین بررسی",
        ])

        for idx, r in enumerate(records, start=1):
            rw = r.get("rewards") or {}
            v_summaries = []
            for v in rw.get("vouchers") or []:
                v_code = v.get("promotion_code") or "خودکار"
                v_summaries.append(f"[{v.get('venture_title')}: {v.get('title')} (کد: {v_code} | {v.get('expiry_text')})]")

            pts_val = rw.get("club_points")
            pts_col = f"{pts_val} امتیاز" if pts_val is not None else f"{rw.get('total_distance_km', 0)} km"

            writer.writerow([
                idx,
                r.get("license_key", ""),
                r.get("phone_number", "نامشخص"),
                rw.get("fullname", "نامشخص"),
                rw.get("status_text", "بررسی نشده"),
                rw.get("credit_toman", 0),
                "فعال" if rw.get("is_pro") else "ندارد",
                "فعال" if rw.get("snapp_pay_registered") else "غیرفعال",
                pts_col,
                rw.get("total_vouchers", 0),
                rw.get("cab_vouchers_count", 0),
                rw.get("food_vouchers_count", 0),
                rw.get("shop_vouchers_count", 0),
                rw.get("club_vouchers_count", 0),
                rw.get("unseen_vouchers_count", 0),
                " | ".join(v_summaries) if v_summaries else "بدون کوپن",
                r.get("partner_name", "-"),
                rw.get("checked_at", "-"),
            ])

        doc = io.BytesIO(output.getvalue().encode("utf-8-sig"))
        doc.name = f"Snapp_Rewards_Coupons_{now_tag}.csv"
        await query.message.reply_document(
            doc,
            caption=(
                "📊 *فایل اکسل (CSV) جامع امتیازات، موجودی و کدهای تخفیف دیتابیس اسنپ*\n"
                f"🔢 تعداد رکوردها: `{len(records)}`"
            ),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_stats":
        await query.answer()
        total = len(records)
        known_phones = {
            r.get("phone_number")
            for r in records
            if r.get("phone_number") and r.get("phone_number") not in ("unknown", "نامشخص")
        }
        today_prefix = datetime.now().strftime("%Y-%m-%d")
        today_count = sum(1 for r in records if str(r.get("created_at", "")).startswith(today_prefix))
        latest_time = records[0].get("created_at", "-") if records else "بدون رکورد"
        partners = fetch_all_partners()
        with_coupons = sum(1 for r in records if (r.get("rewards") or {}).get("total_vouchers", 0) > 0)
        total_vouchers = sum((r.get("rewards") or {}).get("total_vouchers", 0) for r in records)

        text = (
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📊 *گزارش آماری دیتابیس اسنپ*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🗂 کل لایسنس‌های موجود: `{total}`\n"
            f"📱 شماره‌های یکتای شناسایی‌شده: `{len(known_phones)}`\n"
            f"🎁 اکانت‌های دارای کوپن تخفیف: `{with_coupons}` (مجموعاً `{total_vouchers}` کوپن)\n"
            f"📅 لایسنس‌های ثبت‌شده امروز: `{today_count}`\n"
            f"👥 تعداد همکاران/دوستان تعریف‌شده: `{len(partners)}`\n"
            f"🕓 آخرین ثبت: `{latest_time}`\n"
        )
        await query.edit_message_text(text, reply_markup=kb_back_to_main(), parse_mode="Markdown")

    elif data_cb == "snapp_partners":
        await query.answer()
        partners = fetch_all_partners()
        lines = [
            "━━━━━━━━━━━━━━━━━━━━━━",
            "👥 *مدیریت سهمیه دوستان و همکاران*",
            "━━━━━━━━━━━━━━━━━━━━━━\n",
        ]
        if not partners:
            lines.append("هنوز هیچ کد اشتراکی نساخته‌اید.\n")
        else:
            for idx, p in enumerate(partners, start=1):
                status_icon = "🟢 فعال" if p.get("active", True) else "🔴 مسدود"
                used = p.get("used", 0)
                limit = p.get("limit", 0)
                rem = max(0, limit - used)
                lines.append(
                    f"{idx}. 👤 *{p.get('name')}* (`{p.get('code')}`)\n"
                    f"   ├ وضعیت: {status_icon}\n"
                    f"   └ مصرف‌شده: `{used} / {limit}` (باقی‌مانده: `{rem}`)\n"
                )

        lines.append("📌 *دستورات مدیریت سهمیه:*")
        lines.append("➕ ساخت کد جدید:\n`/add_user Ali 50`")
        lines.append("🔄 تغییر سقف سهمیه:\n`/set_limit SUB-XXXXXX 100`")
        lines.append("🚫 مسدود کردن کد:\n`/block_user SUB-XXXXXX`")

        await query.edit_message_text(
            "\n".join(lines),
            reply_markup=kb_back_to_main(),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_list_inline":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        await query.answer()
        preview_limit = 30
        lines = [
            "━━━━━━━━━━━━━━━━━━━━━━",
            f"📋 *لیست لایسنس‌ها و شماره‌ها ({min(len(records), preview_limit)} مورد اخیر از {len(records)})*",
            "━━━━━━━━━━━━━━━━━━━━━━\n",
        ]
        for idx, r in enumerate(records[:preview_limit], start=1):
            lic = r.get("license_key", "-")
            phone = r.get("phone_number", "نامشخص")
            pname = r.get("partner_name", "")
            rw = r.get("rewards") or {}
            v_cnt = rw.get("total_vouchers", 0)
            ptag = f" ({pname})" if pname else ""
            vtag = f" | 🎫`{v_cnt}`" if v_cnt > 0 else ""
            lines.append(f"{idx}. 📱 `{phone}` ➔ `{lic}`{ptag}{vtag}")

        if len(records) > preview_limit:
            lines.append(f"\n💡 _برای مشاهده تمام {len(records)} مورد، دکمه «گزارش لایسنس + شماره (TXT)» را بزنید._")

        await query.edit_message_text(
            "\n".join(lines),
            reply_markup=kb_back_to_main(),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_export_pairs":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        await query.answer("⏳ در حال ساخت فایل گزارش لایسنس و شماره‌ها...")
        lines = [
            "==========================================",
            "   گزارش لایسنس‌های اسنپ به همراه شماره   ",
            f"   تاریخ خروجی: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"   تعداد کل: {len(records)}",
            "==========================================\n",
        ]
        for idx, r in enumerate(records, start=1):
            lic = r.get("license_key", "-")
            phone = r.get("phone_number", "نامشخص")
            pname = r.get("partner_name", "-")
            created = r.get("created_at", "-")
            rw = r.get("rewards") or {}
            v_cnt = rw.get("total_vouchers", 0)
            cred = rw.get("credit_toman", 0)
            lines.append(
                f"[{idx}] شماره: {phone} | لایسنس: {lic} | کوپن فعال: {v_cnt} | موجودی: {cred:,} تومان | سازنده: {pname} | زمان: {created}"
            )

        doc = io.BytesIO("\n".join(lines).encode("utf-8"))
        doc.name = f"Snapp_License_Phone_Report_{now_tag}.txt"
        await query.message.reply_document(
            doc,
            caption=(
                "📄 *گزارش کامل لایسنس‌ها به همراه شماره موبایل، تعداد کوپن و نام سازنده*\n"
                f"🔢 تعداد کل: `{len(records)}`"
            ),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_export_keys":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        await query.answer("⏳ در حال استخراج لیست لایسنس‌ها...")
        keys_only = [r.get("license_key", "") for r in records if r.get("license_key")]
        doc = io.BytesIO("\n".join(keys_only).encode("utf-8"))
        doc.name = f"Snapp_Only_Licenses_{now_tag}.txt"
        await query.message.reply_document(
            doc,
            caption=f"🔑 *لیست خام کلیدهای لایسنس اسنپ*\n🔢 تعداد: `{len(keys_only)}`",
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_export_phones":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        await query.answer("⏳ در حال استخراج لیست شماره‌ها...")
        phones_list = []
        for r in records:
            p = r.get("phone_number", "")
            if p and p not in ("unknown", "نامشخص") and p not in phones_list:
                phones_list.append(p)

        content = "\n".join(phones_list) if phones_list else "هیچ شماره مشخصی ثبت نشده است."
        doc = io.BytesIO(content.encode("utf-8"))
        doc.name = f"Snapp_Phones_{now_tag}.txt"
        await query.message.reply_document(
            doc,
            caption=f"📱 *لیست شماره‌های یکتای استخراج‌شده*\n🔢 تعداد: `{len(phones_list)}`",
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_export_csv":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        await query.answer("⏳ در حال تولید فایل CSV (اکسل)...")
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "ردیف",
            "کلید لایسنس",
            "شماره موبایل",
            "نام حساب",
            "موجودی (تومان)",
            "تعداد کوپن فعال",
            "کوپن سفر/باکس",
            "اشتراک اسنپ‌پرو",
            "سازنده (همکار)",
            "کد اشتراک",
            "تعداد کلیدهای LocalStorage",
            "زمان ثبت",
        ])
        for idx, r in enumerate(records, start=1):
            rw = r.get("rewards") or {}
            writer.writerow([
                idx,
                r.get("license_key", ""),
                r.get("phone_number", "نامشخص"),
                rw.get("fullname", "-"),
                rw.get("credit_toman", 0),
                rw.get("total_vouchers", 0),
                rw.get("cab_vouchers_count", 0),
                "فعال" if rw.get("is_pro") else "ندارد",
                r.get("partner_name", "-"),
                r.get("partner_code", "-"),
                r.get("storage_keys_count", "-"),
                r.get("created_at", ""),
            ])

        doc = io.BytesIO(output.getvalue().encode("utf-8-sig"))
        doc.name = f"Snapp_Report_{now_tag}.csv"
        await query.message.reply_document(
            doc,
            caption=f"📊 *فایل اکسل (CSV) گزارش لایسنس‌ها، شماره‌ها، کوپن‌ها و سهمیه همکاران*\n🔢 تعداد: `{len(records)}`",
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_export_json":
        if not records:
            await query.answer("⚠️ دیتابیس خالی است!", show_alert=True)
            return

        await query.answer("⏳ در حال آماده‌سازی بکاپ کامل JSON...")
        json_bytes = json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8")
        doc = io.BytesIO(json_bytes)
        doc.name = f"Snapp_Full_Backup_{now_tag}.json"
        await query.message.reply_document(
            doc,
            caption=(
                "📦 *بکاپ کامل دیتابیس اسنپ (شامل LocalStorage و گزارش کوپن‌ها)*\n"
                f"🔢 تعداد نشست‌ها: `{len(records)}`"
            ),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_help":
        await query.answer()
        help_text = (
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🔍 *راهنمای دستورات مدیریتی، کوپن و سهمیه‌بندی*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "1️⃣ *استعلام آنلاین کوپن‌ها و امتیاز یک اکانت:*\n"
            "`/check Snapp-XXXX-XXXX`\n"
            "`/check 09123456789`\n\n"
            "2️⃣ *اسکن یکجای کوپن‌ها و امتیازات کل دیتابیس:*\n"
            "`/check_all`\n\n"
            "3️⃣ *تعریف کد اشتراک برای دوست با سقف تعداد:*\n"
            "`/add_user Ali 50`\n\n"
            "4️⃣ *افزایش یا تغییر سقف سهمیه دوست:*\n"
            "`/set_limit SUB-XXXXXX 120`\n\n"
            "5️⃣ *مسدود کردن کد اشتراک دوست:*\n"
            "`/block_user SUB-XXXXXX`\n\n"
            "6️⃣ *جستجو و دریافت فایل لایسنس:*\n"
            "`/get Snapp-XXXX-XXXX`\n"
            "`/get 09123456789`\n"
            "`/get SUB-XXXXXX`\n\n"
            "7️⃣ *حذف یک لایسنس مشخص:*\n"
            "`/delete Snapp-XXXX-XXXX`"
        )
        await query.edit_message_text(help_text, reply_markup=kb_back_to_main(), parse_mode="Markdown")

    elif data_cb == "snapp_clear_confirm":
        await query.answer()
        confirm_kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ بله، کل دیتابیس پاک شود", callback_data="snapp_clear_execute"),
                InlineKeyboardButton("❌ انصراف", callback_data="snapp_home"),
            ]
        ])
        await query.edit_message_text(
            "⚠️ *هشدار جدی:*\n\n"
            f"آیا از حذف تمامی `{len(records)}` لایسنس ذخیره شده در دیتابیس اسنپ اطمینان دارید؟\n"
            "پیشنهاد می‌شود پیش از حذف، یک بکاپ JSON تهیه کنید.",
            reply_markup=confirm_kb,
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_clear_execute":
        keys = db.keys(f"{REDIS_PREFIX}*")
        deleted_count = db.delete(*keys) if keys else 0
        await query.answer(f"✅ {deleted_count} لایسنس حذف شد.", show_alert=True)
        await query.edit_message_text(
            get_welcome_text(),
            reply_markup=kb_snapp_admin(),
            parse_mode="Markdown",
        )


# ======================== راه‌اندازی ربات داخل چرخه حیات FastAPI ========================
tg_application = None


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    global tg_application
    if TELEGRAM_BOT_TOKEN and TELEGRAM_BOT_TOKEN != "توکن_ربات_اسنپ_را_اینجا_بگذارید":
        try:
            tg_application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
            tg_application.add_handler(CommandHandler("start", start_snapp_bot))
            tg_application.add_handler(CommandHandler("admin", start_snapp_bot))
            tg_application.add_handler(CommandHandler("add_user", add_user_cmd))
            tg_application.add_handler(CommandHandler("set_limit", set_limit_cmd))
            tg_application.add_handler(CommandHandler("block_user", block_user_cmd))
            tg_application.add_handler(CommandHandler("check", check_rewards_cmd))
            tg_application.add_handler(CommandHandler("check_all", check_all_rewards_cmd))
            tg_application.add_handler(CommandHandler("get", get_license_cmd))
            tg_application.add_handler(CommandHandler("delete", delete_snapp_license))
            tg_application.add_handler(CallbackQueryHandler(snapp_callbacks, pattern="^snapp_"))

            await tg_application.initialize()
            await tg_application.bot.delete_webhook(drop_pending_updates=True)
            await tg_application.start()
            await tg_application.updater.start_polling(drop_pending_updates=True)
            logger.info("🤖 ربات مدیریت اسنپ (همراه با ماژول بررسی کوپن و امتیازات) با موفقیت استارت شد!")
        except Exception as e:
            logger.error(f"❌ خطا در استارت ربات تلگرام: {e}")
    else:
        logger.critical("❌ متغیر TELEGRAM_BOT_TOKEN یا BOT_TOKEN در Railway تنظیم نشده است!")

    yield

    if tg_application:
        try:
            await tg_application.updater.stop()
            await tg_application.stop()
            await tg_application.shutdown()
        except Exception:
            pass


# ======================== وب‌سرور FastAPI ========================
class SnappData(BaseModel):
    cookies: str = "empty"
    local_storage: str
    phone_number: str = "unknown"
    partner_code: str = ""


app = FastAPI(title="Snapp License, Quota & Rewards API", docs_url=None, redoc_url=None, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/snapp/partner-status/{partner_code}")
async def check_partner_status(partner_code: str):
    """بررسی وضعیت و سهمیه باقی‌مانده کد اشتراک همکار از داخل برنامه اندروید."""
    rk, pdata = get_partner_record(partner_code)
    if not rk or not pdata:
        raise HTTPException(status_code=404, detail="کد اشتراک یافت نشد")
    used = int(pdata.get("used", 0))
    limit = int(pdata.get("limit", 0))
    return {
        "success": True,
        "name": pdata.get("name", "همکار"),
        "code": pdata.get("code"),
        "active": bool(pdata.get("active", True)),
        "used": used,
        "limit": limit,
        "remaining": max(0, limit - used),
    }


@app.post("/api/snapp/create-license")
@app.post("/api/tapsi/create-license")
async def create_snapp_license(data: SnappData):
    db = get_redis()
    if not db:
        raise HTTPException(status_code=503, detail="Database unavailable")

    # الزام قطعی به وارد کردن کد اشتراک معتبر قبل از صدور هرگونه لایسنس
    partner_code = (data.partner_code or "").strip().upper()
    if not partner_code:
        raise HTTPException(
            status_code=403,
            detail="بدون وارد کردن کد اشتراک امکان صدور لایسنس وجود ندارد. ابتدا کد اشتراک خود را در بالای برنامه ثبت کنید.",
        )

    rk, pdata = get_partner_record(partner_code)
    if not rk or not pdata:
        raise HTTPException(
            status_code=403,
            detail="کد اشتراک وارد شده معتبر نیست. لطفاً کد صحیح را از مدیر دریافت کنید.",
        )
    if not pdata.get("active", True):
        raise HTTPException(
            status_code=403,
            detail="این کد اشتراک توسط مدیریت غیرفعال شده است.",
        )

    used = int(pdata.get("used", 0))
    limit = int(pdata.get("limit", 0))
    if used >= limit:
        raise HTTPException(
            status_code=403,
            detail=f"سهمیه ساخت لایسنس شما به پایان رسیده است ({used}/{limit}). جهت شارژ مجدد با مدیر هماهنگ کنید.",
        )

    raw_storage = (data.local_storage or "").strip()
    if not raw_storage or raw_storage == "{}":
        raise HTTPException(status_code=400, detail="LocalStorage خالی است؛ ابتدا وارد حساب اسنپ شوید.")

    used += 1
    pdata["used"] = used
    db.set(rk, json.dumps(pdata, ensure_ascii=False))

    partner_name = pdata.get("name", "همکار")
    quota_used = used
    quota_limit = limit
    quota_remaining = max(0, limit - used)

    license_key = generate_snapp_license()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    detected_phone = extract_phone_from_local_storage(raw_storage, data.phone_number)

    # استعلام خودکار کوپن‌های تخفیف، موجودی کیف‌پول و پروفایل هنگام صدور لایسنس
    rewards_info = await inspect_snapp_account_rewards(raw_storage)
    if rewards_info.get("cellphone") and detected_phone in ("", "unknown", "نامشخص"):
        detected_phone = rewards_info["cellphone"]

    try:
        storage_obj = json.loads(raw_storage)
        storage_keys_count = len(storage_obj) if isinstance(storage_obj, dict) else 1
    except Exception:
        storage_keys_count = 1

    redis_data = {
        "license_key": license_key,
        "phone_number": detected_phone,
        "local_storage": raw_storage,
        "cookies": data.cookies or "empty",
        "storage_keys_count": storage_keys_count,
        "partner_code": partner_code,
        "partner_name": partner_name,
        "created_at": now_str,
        "rewards": rewards_info,
    }

    try:
        db.set(f"{REDIS_PREFIX}{license_key}", json.dumps(redis_data, ensure_ascii=False))
    except Exception as e:
        logger.error(f"Redis save error: {e}")
        raise HTTPException(status_code=500, detail="Database save error")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_BOT_TOKEN != "توکن_ربات_اسنپ_را_اینجا_بگذارید":
        bot = tg_application.bot if tg_application else Bot(token=TELEGRAM_BOT_TOKEN)
        fullname = rewards_info.get("fullname", "نامشخص")
        credit_disp = rewards_info.get("credit_display", "۰ ریال")
        tot_v = rewards_info.get("total_vouchers", 0)
        cab_v = rewards_info.get("cab_vouchers_count", 0)
        club_v = rewards_info.get("club_vouchers_count", 0)
        pro_str = "🌟 فعال" if rewards_info.get("is_pro") else "ندارد"

        voucher_lines = []
        for v in (rewards_info.get("vouchers") or [])[:5]:
            v_code = v.get("promotion_code") or "خودکار"
            voucher_lines.append(f"  • {v.get('venture_title')}: {v.get('title')} (`{v_code}`)")
        vouchers_preview = "\n".join(voucher_lines) if voucher_lines else "  • بدون کوپن فعال"

        for admin_id in ALLOWED_USER_IDS:
            try:
                msg = (
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    "🟢 *لایسنس جدید اسنپ صادر و بررسی شد!*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🔑 لایسنس: `{license_key}`\n"
                    f"📱 شماره موبایل: `{detected_phone}`\n"
                    f"👤 نام حساب: `{fullname}`\n"
                    f"💰 موجودی کیف‌پول: `{credit_disp}`\n"
                    f"👑 اشتراک اسنپ‌پرو: `{pro_str}`\n"
                    f"🎫 کوپن‌های فعال: `{tot_v}` عدد (🚕 سفر: `{cab_v}` | 🏆 کلاب: `{club_v}`)\n"
                    f"{vouchers_preview}\n\n"
                    f"👤 سازنده: `{partner_name}` (`{partner_code}`)\n"
                    f"📊 سهمیه مصرف‌شده: `{quota_used}/{quota_limit}` (باقی‌مانده: `{quota_remaining}`)\n"
                    f"⏰ زمان ثبت: `{now_str}`\n"
                    f"🔎 جزئیات کامل کوپن‌ها: `/check {license_key}`"
                )
                await bot.send_message(chat_id=admin_id, text=msg, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")

    return JSONResponse(
        content={
            "success": True,
            "license_key": license_key,
            "phone_number": detected_phone,
            "fullname": rewards_info.get("fullname", ""),
            "credit_toman": rewards_info.get("credit_toman", 0),
            "total_vouchers": rewards_info.get("total_vouchers", 0),
            "cab_vouchers_count": rewards_info.get("cab_vouchers_count", 0),
            "partner_name": partner_name,
            "quota_used": quota_used,
            "quota_limit": quota_limit,
            "quota_remaining": quota_remaining,
            "message": "Snapp license created and rewards checked successfully",
        }
    )


@app.get("/api/snapp/check-rewards/{license_key}")
async def api_check_license_rewards(license_key: str):
    """اندپوینت بررسی زنده کوپن‌ها، امتیازات و موجودی یک لایسنس."""
    db = get_redis()
    if not db:
        raise HTTPException(status_code=503, detail="Database unavailable")

    rk, raw = find_redis_key_for_license(license_key)
    if not rk or not raw:
        raise HTTPException(status_code=404, detail="لایسنس یافت نشد")

    record = json.loads(raw)
    rw = await inspect_snapp_account_rewards(record.get("local_storage", "{}"))
    record["rewards"] = rw
    db.set(rk, json.dumps(record, ensure_ascii=False))
    return {"success": True, "license_key": record.get("license_key"), "phone_number": record.get("phone_number"), "rewards": rw}


@app.get("/api/snapp/get-license/{license_key}")
@app.get("/api/tapsi/get-license/{license_key}")
async def get_snapp_license(license_key: str):
    clean_key = (license_key or "").strip()
    if not (clean_key.lower().startswith("snapp-") or clean_key.lower().startswith("tapsi-")):
        raise HTTPException(status_code=400, detail="فرمت لایسنس نامعتبر است (باید با Snapp- شروع شود)")
    db = get_redis()
    if not db:
        raise HTTPException(status_code=503, detail="Database unavailable")

    _, raw = find_redis_key_for_license(clean_key)
    if not raw:
        raise HTTPException(status_code=404, detail="لایسنس مورد نظر یافت نشد یا منقضی شده است")

    return JSONResponse(content={"success": True, "data": json.loads(raw)})


@app.get("/health")
async def health_check():
    db = get_redis()
    db_status = "connected" if db else "disconnected"
    bot_status = "running" if tg_application else "stopped"
    return {"status": "ok", "service": "snapp-license-manager", "database": db_status, "bot": bot_status}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
