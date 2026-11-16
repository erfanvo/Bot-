# -*- coding: utf-8 -*-
import logging
import json
import os
import re
import csv
import uuid
import asyncio
import redis
import io
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

# اگر REQUIRE_PARTNER_CODE برابر false باشد، بدون کد اشتراک هم می‌توان لایسنس ساخت؛ به صورت پیش‌فرض برای امنیت سهمیه فعال است
REQUIRE_PARTNER_CODE = os.getenv("REQUIRE_PARTNER_CODE", "true").lower() != "false"

REDIS_URL = os.getenv("REDIS_URL")
PORT = int(os.getenv("PORT", 8000))
REDIS_PREFIX = "snapp:license:"
PARTNER_PREFIX = "snapp:partner:"

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


# ======================== کیبوردها و هندلرهای ربات تلگرام ========================
def kb_snapp_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
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
    db_badge = "🟢 متصل" if db else "🔴 قطع (متغیر REDIS_URL را بررسی کنید)"
    return (
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🚀 *پنل مدیریت حرفه‌ای لایسنس و سهمیه اسنپ*\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📡 وضعیت دیتابیس ردیس: *{db_badge}*\n"
        f"🗄 مجموع لایسنس‌های ثبت‌شده: `{len(records)}`\n"
        f"👥 تعداد کدهای اشتراک دوستان: `{len(partners)}`\n\n"
        "از منوی زیر گزارش، خروجی یا مدیریت سهمیه همکاران را انتخاب کنید:"
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
        "💡 این کد (`" + code + "`) را به دوستتان بدهید تا در بخش «🔑 کد اشتراک» بالای برنامه وارد کند."
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
    """مسدود یا حذف کردن کد اشتراک دوست: /block_user SUB-XXXXXX"""
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
    records = fetch_all_snapp_records()
    matched = [
        r for r in records
        if r.get("license_key", "").lower() == query_str.lower()
        or r.get("phone_number", "") == query_str
        or r.get("partner_code", "").lower() == query_str.lower()
    ]

    if not matched:
        await update.effective_message.reply_text(f"❌ هیچ رکوردی برای `{query_str}` یافت نشد.", parse_mode="Markdown")
        return

    for item in matched[:5]:
        lic = item.get("license_key", "-")
        phone = item.get("phone_number", "نامشخص")
        created = item.get("created_at", "-")
        partner_name = item.get("partner_name", "ادمین")
        storage_str = item.get("local_storage", "{}")

        doc = io.BytesIO(storage_str.encode("utf-8"))
        doc.name = f"{lic}_{phone}.json"
        caption = (
            "🔍 *اطلاعات لایسنس یافت‌شده:*\n\n"
            f"🔑 لایسنس: `{lic}`\n"
            f"📱 شماره: `{phone}`\n"
            f"👤 سازنده: `{partner_name}`\n"
            f"⏰ زمان ثبت: `{created}`\n"
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

        text = (
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📊 *گزارش آماری دیتابیس اسنپ*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🗂 کل لایسنس‌های موجود: `{total}`\n"
            f"📱 شماره‌های یکتای شناسایی‌شده: `{len(known_phones)}`\n"
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
            ptag = f" ({pname})" if pname else ""
            lines.append(f"{idx}. 📱 `{phone}` ➔ `{lic}`{ptag}")

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
            pname = r.get("partner_name", "آزاد/ادمین")
            created = r.get("created_at", "-")
            lines.append(f"[{idx}] شماره: {phone} | لایسنس: {lic} | سازنده: {pname} | زمان: {created}")

        doc = io.BytesIO("\n".join(lines).encode("utf-8"))
        doc.name = f"Snapp_License_Phone_Report_{now_tag}.txt"
        await query.message.reply_document(
            doc,
            caption=(
                "📄 *گزارش کامل لایسنس‌ها به همراه شماره موبایل و نام سازنده*\n"
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
        writer.writerow(["ردیف", "کلید لایسنس", "شماره موبایل", "سازنده (همکار)", "کد اشتراک", "تعداد کلیدهای LocalStorage", "زمان ثبت"])
        for idx, r in enumerate(records, start=1):
            writer.writerow([
                idx,
                r.get("license_key", ""),
                r.get("phone_number", "نامشخص"),
                r.get("partner_name", "ادمین"),
                r.get("partner_code", "-"),
                r.get("storage_keys_count", "-"),
                r.get("created_at", ""),
            ])

        doc = io.BytesIO(output.getvalue().encode("utf-8-sig"))
        doc.name = f"Snapp_Report_{now_tag}.csv"
        await query.message.reply_document(
            doc,
            caption=f"📊 *فایل اکسل (CSV) گزارش لایسنس‌ها، شماره‌ها و سهمیه همکاران*\n🔢 تعداد: `{len(records)}`",
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
                "📦 *بکاپ کامل دیتابیس اسنپ (شامل LocalStorage)*\n"
                f"🔢 تعداد نشست‌ها: `{len(records)}`"
            ),
            parse_mode="Markdown",
        )

    elif data_cb == "snapp_help":
        await query.answer()
        help_text = (
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🔍 *راهنمای دستورات مدیریتی و سهمیه‌بندی*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "1️⃣ *تعریف کد اشتراک برای دوست با سقف تعداد:*\n"
            "`/add_user Ali 50`\n\n"
            "2️⃣ *افزایش یا تغییر سقف سهمیه دوست:*\n"
            "`/set_limit SUB-XXXXXX 120`\n\n"
            "3️⃣ *مسدود کردن کد اشتراک دوست:*\n"
            "`/block_user SUB-XXXXXX`\n\n"
            "4️⃣ *جستجوی لایسنس، شماره یا کد همکار:*\n"
            "`/get Snapp-XXXX-XXXX`\n"
            "`/get 09123456789`\n"
            "`/get SUB-XXXXXX`\n\n"
            "5️⃣ *حذف یک لایسنس مشخص:*\n"
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
            tg_application.add_handler(CommandHandler("get", get_license_cmd))
            tg_application.add_handler(CommandHandler("delete", delete_snapp_license))
            tg_application.add_handler(CallbackQueryHandler(snapp_callbacks, pattern="^snapp_"))

            await tg_application.initialize()
            await tg_application.bot.delete_webhook(drop_pending_updates=True)
            await tg_application.start()
            await tg_application.updater.start_polling(drop_pending_updates=True)
            logger.info("🤖 ربات مدیریت اسنپ و سهمیه‌بندی با موفقیت استارت شد!")
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


app = FastAPI(title="Snapp License & Quota API", docs_url=None, redoc_url=None, lifespan=lifespan)

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

    raw_storage = (data.local_storage or "").strip()
    if not raw_storage or raw_storage == "{}":
        raise HTTPException(status_code=400, detail="LocalStorage is empty")

    # بررسی کد اشتراک و سهمیه در صورت وجود کد اشتراک در دیتابیس
    partner_code = (data.partner_code or "").strip().upper()
    existing_partners = db.keys(f"{PARTNER_PREFIX}*")

    partner_name = "ادمین / آزاد"
    quota_used = 0
    quota_limit = 0
    quota_remaining = -1

    if partner_code or (REQUIRE_PARTNER_CODE and len(existing_partners) > 0):
        if not partner_code:
            raise HTTPException(
                status_code=403,
                detail="لطفاً ابتدا کد اشتراک (سهمیه) خود را در بالای برنامه وارد کنید.",
            )
        rk, pdata = get_partner_record(partner_code)
        if not rk or not pdata:
            raise HTTPException(status_code=403, detail="کد اشتراک وارد شده نامعتبر است.")
        if not pdata.get("active", True):
            raise HTTPException(status_code=403, detail="این کد اشتراک توسط مدیریت مسدود شده است.")

        used = int(pdata.get("used", 0))
        limit = int(pdata.get("limit", 0))
        if used >= limit:
            raise HTTPException(
                status_code=403,
                detail=f"سهمیه ساخت لایسنس شما به پایان رسیده است ({used}/{limit}). جهت شارژ مجدد با مدیر هماهنگ کنید.",
            )

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
        "partner_code": partner_code or "-",
        "partner_name": partner_name,
        "created_at": now_str,
    }

    try:
        db.set(f"{REDIS_PREFIX}{license_key}", json.dumps(redis_data, ensure_ascii=False))
    except Exception as e:
        logger.error(f"Redis save error: {e}")
        raise HTTPException(status_code=500, detail="Database save error")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_BOT_TOKEN != "توکن_ربات_اسنپ_را_اینجا_بگذارید":
        bot = tg_application.bot if tg_application else Bot(token=TELEGRAM_BOT_TOKEN)
        quota_line = (
            f"👤 سازنده: `{partner_name}` (`{quota_used}/{quota_limit}` - باقی‌مانده: `{quota_remaining}`)\n"
            if quota_limit > 0
            else f"👤 سازنده: `{partner_name}`\n"
        )
        for admin_id in ALLOWED_USER_IDS:
            try:
                msg = (
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    "🟢 *لایسنس جدید اسنپ صادر شد!*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🔑 لایسنس: `{license_key}`\n"
                    f"📱 شماره موبایل: `{detected_phone}`\n"
                    f"{quota_line}"
                    f"🗂 تعداد کلیدهای LocalStorage: `{storage_keys_count}`\n"
                    f"⏰ زمان ثبت: `{now_str}`\n\n"
                    "✅ اطلاعات LocalStorage با موفقیت در ردیس ذخیره شد."
                )
                await bot.send_message(chat_id=admin_id, text=msg, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")

    return JSONResponse(
        content={
            "success": True,
            "license_key": license_key,
            "phone_number": detected_phone,
            "partner_name": partner_name,
            "quota_used": quota_used,
            "quota_limit": quota_limit,
            "quota_remaining": quota_remaining,
            "message": "Snapp license created successfully",
        }
    )


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
