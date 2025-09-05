# pip install python-telegram-bot==21.4 requests beautifulsoup4 lxml pytz python-dateutil

import os, json, re, logging, time, asyncio
from datetime import datetime, timedelta
import pytz, requests
from bs4 import BeautifulSoup
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, ContextTypes, ConversationHandler,
    MessageHandler, filters
)
from telegram.error import TelegramError
from dotenv import load_dotenv

# Load .env dari working directory
load_dotenv()

# ========================= CONFIG =========================
BASE = "https://bromotenggersemeru.id"
SITE_PATH = "/booking/site/lembah-watangan"
CAP_URL = f"{BASE}/website/home/get_view"
ACTION_URL = f"{BASE}/website/booking/action"
ASIA_JAKARTA = pytz.timezone("Asia/Jakarta")

ID_SITE   = "4"      # Bromo
ID_SECTOR = "1"      # Gunung Bromo
SITE_LABEL = "Bromo"

STORAGE_FILE = "storage.json"   # { "<user_id>": {"ci_session": "...", "jobs": {...}} }

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bromo-bot")

MONTHS_ID = {
    "januari":"01","februari":"02","maret":"03","april":"04","mei":"05","juni":"06",
    "juli":"07","agustus":"08","september":"09","oktober":"10","november":"11","desember":"12"
}

# =================== STORAGE ===================
def load_storage():
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}
def save_storage(data):
    with open(STORAGE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
storage = load_storage()
def get_ci(uid: str) -> str:
    return storage.get(uid, {}).get("ci_session", "")
def set_ci(uid: str, ci: str):
    storage.setdefault(uid, {})["ci_session"] = ci
    save_storage(storage)
def get_jobs_store(uid: str) -> dict:
    storage.setdefault(uid, {})
    storage[uid].setdefault("jobs", {})
    return storage[uid]["jobs"]

# =================== HELPERS ===================
def parse_date_indo_to_iso(date_str: str) -> str:
    s = date_str.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):  # YYYY-MM-DD
        return s
    if re.fullmatch(r"\d{2}-\d{2}-\d{4}", s):  # DD-MM-YYYY
        d, m, y = s.split("-"); return f"{y}-{m}-{d}"
    if "," in s: s = s.split(",", 1)[1].strip()
    parts = s.split()
    if len(parts) == 3 and parts[1].lower() in MONTHS_ID:
        day = parts[0].zfill(2); month = MONTHS_ID[parts[1].lower()]; year = parts[2]
        return f"{year}-{month}-{day}"
    raise ValueError("Format tanggal tidak dikenali.")

def year_month_from_iso(iso: str) -> str: return iso[:7]
def extract_int(text: str) -> int:
    m = re.findall(r"\d+", text); return int("".join(m)) if m else 0
def build_referer_url(iso_date: str) -> str:
    return f"{BASE}{SITE_PATH}?date_depart={iso_date}"

def slugify(s: str, maxlen: int = 18) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    if len(s) > maxlen:
        s = s[:maxlen].rstrip("-")
    return s or "ketua"

def get_tokens_from_cnt_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    holder = soup.select_one(".cnt-page")
    if not holder: raise RuntimeError("Tidak menemukan .cnt-page di HTML.")
    data = json.loads(holder.get_text("", strip=True))
    booking = data["booking"]
    return booking.get("secret"), booking.get("form_hash"), booking

def find_quota_for_date(rows, iso_date: str):
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 2: continue
        tanggal_text = " ".join(tds[0].stripped_strings)
        try: iso_from_cell = parse_date_indo_to_iso(tanggal_text)
        except: continue
        if iso_from_cell == iso_date:
            quota_text = " ".join(tds[1].stripped_strings)
            quota = extract_int(quota_text)
            status = "Tersedia" if quota > 0 else "Habis / Tidak tersedia"
            url_detail = f"{BASE}/peraturan/lembah-watangan?date_depart={iso_date}"
            return {"tanggal_cell": tanggal_text, "quota": quota, "status": status, "iso_date": iso_date, "url": url_detail}
    return None

def check_capacity(iso_date: str) -> dict | None:
    year_month = year_month_from_iso(iso_date)
    payload = {"action": "kapasitas", "year_month": year_month, "id_site": ID_SITE}
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.post(CAP_URL, data=payload, headers=headers, timeout=30)
    log.info(
        "check_capacity response iso=%s status=%s body=%s",
        iso_date,
        resp.status_code,
        resp.text,
    )
    soup = BeautifulSoup(resp.text, "lxml")
    rows = soup.select("table.table tbody tr")
    return find_quota_for_date(rows, iso_date)

def make_session_with_cookies(ci_session: str, extra_cookies: dict | None = None):
    sess = requests.Session()
    ua = ('Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
          'AppleWebKit/537.36 (KHTML, like Gecko) '
          'Chrome/139.0.0.0 Mobile Safari/537.36 Edg/139.0.0.0')
    sess.headers.update({"User-Agent": ua, "Accept": "*/*", "Accept-Language": "id,en;q=0.9,en-GB;q=0.8,en-US;q=0.7"})

    # set cookies per-job
    if extra_cookies:
        if extra_cookies.get("_ga"):
            sess.cookies.set("_ga", extra_cookies["_ga"], domain=".bromotenggersemeru.id", path="/")
        if extra_cookies.get("_ga_TMVP85FKW9"):
            sess.cookies.set("_ga_TMVP85FKW9", extra_cookies["_ga_TMVP85FKW9"], domain=".bromotenggersemeru.id", path="/")
        if extra_cookies.get("ci_session"):
            sess.cookies.set("ci_session", extra_cookies["ci_session"], domain="bromotenggersemeru.id", path="/")

    # fallback dari user-level (global) jika job-level kosong
    if ci_session and not sess.cookies.get("ci_session"):
        sess.cookies.set("ci_session", ci_session, domain="bromotenggersemeru.id", path="/")
    return sess

def add_or_update_members(sess: requests.Session, secret: str, male: int, female: int, id_country: str = "99"):
    if male < 0 or female < 0: return
    if male == 0 and female == 0: return
    payload = {"action": "anggota_update", "secret": secret, "id": "", "male": str(male), "female": str(female), "id_country": id_country}
    try: _ = sess.post(ACTION_URL, data=payload, timeout=30)
    except Exception as e: log.warning("anggota_update error: %s", e)

def do_booking_flow(ci_session: str, iso_date: str, profile: dict, job_cookies: dict | None = None) -> tuple[bool, str, float, dict | None]:
    """
    Jalankan alur booking lengkap.
    Return: (ok, message_str, elapsed_seconds, raw_json_or_None)
    """
    t0 = time.perf_counter()

    sess = make_session_with_cookies(ci_session, job_cookies)
    referer = build_referer_url(iso_date)
    r = sess.get(referer, timeout=30)
    if r.status_code != 200:
        return False, f"Gagal GET booking page: {r.status_code}", time.perf_counter()-t0, None
    try:
        secret, form_hash, _ = get_tokens_from_cnt_page(r.text)
    except Exception as e:
        return False, f"Gagal ekstrak token: {e}", time.perf_counter()-t0, None

    sess.headers.update({"X-Requested-With": "XMLHttpRequest", "Origin": BASE, "Referer": referer})
    try:
        _ = sess.post(ACTION_URL, data={"action":"update_hash","secret":secret,"form_hash":form_hash}, timeout=30)
        _ = sess.post(ACTION_URL, data={"action":"validate_booking","secret":secret,"form_hash":form_hash}, timeout=30)
    except Exception as e:
        return False, f"Gagal update/validate hash: {e}", time.perf_counter()-t0, None

    # anggota
    male = int(profile.get("male", "0") or 0)
    female = int(profile.get("female", "0") or 0)
    add_or_update_members(sess, secret, male, female, profile.get("id_country", "99"))

    payload = {
        "action": "do_booking",
        "secret": secret,
        "id_sector": ID_SECTOR,
        "form_hash": form_hash,
        "site": SITE_LABEL,
        "id_gate": profile.get("id_gate", "2"),
        "id_vehicle": profile.get("id_vehicle", "2"),
        "vehicle_count": profile.get("vehicle_count", "1"),
        "date_depart": iso_date,
        "date_arrival": iso_date,
        "name": profile.get("name", ""),
        "id_country": profile.get("id_country", "99"),
        "birthdate": profile.get("birthdate", ""),
        "id_gender": profile.get("id_gender", "1"),
        "id_identity": profile.get("id_identity", "1"),
        "identity_no": profile.get("identity_no", ""),
        "address": profile.get("address", ""),
        "id_province": profile.get("id_province", ""),
        "id_district": profile.get("id_district", ""),
        "hp": profile.get("hp", ""),
        "table-booking-detail_length": "10",
        "bank": profile.get("bank", "qris"),
        "termsCheckbox": "on"
    }
    try:
        resp = sess.post(ACTION_URL, data=payload, timeout=60)
    except Exception as e:
        return False, f"Gagal POST do_booking: {e}", time.perf_counter()-t0, None

    elapsed = time.perf_counter() - t0
    ct = (resp.headers.get("Content-Type") or "").lower()

    if "json" in ct:
        try:
            data = resp.json()
        except Exception:
            return False, f"Respon tidak bisa dibaca JSON: {resp.text[:400]}", elapsed, None

        if data.get("status") is True:
            link = data.get("booking_link") or data.get("link_redirect") or "(tidak ada link)"
            msg = f"Booking BERHASIL.\nLink: {link}\nServer message: {data.get('message','-')}"
            return True, msg, elapsed, data
        else:
            msg = f"Booking GAGAL: {data.get('message') or data}"
            return False, msg, elapsed, data

    return False, f"Respon non-JSON: {resp.text[:400]}", elapsed, None

# =================== FORM PARSER ===================
FORM_KEYS = {
    "nama": "name",
    "no ktp": "identity_no",
    "no hp": "hp",
    "pintu masuk (1-4)": "id_gate",
    "jenis kendaraan": "id_vehicle",
    "jumlah kendaraan": "vehicle_count",
    "metode bayar": "bank",
    "jumlah laki-laki": "male",
    "jumlah perempuan": "female",
    "tanggal lahir": "birthdate",
    "alamat": "address",
    "id provinsi": "id_province",
    "id kabupaten/kota": "id_district",

    # tambahan: cookie & reminder
    "_ga": "_ga",
    "_ga_tmpvp85fkw9": "_ga_TMVP85FKW9",   # toleransi case
    "_ga_tmvp85fkw9": "_ga_TMVP85FKW9",
    "ci_session": "ci_session",
    "ingatkan (menit)": "reminder_minutes",
}
FORM_PROMPT = (
"""Silakan balas *dalam satu pesan* dengan format berikut:

Nama               : 
No KTP             : 
No HP              : 
Pintu Masuk (1-4)  : 
Jenis Kendaraan    : 
Jumlah Kendaraan   : 
Metode Bayar       : 
Jumlah Laki-laki   : 
Jumlah Perempuan   : 
Tanggal Lahir      : 
Alamat             : 
ID Provinsi        : 
ID Kabupaten/Kota  : 

# OPSIONAL (per Job):
_ga               : 
_ga_TMVP85FKW9    : 
ci_session        : 
Ingatkan (menit)  :  (contoh: 15)  -> bot akan kirim reminder sebelum eksekusi agar kamu update cookie

Keterangan:
- Pintu Masuk: 1=Pasuruan, 2=Malang, 3=Lumajang, 4=Probolinggo
- Jenis Kendaraan: 1=R4, 2=R2, 3=Sepeda, 4=Kuda, 6=Jalan Kaki
- Metode Bayar: qris / VA-Mandiri / VA-BNI
- Field kosong boleh dibiarkan (opsional)."""
)

def parse_form_block(text: str) -> tuple[dict, dict, int | None, list]:
    """
    Return: (profile_dict, cookies_dict, reminder_minutes, errors)
    """
    profile = {
        "id_country": "99", "id_gender": "1", "id_identity": "1",
        "id_gate": "2", "id_vehicle": "2", "vehicle_count": "1",
        "bank": "qris", "male": "0", "female": "0",
        "birthdate": "", "address": "", "id_province": "", "id_district": ""
    }
    cookies = {"_ga": "", "_ga_TMVP85FKW9": "", "ci_session": ""}
    reminder_minutes = None
    errors = []

    for raw_line in text.splitlines():
        if ":" not in raw_line: continue
        label, value = raw_line.split(":", 1)
        key = label.strip().lower()
        val = value.strip()
        if key in FORM_KEYS:
            mapped = FORM_KEYS[key]
            if mapped in {"_ga", "_ga_TMVP85FKW9", "ci_session"}:
                cookies[mapped] = val
            elif mapped == "reminder_minutes":
                if val:
                    if not val.isdigit() or int(val) < 0 or int(val) > 120:
                        errors.append("Ingatkan (menit) harus 0..120 (opsional).")
                    else:
                        reminder_minutes = int(val)
            else:
                profile[mapped] = val

    # Validasi minimal
    if not profile["name"]: errors.append("Nama wajib.")
    if not profile["identity_no"]: errors.append("No KTP wajib.")
    if not profile["hp"]: errors.append("No HP wajib.")
    if profile["id_gate"] and profile["id_gate"] not in {"1","2","3","4"}:
        errors.append("Pintu Masuk harus 1/2/3/4.")
    if profile["id_vehicle"] and profile["id_vehicle"] not in {"1","2","3","4","6"}:
        errors.append("Jenis Kendaraan harus 1/2/3/4/6.")
    if profile["vehicle_count"] and (not profile["vehicle_count"].isdigit() or not (1 <= int(profile["vehicle_count"]) <= 20)):
        errors.append("Jumlah Kendaraan harus 1-20.")
    if profile["bank"].lower() not in {"qris","va-mandiri","va-bni"}:
        errors.append("Metode Bayar harus qris / VA-Mandiri / VA-BNI.")
    for fld in ["male","female"]:
        if profile[fld] and (not profile[fld].isdigit() or not (0 <= int(profile[fld]) <= 19)):
            errors.append(f"{'Laki-laki' if fld=='male' else 'Perempuan'} harus 0–19.")
    if profile["birthdate"]:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", profile["birthdate"]):
            errors.append("Tanggal Lahir harus YYYY-MM-DD (atau kosong).")

    return profile, cookies, reminder_minutes, errors

# =================== TELEGRAM ===================
HELP_TEXT = (
    "Perintah:\n"
    "/start, /help\n"
    "/set_session <ci_session>\n"
    "/book <tanggal_booking>\n"
    "/schedule <tanggal_booking> <tgl_eksekusi> <HH:MM>\n"
    "/jobs — daftar job terjadwal\n"
    "/job_detail <job|index>\n"
    "/job_cancel <job|index>\n"
    "/job_edit_time <job|index> <exec_YYYY-MM-DD> <HH:MM>\n"
    "/job_edit_fields <job|index> key=value;key=value;...  (dukung booking_date=YYYY-MM-DD)\n"
    "/job_edit_when <job|index> <booking_YYYY-MM-DD> <exec_YYYY-MM-DD> <HH:MM>\n"
    "/job_update_cookies <job|index> _ga=...;_ga_TMVP85FKW9=...;ci_session=...\n"
    "/cancel\n\n"
    "Contoh: /book 2025-08-31\n"
    "Contoh: /schedule 2025-08-31 2025-08-20 07:59\n"
    "Contoh update cookies: /job_update_cookies 1 _ga=GA1.1...;_ga_TMVP85FKW9=GS2.1...;ci_session=abcdef\n"
    "Catatan: kamu juga bisa isi cookies & 'Ingatkan (menit)' langsung di form /book atau /schedule."
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Halo! Bot siap.\n\n" + HELP_TEXT)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)

async def set_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not context.args:
        await update.message.reply_text("Format: /set_session <ci_session>")
        return
    ci = context.args[0].strip()
    set_ci(uid, ci)
    await update.message.reply_text("ci_session disimpan ✅")

# ====== Conversations ======
BOOK_ASK_FORM, BOOK_CONFIRM = range(2)
SCHED_ASK_FORM, SCHED_CONFIRM = range(2)

async def book_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not get_ci(uid):
        await update.message.reply_text("Set dulu /set_session <ci_session> (ambil dari browser yang sudah login).")
        return ConversationHandler.END
    if not context.args:
        await update.message.reply_text("Format: /book <tanggal_booking> (YYYY-MM-DD / DD-MM-YYYY / 31 Agustus 2025).")
        return ConversationHandler.END
    try:
        iso = parse_date_indo_to_iso(" ".join(context.args))
    except Exception as e:
        await update.message.reply_text(f"Format tanggal salah: {e}")
        return ConversationHandler.END
    context.user_data.clear()
    context.user_data["booking_iso"] = iso
    await update.message.reply_text(FORM_PROMPT, parse_mode="Markdown")
    return BOOK_ASK_FORM

async def book_collect_form(update: Update, context: ContextTypes.DEFAULT_TYPE):
    form_text = update.message.text
    profile, cookies, reminder_minutes, errors = parse_form_block(form_text)
    if errors:
        await update.message.reply_text("Ada masalah pada input:\n- " + "\n- ".join(errors) + "\n\nSilakan kirim ulang sesuai format.")
        return BOOK_ASK_FORM
    context.user_data["profile"] = profile
    context.user_data["cookies"] = cookies
    context.user_data["reminder_minutes"] = reminder_minutes

    iso = context.user_data["booking_iso"]
    cap = await asyncio.to_thread(check_capacity, iso)
    if not cap:
        await update.message.reply_text(f"Tanggal {iso} tidak ditemukan di kalender bulan itu.")
        return ConversationHandler.END
    if cap["quota"] <= 0:
        await update.message.reply_text(f"{cap['tanggal_cell']}\nKuota: {cap['quota']} → Tidak tersedia.")
        return ConversationHandler.END

    total = 1 + int(profile["male"]) + int(profile["female"])
    cookie_hint = []
    for k in ["_ga","_ga_TMVP85FKW9","ci_session"]:
        v = cookies.get(k,"")
        cookie_hint.append(f"{k}={'(diisi)' if v else '(kosong)'}")
    remind_txt = f"{reminder_minutes} menit" if reminder_minutes is not None else "tidak"
    summary = (
        f"Tanggal Booking: {iso}\n"
        f"Nama: {profile['name']}\n"
        f"KTP: {profile['identity_no']} | HP: {profile['hp']}\n"
        f"Gate: {profile['id_gate']} | Kendaraan: {profile['id_vehicle']} x {profile['vehicle_count']}\n"
        f"Bayar: {profile['bank']} | L: {profile['male']} P: {profile['female']} | Total: {total}\n"
        f"Cookies: {', '.join(cookie_hint)} | Reminder: {remind_txt}\n\n"
        "Ketik 'YA' untuk konfirmasi booking sekarang, atau 'BATAL' untuk membatalkan."
    )
    await update.message.reply_text(summary)
    return BOOK_CONFIRM

async def book_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip().lower()
    if txt not in {"ya","y","yes"}:
        await update.message.reply_text("Dibatalkan.")
        return ConversationHandler.END
    uid = str(update.effective_user.id)
    ci = get_ci(uid)
    if not ci:
        await update.message.reply_text("ci_session (global) kosong. /set_session <ci_session> dulu atau isi di form.")
        return ConversationHandler.END

    ok, msg, elapsed_s, raw = do_booking_flow(
        ci,
        context.user_data["booking_iso"],
        context.user_data["profile"],
        job_cookies=context.user_data.get("cookies")
    )
    extra = ""
    if raw:
        server_msg = raw.get("message", "-")
        link = raw.get("booking_link") or raw.get("link_redirect") or "-"
        extra = f"\n\n[Server]\nmessage: {server_msg}\nlink: {link}"
    await update.message.reply_text(
        ("✅ " if ok else "❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra
    )
    return ConversationHandler.END

async def schedule_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not get_ci(uid):
        await update.message.reply_text("Set dulu /set_session <ci_session> (global). Kamu juga bisa override di form.")
        return ConversationHandler.END
    # /schedule <tgl_booking> <tgl_eksekusi> <HH:MM>
    if len(context.args) < 3:
        await update.message.reply_text("Format: /schedule <tgl_booking> <tgl_eksekusi> <HH:MM>")
        return ConversationHandler.END
    try:
        booking_iso = parse_date_indo_to_iso(context.args[0])
        exec_iso = parse_date_indo_to_iso(context.args[1])
    except Exception as e:
        await update.message.reply_text(f"Format tanggal salah: {e}")
        return ConversationHandler.END
    hhmm = context.args[2]
    if not re.fullmatch(r"\d{2}:\d{2}", hhmm):
        await update.message.reply_text("Jam harus HH:MM, contoh 07:59")
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data["booking_iso"] = booking_iso
    context.user_data["exec_iso"] = exec_iso
    context.user_data["time"] = hhmm
    await update.message.reply_text(FORM_PROMPT, parse_mode="Markdown")
    return SCHED_ASK_FORM

async def schedule_collect_form(update: Update, context: ContextTypes.DEFAULT_TYPE):
    form_text = update.message.text
    profile, cookies, reminder_minutes, errors = parse_form_block(form_text)
    if errors:
        await update.message.reply_text("Ada masalah pada input:\n- " + "\n- ".join(errors) + "\n\nSilakan kirim ulang sesuai format.")
        return SCHED_ASK_FORM
    context.user_data["profile"] = profile
    context.user_data["cookies"] = cookies
    context.user_data["reminder_minutes"] = reminder_minutes

    iso = context.user_data["booking_iso"]
    cap = await asyncio.to_thread(check_capacity, iso)
    if not cap:
        await update.message.reply_text(f"Tanggal {iso} tidak ditemukan.")
        return ConversationHandler.END

    total = 1 + int(profile["male"]) + int(profile["female"])
    cookie_hint = []
    for k in ["_ga","_ga_TMVP85FKW9","ci_session"]:
        v = cookies.get(k,"")
        cookie_hint.append(f"{k}={'(diisi)' if v else '(kosong)'}")
    remind_txt = f"{reminder_minutes} menit" if reminder_minutes is not None else "tidak"

    summary = (
        f"[Jadwal]\n"
        f"- Booking untuk: {context.user_data['booking_iso']}\n"
        f"- Dieksekusi pada: {context.user_data['exec_iso']} {context.user_data['time']} Asia/Jakarta\n"
        f"Kuota saat ini: {cap['quota']} → {cap['status']}\n"
        f"Nama: {profile['name']} | KTP: {profile['identity_no']} | HP: {profile['hp']}\n"
        f"Gate: {profile['id_gate']} | Kendaraan: {profile['id_vehicle']} x {profile['vehicle_count']}\n"
        f"Bayar: {profile['bank']} | L: {profile['male']} P: {profile['female']} | Total: {total}\n"
        f"Cookies: {', '.join(cookie_hint)} | Reminder: {remind_txt}\n\n"
        "Ketik 'YA' untuk membuat jadwal, atau 'BATAL' untuk membatalkan."
    )
    await update.message.reply_text(summary)
    return SCHED_CONFIRM

# ---- JobQueue helper
def require_job_queue(context: ContextTypes.DEFAULT_TYPE):
    jq = getattr(context.application, "job_queue", None)
    if jq is None:
        raise RuntimeError(
            "JobQueue tidak aktif. Install dgn: pip install 'python-telegram-bot[job-queue]' "
            "lalu restart bot."
        )
    return jq

def make_job_name(uid: str, leader_name: str, booking_iso: str, exec_iso: str, hhmm: str) -> str:
    slug = slugify(leader_name or "ketua")
    return f"book-{uid}-{slug}-{booking_iso}-{exec_iso}-{hhmm.replace(':','')}"

async def schedule_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip().lower()
    if txt not in {"ya","y","yes"}:
        await update.message.reply_text("Dibatalkan.")
        return ConversationHandler.END

    try:
        jq = require_job_queue(context)
    except RuntimeError as e:
        await update.message.reply_text(str(e))
        return ConversationHandler.END

    booking_iso = context.user_data["booking_iso"]
    exec_iso = context.user_data["exec_iso"]
    hh, mm = map(int, context.user_data["time"].split(":"))
    y, M, d = map(int, exec_iso.split("-"))
    run_at = ASIA_JAKARTA.localize(datetime(y, M, d, hh, mm, 0))
    if run_at < datetime.now(ASIA_JAKARTA):
        await update.message.reply_text("Waktu eksekusi sudah lewat di Asia/Jakarta. Dibatalkan.")
        return ConversationHandler.END

    uid = str(update.effective_user.id)
    leader = context.user_data["profile"].get("name","ketua")
    job_name = make_job_name(uid, leader, booking_iso, exec_iso, context.user_data["time"])

    # bersihkan job lama dg nama sama
    old = jq.get_jobs_by_name(job_name)
    for j in old: j.schedule_removal()

    # simpan ke storage (termasuk cookies & reminder)
    jobs_store = get_jobs_store(uid)
    jobs_store[job_name] = {
        "booking_iso": booking_iso,
        "exec_iso": exec_iso,
        "time": context.user_data["time"],
        "profile": context.user_data["profile"],
        "cookies": context.user_data.get("cookies", {}),
        "reminder_minutes": context.user_data.get("reminder_minutes"),
        "created_at": datetime.now(ASIA_JAKARTA).isoformat(),
        "chat_id": update.effective_chat.id
    }
    save_storage(storage)

    # jadwalkan job eksekusi
    jq.run_once(
        scheduled_job, when=run_at, name=job_name,
        data={
            "user_id": uid,
            "iso": booking_iso,
            "profile": context.user_data["profile"],
            "cookies": context.user_data.get("cookies", {})
        },
        chat_id=update.effective_chat.id
    )

    # jadwalkan reminder sebelum eksekusi (jika diminta)
    remind_min = context.user_data.get("reminder_minutes")
    if isinstance(remind_min, int) and remind_min > 0:
        remind_at = run_at - timedelta(minutes=remind_min)
        if remind_at > datetime.now(ASIA_JAKARTA):
            jq.run_once(
                reminder_job, when=remind_at, name=f"rem-{job_name}",
                data={"user_id": uid, "job_name": job_name},
                chat_id=update.effective_chat.id
            )

    await update.message.reply_text(
        f"Terjadwal ✅\n- Booking: {booking_iso}\n- Eksekusi: {exec_iso} {context.user_data['time']} (Asia/Jakarta)\n"
        f"- Reminder: {context.user_data.get('reminder_minutes','tidak')} menit sebelum\n"
        f"Job: {job_name}"
    )
    return ConversationHandler.END

async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    uid = str(data["user_id"])
    job_name = data["job_name"]
    rec = get_jobs_store(uid).get(job_name)
    if not rec:
        return
    chat_id = rec.get("chat_id")

    # Masking cookie agar aman
    def mask(s):
        if not s: return "(kosong)"
        return s[:6] + "..." + s[-4:]

    ck = rec.get("cookies", {})
    msg = (
        f"⏰ Reminder untuk job:\n{job_name}\n\n"
        f"Eksekusi: {rec['exec_iso']} {rec['time']} (Asia/Jakarta)\n"
        f"Booking: {rec['booking_iso']}\n\n"
        "Jika cookies berpotensi **expired**, update sekarang:\n"
        f"- _ga: {mask(ck.get('_ga'))}\n"
        f"- _ga_TMVP85FKW9: {mask(ck.get('_ga_TMVP85FKW9'))}\n"
        f"- ci_session: {mask(ck.get('ci_session'))}\n\n"
        "Gunakan perintah:\n"
        f"/job_update_cookies {job_name} _ga=<baru>;"
        f"_ga_TMVP85FKW9=<baru>;ci_session=<baru>\n"
    )
    await context.bot.send_message(chat_id, text=msg)

async def scheduled_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    uid = str(data["user_id"])
    iso = data["iso"]          # tanggal booking
    prof = data["profile"]
    job_cookies = data.get("cookies") or {}
    chat_id = context.job.chat_id
    job_name = context.job.name or ""

    ci = get_ci(uid)
    if not ci and not job_cookies.get("ci_session"):
        await context.bot.send_message(chat_id, text="[Jadwal] ci_session kosong/expired. /set_session atau /job_update_cookies dulu.")
        return

    cap = await asyncio.to_thread(check_capacity, iso)
    if not cap:
        await context.bot.send_message(chat_id, text=f"[Jadwal] {iso}: tanggal tidak ditemukan.")
        return
    await context.bot.send_message(chat_id, text=f"[Jadwal] {cap['tanggal_cell']}\nKuota: {cap['quota']} → {cap['status']}")
    if cap["quota"] <= 0:
        return

    ok, msg, elapsed_s, raw = await asyncio.to_thread(
        do_booking_flow, ci, iso, prof, job_cookies=job_cookies
    )
    extra = ""
    if raw:
        server_msg = raw.get("message","-")
        link = raw.get("booking_link") or raw.get("link_redirect") or "-"
        extra = f"\n[Server]\nmessage: {server_msg}\nlink: {link}"
    await context.bot.send_message(chat_id, text=("[Jadwal] ✅ " if ok else "[Jadwal] ❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra)
    trigger_next_cookie_job(context, uid, job_name, job_cookies, chat_id)


def trigger_next_cookie_job(context: ContextTypes.DEFAULT_TYPE, uid: str, current_name: str,
                            job_cookies: dict, chat_id: int) -> None:
    if not job_cookies:
        return
    jq = getattr(context.application, "job_queue", None)
    if jq is None:
        return
    now = datetime.now(ASIA_JAKARTA)
    jobs = get_jobs_store(uid)
    candidates = []
    for name, rec in jobs.items():
        if name == current_name:
            continue
        if rec.get("cookies") == job_cookies and jq.get_jobs_by_name(name):
            t = rec.get("time", "")
            fmt = "%Y-%m-%d %H:%M:%S" if t.count(":") == 2 else "%Y-%m-%d %H:%M"
            run_at = ASIA_JAKARTA.localize(datetime.strptime(f"{rec['exec_iso']} {t}", fmt))
            if run_at > now:
                candidates.append((run_at, name, rec))
    if not candidates:
        return
    candidates.sort(key=lambda x: x[0])
    _, next_name, rec = candidates[0]
    for j in jq.get_jobs_by_name(next_name):
        j.schedule_removal()
    jq.run_once(
        scheduled_job,
        when=datetime.now(ASIA_JAKARTA),
        name=next_name,
        data={
            "user_id": uid,
            "iso": rec["booking_iso"],
            "profile": rec["profile"],
            "cookies": rec.get("cookies", {}),
        },
        chat_id=chat_id,
    )

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Dialog dibatalkan.")
    return ConversationHandler.END

# -------- Global Error Handler ----------
async def on_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.exception("Update caused error", exc_info=context.error)
    try:
        if update and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Maaf, terjadi error tak terduga. Coba lagi ya."
            )
    except TelegramError:
        pass

# =================== JOB MANAGEMENT COMMANDS ===================
def resolve_job_selector(uid: str, selector: str) -> str | None:
    jobs_store = get_jobs_store(uid)
    if not jobs_store:
        return None
    names = sorted(jobs_store.keys())
    if selector.isdigit():
        idx = int(selector)
        if 1 <= idx <= len(names):
            return names[idx-1]
        return None
    return selector if selector in jobs_store else None

async def jobs_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    jobs_store = get_jobs_store(uid)
    if not jobs_store:
        await update.message.reply_text("Belum ada job terjadwal.")
        return

    jq = getattr(context.application, "job_queue", None)
    live_names = set()
    if jq:
        for j in jq.jobs():
            if j.name and j.chat_id:
                live_names.add(j.name)

    names = sorted(jobs_store.keys())
    lines = []
    for i, name in enumerate(names, start=1):
        rec = jobs_store[name]
        status = "AKTIF" if name in live_names else "TIDAK AKTIF"
        leader = rec['profile'].get('name','-')
        lines.append(
            f"{i}. {name}  [{status}]  "
            f"Booking: {rec['booking_iso']}  Eksekusi: {rec['exec_iso']} {rec['time']}  Ketua: {leader}"
        )
    await update.message.reply_text("Daftar Job:\n" + "\n".join(lines))

async def job_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not context.args:
        await update.message.reply_text("Format: /job_detail <job|index>")
        return
    job_name = resolve_job_selector(uid, " ".join(context.args).strip())
    if not job_name:
        await update.message.reply_text("Job tidak ditemukan.")
        return
    rec = get_jobs_store(uid).get(job_name)
    jq = getattr(context.application, "job_queue", None)
    live = False
    if jq:
        live = bool(jq.get_jobs_by_name(job_name))

    # Mask cookies in detail
    def mask(s):
        if not s: return "(kosong)"
        return s[:6] + "..." + s[-4:]
    ck = rec.get("cookies", {})
    safe_ck = {k: mask(ck.get(k)) for k in ["_ga","_ga_TMVP85FKW9","ci_session"]}

    msg = {
        "job": job_name,
        "status": "AKTIF" if live else "TIDAK AKTIF",
        "booking_iso": rec["booking_iso"],
        "exec_iso": rec["exec_iso"],
        "time": rec["time"],
        "reminder_minutes": rec.get("reminder_minutes"),
        "profile": rec["profile"],
        "cookies": safe_ck,
        "created_at": rec.get("created_at","-"),
        "chat_id": rec.get("chat_id","-")
    }
    await update.message.reply_text(json.dumps(msg, ensure_ascii=False, indent=2))

async def job_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not context.args:
        await update.message.reply_text("Format: /job_cancel <job|index>")
        return
    selector = " ".join(context.args).strip()
    job_name = resolve_job_selector(uid, selector)
    if not job_name:
        await update.message.reply_text("Job tidak ditemukan.")
        return
    try:
        jq = require_job_queue(context)
        for j in jq.get_jobs_by_name(job_name):
            j.schedule_removal()
        for j in jq.get_jobs_by_name(f"rem-{job_name}"):
            j.schedule_removal()
    except RuntimeError:
        pass
    jobs_store = get_jobs_store(uid)
    jobs_store.pop(job_name, None)
    save_storage(storage)
    await update.message.reply_text(f"Job '{job_name}' dibatalkan & dihapus.")

def parse_kv_pairs(s: str) -> dict:
    out = {}
    for part in s.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip()
    return out

def validate_and_merge_profile(old_prof: dict, kv: dict) -> tuple[dict, list, str | None]:
    prof = dict(old_prof)
    errors = []
    alias = {
        "gate": "id_gate", "vehicle": "id_vehicle", "kendaraan": "id_vehicle",
        "jumlah_kendaraan": "vehicle_count", "ktp": "identity_no", "hp": "hp",
        "l": "male", "p": "female", "payment": "bank", "bayar": "bank",
    }
    normalized = {}
    for k, v in kv.items():
        key = alias.get(k.lower(), k.lower())
        normalized[key] = v

    booking_date = normalized.pop("booking_date", None)
    for k, v in normalized.items():
        prof[k] = v

    _, errs = parse_form_block(
        "\n".join([
            f"Nama               : {prof.get('name','')}",
            f"No KTP             : {prof.get('identity_no','')}",
            f"No HP              : {prof.get('hp','')}",
            f"Pintu Masuk (1-4)  : {prof.get('id_gate','')}",
            f"Jenis Kendaraan    : {prof.get('id_vehicle','')}",
            f"Jumlah Kendaraan   : {prof.get('vehicle_count','')}",
            f"Metode Bayar       : {prof.get('bank','')}",
            f"Jumlah Laki-laki   : {prof.get('male','')}",
            f"Jumlah Perempuan   : {prof.get('female','')}",
            f"Tanggal Lahir      : {prof.get('birthdate','')}",
            f"Alamat             : {prof.get('address','')}",
            f"ID Provinsi        : {prof.get('id_province','')}",
            f"ID Kabupaten/Kota  : {prof.get('id_district','')}",
        ])
    )
    errors.extend(errs)
    return prof, errors if errors else [], booking_date

async def reschedule_job(context: ContextTypes.DEFAULT_TYPE, uid: str, old_name: str,
                         booking_iso: str, exec_iso: str, hhmm: str,
                         profile: dict, cookies: dict, reminder_minutes: int | None,
                         chat_id: int) -> str:
    jq = require_job_queue(context)
    # cancel old exec & reminder
    for j in jq.get_jobs_by_name(old_name): j.schedule_removal()
    for j in jq.get_jobs_by_name(f"rem-{old_name}"): j.schedule_removal()

    leader = profile.get("name","ketua")
    new_name = make_job_name(uid, leader, booking_iso, exec_iso, hhmm)

    # schedule exec
    hh, mm = map(int, hhmm.split(":"))
    y, M, d = map(int, exec_iso.split("-"))
    run_at = ASIA_JAKARTA.localize(datetime(y, M, d, hh, mm, 0))
    if run_at < datetime.now(ASIA_JAKARTA):
        raise ValueError("Waktu eksekusi baru sudah lewat di Asia/Jakarta.")
    jq.run_once(
        scheduled_job, when=run_at, name=new_name,
        data={"user_id": uid, "iso": booking_iso, "profile": profile, "cookies": cookies},
        chat_id=chat_id
    )
    # schedule reminder again
    if isinstance(reminder_minutes, int) and reminder_minutes > 0:
        remind_at = run_at - timedelta(minutes=reminder_minutes)
        if remind_at > datetime.now(ASIA_JAKARTA):
            jq.run_once(
                reminder_job, when=remind_at, name=f"rem-{new_name}",
                data={"user_id": uid, "job_name": new_name},
                chat_id=chat_id
            )
    return new_name

async def job_edit_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /job_edit_time <job|index> <exec_YYYY-MM-DD> <HH:MM>
    (Tidak mengubah tanggal booking / field / cookies)
    """
    uid = str(update.effective_user.id)
    if len(context.args) < 3:
        await update.message.reply_text("Format: /job_edit_time <job|index> <exec_YYYY-MM-DD> <HH:MM>")
        return
    selector, exec_iso_in, hhmm = context.args[0], context.args[1], context.args[2]
    try:
        exec_iso = parse_date_indo_to_iso(exec_iso_in)
        if not re.fullmatch(r"\d{2}:\d{2}", hhmm):
            raise ValueError("Jam harus HH:MM")
    except Exception as e:
        await update.message.reply_text(f"Input tidak valid: {e}")
        return

    job_name = resolve_job_selector(uid, selector)
    if not job_name:
        await update.message.reply_text("Job tidak ditemukan.")
        return
    jobs_store = get_jobs_store(uid)
    rec = jobs_store.get(job_name)
    if not rec:
        await update.message.reply_text("Job tidak ditemukan.")
        return

    try:
        new_name = await reschedule_job(
            context, uid, job_name, rec["booking_iso"], exec_iso, hhmm,
            rec["profile"], rec.get("cookies", {}), rec.get("reminder_minutes"),
            rec.get("chat_id", update.effective_chat.id)
        )
    except Exception as e:
        await update.message.reply_text(f"Gagal menjadwalkan ulang: {e}")
        return

    # update storage
    jobs_store.pop(job_name, None)
    rec["exec_iso"] = exec_iso
    rec["time"] = hhmm
    jobs_store[new_name] = rec
    save_storage(storage)
    await update.message.reply_text(f"Job diubah waktunya ✅\nLama: {job_name}\nBaru: {new_name}")

async def job_edit_fields(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /job_edit_fields <job|index> key=value;key=value;...
    Bisa juga set booking_date=YYYY-MM-DD (ubah tanggal booking).
    Juga dukung reminder_minutes=N (0..120) dan update cookies via:
      _ga=...;_ga_TMVP85FKW9=...;ci_session=...
    """
    uid = str(update.effective_user.id)
    if len(context.args) < 2:
        await update.message.reply_text("Format: /job_edit_fields <job|index> key=value;key=value;...")
        return
    selector = context.args[0]
    kv = parse_kv_pairs(" ".join(context.args[1:]))

    job_name = resolve_job_selector(uid, selector)
    if not job_name:
        await update.message.reply_text("Job tidak ditemukan.")
        return
    jobs_store = get_jobs_store(uid)
    rec = jobs_store.get(job_name)
    if not rec:
        await update.message.reply_text("Job tidak ditemukan.")
        return

    # cookies edit
    cookies = rec.get("cookies", {}).copy()
    if "_ga" in kv: cookies["_ga"] = kv["_ga"]
    if "_ga_TMVP85FKW9" in kv: cookies["_ga_TMVP85FKW9"] = kv["_ga_TMVP85FKW9"]
    if "ci_session" in kv: cookies["ci_session"] = kv["ci_session"]

    # reminder edit
    reminder_minutes = rec.get("reminder_minutes")
    if "reminder_minutes" in kv:
        v = kv["reminder_minutes"]
        if not v.isdigit() or not (0 <= int(v) <= 120):
            await update.message.reply_text("reminder_minutes harus 0..120")
            return
        reminder_minutes = int(v)

    # merge profile
    new_prof, errs, booking_date_override = validate_and_merge_profile(rec["profile"], kv)
    if errs:
        await update.message.reply_text("Ada error pada field:\n- " + "\n- ".join(errs))
        return

    new_booking_iso = rec["booking_iso"]
    if booking_date_override:
        try: new_booking_iso = parse_date_indo_to_iso(booking_date_override)
        except Exception as e:
            await update.message.reply_text(f"booking_date tidak valid: {e}")
            return

    try:
        new_name = await reschedule_job(
            context, uid, job_name, new_booking_iso, rec["exec_iso"], rec["time"],
            new_prof, cookies, reminder_minutes,
            rec.get("chat_id", update.effective_chat.id)
        )
    except Exception as e:
        await update.message.reply_text(f"Gagal menjadwalkan ulang: {e}")
        return

    jobs_store.pop(job_name, None)
    rec["booking_iso"] = new_booking_iso
    rec["profile"] = new_prof
    rec["cookies"] = cookies
    rec["reminder_minutes"] = reminder_minutes
    jobs_store[new_name] = rec
    save_storage(storage)
    await update.message.reply_text(f"Job diupdate ✅\nLama: {job_name}\nBaru: {new_name}")

async def job_edit_when(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /job_edit_when <job|index> <booking_YYYY-MM-DD> <exec_YYYY-MM-DD> <HH:MM>
    """
    uid = str(update.effective_user.id)
    if len(context.args) < 4:
        await update.message.reply_text("Format: /job_edit_when <job|index> <booking_YYYY-MM-DD> <exec_YYYY-MM-DD> <HH:MM>")
        return
    selector, new_booking_in, new_exec_in, hhmm = context.args[0], context.args[1], context.args[2], context.args[3]
    try:
        new_booking_iso = parse_date_indo_to_iso(new_booking_in)
        new_exec_iso = parse_date_indo_to_iso(new_exec_in)
        if not re.fullmatch(r"\d{2}:\d{2}", hhmm):
            raise ValueError("Jam harus HH:MM")
    except Exception as e:
        await update.message.reply_text(f"Input tidak valid: {e}")
        return

    job_name = resolve_job_selector(uid, selector)
    if not job_name:
        await update.message.reply_text("Job tidak ditemukan.")
        return
    jobs_store = get_jobs_store(uid)
    rec = jobs_store.get(job_name)
    if not rec:
        await update.message.reply_text("Job tidak ditemukan.")
        return

    try:
        new_name = await reschedule_job(
            context, uid, job_name, new_booking_iso, new_exec_iso, hhmm,
            rec["profile"], rec.get("cookies", {}), rec.get("reminder_minutes"),
            rec.get("chat_id", update.effective_chat.id)
        )
    except Exception as e:
        await update.message.reply_text(f"Gagal menjadwalkan ulang: {e}")
        return

    jobs_store.pop(job_name, None)
    rec["booking_iso"] = new_booking_iso
    rec["exec_iso"] = new_exec_iso
    rec["time"] = hhmm
    jobs_store[new_name] = rec
    save_storage(storage)
    await update.message.reply_text(f"Job diubah jadwal & tanggal booking ✅\nLama: {job_name}\nBaru: {new_name}")

async def job_update_cookies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /job_update_cookies <job|index> _ga=...;_ga_TMVP85FKW9=...;ci_session=...
    """
    uid = str(update.effective_user.id)
    if len(context.args) < 2:
        await update.message.reply_text("Format: /job_update_cookies <job|index> _ga=...;_ga_TMVP85FKW9=...;ci_session=...")
        return
    selector = context.args[0]
    kv = parse_kv_pairs(" ".join(context.args[1:]))

    job_name = resolve_job_selector(uid, selector)
    if not job_name:
        await update.message.reply_text("Job tidak ditemukan.")
        return
    jobs_store = get_jobs_store(uid)
    rec = jobs_store.get(job_name)
    if not rec:
        await update.message.reply_text("Job tidak ditemukan.")
        return

    cookies = rec.get("cookies", {}).copy()
    changed = []
    for k in ["_ga","_ga_TMVP85FKW9","ci_session"]:
        if k in kv:
            cookies[k] = kv[k]
            changed.append(k)
    if not changed:
        await update.message.reply_text("Tidak ada cookie yang diubah. Sertakan minimal salah satu: _ga / _ga_TMVP85FKW9 / ci_session.")
        return

    # simpan & jadwalkan ulang (tanpa mengubah waktu)
    try:
        new_name = await reschedule_job(
            context, uid, job_name, rec["booking_iso"], rec["exec_iso"], rec["time"],
            rec["profile"], cookies, rec.get("reminder_minutes"),
            rec.get("chat_id", update.effective_chat.id)
        )
    except Exception as e:
        await update.message.reply_text(f"Gagal menjadwalkan ulang: {e}")
        return

    jobs_store.pop(job_name, None)
    rec["cookies"] = cookies
    jobs_store[new_name] = rec
    save_storage(storage)
    await update.message.reply_text(f"Cookies job diupdate ✅ ({', '.join(changed)})\nLama: {job_name}\nBaru: {new_name}")

# =================== BOOT ===================
def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        token = "PASTE_TELEGRAM_BOT_TOKEN_DI_SINI"  # lebih aman via env var

    app = Application.builder().token(token).build()

    # basic
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("set_session", set_session))
    app.add_handler(CommandHandler("jobs", jobs_list))
    app.add_handler(CommandHandler("job_detail", job_detail))
    app.add_handler(CommandHandler("job_cancel", job_cancel))
    app.add_handler(CommandHandler("job_edit_time", job_edit_time))
    app.add_handler(CommandHandler("job_edit_fields", job_edit_fields))
    app.add_handler(CommandHandler("job_edit_when", job_edit_when))
    app.add_handler(CommandHandler("job_update_cookies", job_update_cookies))
    app.add_handler(CommandHandler("cancel", cancel))

    # conversations
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("book", book_entry)],
        states={
            BOOK_ASK_FORM: [MessageHandler(filters.TEXT & ~filters.COMMAND, book_collect_form)],
            BOOK_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, book_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="book_flow", persistent=False
    ))

    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("schedule", schedule_entry)],
        states={
            SCHED_ASK_FORM: [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_collect_form)],
            SCHED_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="schedule_flow", persistent=False
    ))

    app.add_error_handler(on_error)
    app.run_polling()

if __name__ == "__main__":
    main()
