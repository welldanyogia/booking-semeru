# pip install python-telegram-bot==21.4 requests beautifulsoup4 lxml pytz python-dateutil python-dotenv

import os, json, re, logging, time
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
import json
import logging
from bs4 import BeautifulSoup
from difflib import get_close_matches


# Setup logging
logging.basicConfig(
    level=logging.INFO,  # bisa diganti DEBUG kalau mau lebih detail
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Load .env
load_dotenv()

# ========================= CONFIG =========================
BASE = "https://bromotenggersemeru.id"
SITE_PATH_BROMO   = "/booking/site/lembah-watangan"
SITE_PATH_SEMERU  = "/booking/site/semeru"
CAP_URL    = f"{BASE}/website/home/get_view"
ACTION_URL = f"{BASE}/website/booking/action"
COMBO_URL  = f"{BASE}/website/home/combo"
ASIA_JAKARTA = pytz.timezone("Asia/Jakarta")

# --- Bromo ---
BROMO_SITE_ID   = "4"
BROMO_SECTOR_ID = "1"
BROMO_SITE_LABEL = "Bromo"

# --- Semeru ---
SEMERU_SITE_ID   = "8"      # id_site untuk kapasitas
SEMERU_SECTOR_ID = "3"      # sesuai dump HTML (penting!)
SEMERU_SITE_LABEL = "Semeru"

STORAGE_FILE = "storage.json"   # { "<user_id>": {"ci_session": "...", "jobs": {...}} }

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bromo-semeru-bot")

MONTHS_ID = {
    "januari":"01","februari":"02","maret":"03","april":"04","mei":"05","juni":"06",
    "juli":"07","agustus":"08","september":"09","oktober":"10","november":"11","desember":"12"
}
# =================== PROVINCE LOOKUP ===================
# Kode resmi sesuai <select> dari server
PROVINCE_CODES = {
    "ACEH": "11", "BALI": "51", "BANTEN": "36", "BENGKULU": "17",
    "DI YOGYAKARTA": "34", "DKI JAKARTA": "31", "GORONTALO": "75", "JAMBI": "15",
    "JAWA BARAT": "32", "JAWA TENGAH": "33", "JAWA TIMUR": "35",
    "KALIMANTAN BARAT": "61", "KALIMANTAN SELATAN": "63", "KALIMANTAN TENGAH": "62",
    "KALIMANTAN TIMUR": "64", "KALIMANTAN UTARA": "65", "KEPULAUAN BANGKA BELITUNG": "19",
    "KEPULAUAN RIAU": "21", "LAMPUNG": "18", "MALUKU": "81", "MALUKU UTARA": "82",
    "NUSA TENGGARA BARAT": "52", "NUSA TENGGARA TIMUR": "53",
    "PAPUA": "94", "PAPUA BARAT": "91", "RIAU": "14",
    "SULAWESI BARAT": "76", "SULAWESI SELATAN": "73", "SULAWESI TENGAH": "72",
    "SULAWESI TENGGARA": "74", "SULAWESI UTARA": "71",
    "SUMATERA BARAT": "13", "SUMATERA SELATAN": "16", "SUMATERA UTARA": "12",
}

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

_CANON_NAMES = { _norm(k): k for k in PROVINCE_CODES.keys() }

_PROV_SYNONYMS = {
    "diy": "DI YOGYAKARTA", "yogyakarta": "DI YOGYAKARTA", "jogja": "DI YOGYAKARTA",
    "dki": "DKI JAKARTA", "jakarta": "DKI JAKARTA",
    "jabar": "JAWA BARAT", "jateng": "JAWA TENGAH", "jatim": "JAWA TIMUR",
    "kalbar": "KALIMANTAN BARAT", "kalteng": "KALIMANTAN TENGAH",
    "kaltim": "KALIMANTAN TIMUR", "kaltara": "KALIMANTAN UTARA",
    "kalsel": "KALIMANTAN SELATAN", "kepri": "KEPULAUAN RIAU",
    "babel": "KEPULAUAN BANGKA BELITUNG", "malut": "MALUKU UTARA",
    "ntb": "NUSA TENGGARA BARAT", "ntt": "NUSA TENGGARA TIMUR",
    "sulbar": "SULAWESI BARAT", "sulsel": "SULAWESI SELATAN",
    "sulteng": "SULAWESI TENGAH", "sultra": "SULAWESI TENGGARA",
    "sulut": "SULAWESI UTARA",
    "sumbar": "SUMATERA BARAT", "sumsel": "SUMATERA SELATAN", "sumut": "SUMATERA UTARA",
    "di yogyakarta": "DI YOGYAKARTA", "diyogyakarta": "DI YOGYAKARTA",
    "dki jakarta": "DKI JAKARTA", "kep riau": "KEPULAUAN RIAU",
    "kepulauan riau": "KEPULAUAN RIAU", "kep bangka belitung": "KEPULAUAN BANGKA BELITUNG",
    "bangka belitung": "KEPULAUAN BANGKA BELITUNG", "papua barat": "PAPUA BARAT",
}

def province_lookup(q: str) -> tuple[str | None, str | None, list[str]]:
    """
    (code, canonical_name, suggestions)
    Input bisa kode '35' atau nama/singkatan 'Jatim'/'Jawa Timur'
    """
    if not q:
        return None, None, []
    q = q.strip()

    # Jika sudah kode valid
    if re.fullmatch(r"\d{2}", q) and q in PROVINCE_CODES.values():
        for name, code in PROVINCE_CODES.items():
            if code == q:
                return code, name, []
        return q, None, []

    n = _norm(q)

    # Sinonim
    if n in _PROV_SYNONYMS:
        canon = _PROV_SYNONYMS[n]
        return PROVINCE_CODES[canon], canon, []

    # Nama canonical
    if n in _CANON_NAMES:
        canon = _CANON_NAMES[n]
        return PROVINCE_CODES[canon], canon, []

    # Fuzzy
    candidates = list(_CANON_NAMES.keys()) + list(_PROV_SYNONYMS.keys())
    matches = get_close_matches(n, candidates, n=5, cutoff=0.75)
    suggestions = []
    for m in matches:
        if m in _PROV_SYNONYMS:
            suggestions.append(_PROV_SYNONYMS[m])
        elif m in _CANON_NAMES:
            suggestions.append(_CANON_NAMES[m])
    suggestions = list(dict.fromkeys(suggestions))
    return None, None, suggestions


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
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    if re.fullmatch(r"\d{2}-\d{2}-\d{4}", s):
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

def slugify(s: str, maxlen: int = 18) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    if len(s) > maxlen:
        s = s[:maxlen].rstrip("-")
    return s or "ketua"

def build_referer_url(site_path: str, iso_date: str) -> str:
    return f"{BASE}{site_path}?date_depart={iso_date}"


def get_tokens_from_cnt_page(html: str, debug_name: str = "debug.html"):
    logging.info("Parsing HTML untuk mencari .cnt-page ...")
    logging.info(f"{html}")

    soup = BeautifulSoup(html, "lxml")
    holder = soup.select_one(".cnt-page")  # gunakan .cnt-page untuk class

    if not holder:
        logging.error("Elemen .cnt-page tidak ditemukan, simpan HTML ke %s", debug_name)
        try:
            with open(debug_name, "w", encoding="utf-8") as f:
                f.write(html)
        except Exception as e:
            logging.exception("Gagal menyimpan debug file: %s", e)
        raise RuntimeError("Tidak menemukan .cnt-page di HTML.")

    logging.info("Berhasil menemukan elemen .cnt-page, parsing JSON ...")
    raw_text = holder.get_text("", strip=True)
    logging.debug("Raw text JSON: %s", raw_text[:200])  # tampilkan sebagian

    data = json.loads(raw_text)
    booking = data.get("booking", {})

    secret = booking.get("secret")
    form_hash = booking.get("form_hash", "")

    logging.info("Ekstraksi selesai: secret panjang=%d, form_hash='%s'",
                 len(secret) if secret else 0, form_hash)

    return secret, form_hash, booking
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
            return {"tanggal_cell": tanggal_text, "quota": quota, "status": status}
    return None

def check_capacity(iso_date: str, site: str) -> dict | None:
    """
    site: 'bromo' | 'semeru'
    """
    year_month = iso_date[:7]
    if site == "bromo":
        site_id = "4"
    elif site == "semeru":
        site_id = "8"
    else:
        raise ValueError("site harus 'bromo' atau 'semeru'")

    payload = {
        "action": "kapasitas",
        "id_site": site_id,
        "year_month": year_month
    }
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.post(CAP_URL, data=payload, headers=headers, timeout=30)

    soup = BeautifulSoup(resp.text, "lxml")
    rows = soup.select("table.table tbody tr")
    return find_quota_for_date(rows, iso_date)


def make_session_with_cookies(ci_session: str, extra_cookies: dict | None = None):
    sess = requests.Session()
    ua = ('Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
          'AppleWebKit/537.36 (KHTML, like Gecko) '
          'Chrome/139.0.0.0 Mobile Safari/537.36 Edg/139.0.0.0')
    sess.headers.update({"User-Agent": ua, "Accept": "*/*", "Accept-Language": "id,en;q=0.9,en-GB;q=0.8,en-US;q=0.7"})
    # per-job cookies
    if extra_cookies:
        if extra_cookies.get("_ga"):
            sess.cookies.set("_ga", extra_cookies["_ga"], domain=".bromotenggersemeru.id", path="/")
        if extra_cookies.get("_ga_TMVP85FKW9"):
            sess.cookies.set("_ga_TMVP85FKW9", extra_cookies["_ga_TMVP85FKW9"], domain=".bromotenggersemeru.id", path="/")
        if extra_cookies.get("ci_session"):
            sess.cookies.set("ci_session", extra_cookies["ci_session"], domain="bromotenggersemeru.id", path="/")
    # fallback global
    if ci_session and not sess.cookies.get("ci_session"):
        sess.cookies.set("ci_session", ci_session, domain="bromotenggersemeru.id", path="/")
    return sess

def fetch_districts_by_province(id_province: str) -> list[tuple[str, str]]:
    """
    Return list [(kode_kabkota, NAMA_KAB/KOTA), ...]
    """
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        resp = requests.post(COMBO_URL, data={"id_province": str(id_province)}, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.warning("Gagal fetch kab/kota untuk prov %s: %s", id_province, e)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    out: list[tuple[str, str]] = []
    for opt in soup.select("option"):
        val = (opt.get("value") or "").strip()
        name = opt.get_text(strip=True)
        if not val or val == "-":
            continue
        out.append((val, name))
    return out

def format_districts_message(prov_code: str, prov_name: str, pairs: list[tuple[str,str]]) -> str:
    header = f"📍 Daftar Kabupaten/Kota {prov_name.title()} ({prov_code})"
    lines = [header, ""]
    for code, name in pairs:
        lines.append(f"{code} — {name}")
    return "\n".join(lines)

def split_long_message(msg: str, limit: int = 3900) -> list[str]:
    if len(msg) <= limit:
        return [msg]
    parts, cur = [], ""
    for line in msg.splitlines():
        add = (("\n" if cur else "") + line)
        if len(cur) + len(add) > limit:
            parts.append(cur)
            cur = line
        else:
            cur += add
    if cur:
        parts.append(cur)
    return parts


# =================== BROMO FLOWS ===================
def add_or_update_members_bromo(sess: requests.Session, secret: str, male: int, female: int, id_country: str = "99"):
    if male < 0 or female < 0: return
    if male == 0 and female == 0: return
    payload = {"action": "anggota_update", "secret": secret, "id": "", "male": str(male), "female": str(female), "id_country": id_country}
    try: _ = sess.post(ACTION_URL, data=payload, timeout=30)
    except Exception as e: log.warning("anggota_update (Bromo) error: %s", e)

def do_booking_flow_bromo(ci_session: str, iso_date: str, profile: dict,
                          job_cookies: dict | None = None) -> tuple[bool, str, float, dict | None]:
    t0 = time.perf_counter()

    # ✅ JIT: cek kuota saat eksekusi
    cap = check_capacity(iso_date, "bromo")
    if not cap:
        return False, f"Kuota: tanggal {iso_date} tidak ditemukan.", time.perf_counter()-t0, None
    if cap["quota"] <= 0:
        return False, f"Kuota {cap['tanggal_cell']}: {cap['quota']} (Tidak tersedia).", time.perf_counter()-t0, None

    sess = make_session_with_cookies(ci_session, job_cookies)
    referer = build_referer_url(SITE_PATH_BROMO, iso_date)
    r = sess.get(referer, timeout=30)
    if r.status_code != 200:
        return False, f"Gagal GET booking page: {r.status_code}", time.perf_counter()-t0, None
    try:
        secret, form_hash, _ = get_tokens_from_cnt_page(r.text, debug_name="debug_bromo.html")
    except Exception as e:
        return False, f"Gagal ekstrak token: {e}", time.perf_counter()-t0, None

    sess.headers.update({"X-Requested-With": "XMLHttpRequest", "Origin": BASE, "Referer": referer})
    try:
        _ = sess.post(ACTION_URL, data={"action":"update_hash","secret":secret,"form_hash":form_hash}, timeout=30)
        _ = sess.post(ACTION_URL, data={"action":"validate_booking","secret":secret,"form_hash":form_hash}, timeout=30)
    except Exception as e:
        return False, f"Gagal update/validate hash: {e}", time.perf_counter()-t0, None

    male = int(profile.get("male", "0") or 0)
    female = int(profile.get("female", "0") or 0)
    add_or_update_members_bromo(sess, secret, male, female, profile.get("id_country", "99"))

    payload = {
        "action": "do_booking",
        "secret": secret,
        "id_sector": BROMO_SECTOR_ID,
        "form_hash": form_hash,
        "site": BROMO_SITE_LABEL,
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
            return True, f"Booking BERHASIL.\nLink: {link}\nServer message: {data.get('message','-')}", elapsed, data
        return False, f"Booking GAGAL: {data.get('message') or data}", elapsed, data
    return False, f"Respon non-JSON: {resp.text[:400]}", elapsed, None

# =================== SEMERU FLOWS (9 anggota) ===================
FORM_PROMPT_SEMERU = (
    "Silakan balas dalam satu pesan untuk SEMERU dengan format berikut.\n"
    "\n[DATA KETUA]\n"
    "Nama               : \n"
    "No KTP             : \n"
    "No HP              : \n"
    "Tanggal Lahir      :  (YYYY-MM-DD)\n"
    "Alamat             : \n"
    "ID Provinsi        : \n"
    "ID Kabupaten/Kota  : \n"
    "Pendamping (0/1)   : \n"
    "Organisasi         : \n"
    "Leader Setuju (0/1): \n"
    "Metode Bayar       :  (qris / VA-Mandiri / VA-BNI)\n"
    "\n[ANGGOTA 1]\n"
    "Anggota 1 Nama       : \n"
    "Anggota 1 Tgl Lahir  :  (YYYY-MM-DD)\n"
    "Anggota 1 Gender     :  (1=L,2=P)\n"
    "Anggota 1 Alamat     : \n"
    "Anggota 1 Identitas  :  (id_identity, default 1)\n"
    "Anggota 1 NIK        : \n"
    "Anggota 1 HP         : \n"
    "HP Keluarga 1        : \n"
    "Pekerjaan 1 (id_job) :  (default 6)\n"
    "\n[ANGGOTA 2..9] → gunakan pola yang sama (Anggota N ...)\n"
    "\n[OPSIONAL: COOKIES PER JOB & REMINDER]\n"
    "_ga               : \n"
    "_ga_TMVP85FKW9    : \n"
    "ci_session        : \n"
    "Ingatkan (menit)  :  (misal: 15 → bot remind sebelum eksekusi)\n"
)

def parse_form_block_semeru(text: str) -> tuple[dict, list, dict, int | None, list]:
    """
    Return: (leader_profile, members_list(<=9), cookies_dict, reminder_minutes, errors)
    """
    leader = {
        "id_country": "99", "id_gender": "1", "id_identity": "1",
        "name": "", "identity_no": "", "hp": "",
        "birthdate": "", "address": "", "id_province": "", "id_district": "",
        "pendamping": "0", "organisasi": "", "leader_setuju": "0", "bank": "qris",
    }
    cookies = {"_ga":"", "_ga_TMVP85FKW9":"", "ci_session":""}
    reminder_minutes = None
    errors = []
    members = []

    # normalize lines
    lines = [ln for ln in text.splitlines() if ":" in ln]
    kv = {}
    for ln in lines:
        k, v = ln.split(":", 1)
        kv[k.strip()] = v.strip()

    def get_ci(k):  # case-insensitive get
        for key in kv.keys():
            if key.lower() == k.lower():
                return kv[key]
        return ""

    leader["name"]         = get_ci("Nama")
    leader["identity_no"]  = get_ci("No KTP")
    leader["hp"]           = get_ci("No HP")
    leader["birthdate"]    = get_ci("Tanggal Lahir")
    leader["address"]      = get_ci("Alamat")
    leader["id_province"]  = get_ci("ID Provinsi")
    leader["id_district"]  = get_ci("ID Kabupaten/Kota")
    leader["pendamping"]   = get_ci("Pendamping (0/1)") or "0"
    leader["organisasi"]   = get_ci("Organisasi") or ""
    leader["leader_setuju"]= get_ci("Leader Setuju (0/1)") or "0"
    leader_bank_raw        = (get_ci("Metode Bayar") or "qris").strip().lower()
    valid_banks = {"qris": "qris", "va-mandiri": "VA-Mandiri", "va-bni": "VA-BNI"}
    leader["bank"]         = valid_banks.get(leader_bank_raw, "qris")

    # cookies + reminder
    cookies["_ga"]             = get_ci("_ga")
    cookies["_ga_TMVP85FKW9"]  = get_ci("_ga_TMVP85FKW9") or get_ci("_ga_tmpvp85fkw9") or get_ci("_ga_tmvp85fkw9")
    cookies["ci_session"]      = get_ci("ci_session")
    rm = get_ci("Ingatkan (menit)")
    if rm:
        if not rm.isdigit() or not (0 <= int(rm) <= 120):
            errors.append("Ingatkan (menit) harus 0..120")
        else:
            reminder_minutes = int(rm)

    # validate leader
    if not leader["name"]: errors.append("Nama (ketua) wajib.")
    if not leader["identity_no"]: errors.append("No KTP (ketua) wajib.")
    if not leader["hp"]: errors.append("No HP (ketua) wajib.")
    if leader["birthdate"] and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", leader["birthdate"]):
        errors.append("Tanggal Lahir (ketua) harus YYYY-MM-DD")
    if leader["pendamping"] not in {"0","1"}:
        errors.append("Pendamping harus 0/1.")
    if leader["leader_setuju"] not in {"0","1"}:
        errors.append("Leader Setuju harus 0/1.")

    # members (Anggota 1..9)
    for i in range(1, 10):
        base = f"Anggota {i} "
        nama = get_ci(base + "Nama")
        if not nama:
            continue
        m = {
            "nama": nama,
            "birthdate": get_ci(base + "Tgl Lahir"),
            "id_gender": (get_ci(base + "Gender") or "1"),
            "alamat": get_ci(base + "Alamat"),
            "id_identity": (get_ci(base + "Identitas") or "1"),
            "identity_no": get_ci(base + "NIK"),
            "hp_member": get_ci(base + "HP"),
            "hp_keluarga": get_ci(f"HP Keluarga {i}") or get_ci("HP Keluarga"),
            "id_job": (get_ci(f"Pekerjaan {i} (id_job)") or get_ci("Pekerjaan (id_job)") or "6"),
            "id_country": "99",
            "anggota_setuju": "0"
        }
        if m["birthdate"] and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", m["birthdate"]):
            errors.append(f"Anggota {i}: Tgl Lahir harus YYYY-MM-DD.")
        if m["id_gender"] not in {"1","2"}:
            errors.append(f"Anggota {i}: Gender harus 1/2.")
        members.append(m)

    if len(members) > 9:
        errors.append("Maksimal 9 anggota.")

    return leader, members, cookies, reminder_minutes, errors

import time
from datetime import datetime, timedelta

def do_booking_flow_semeru(
    ci_session: str,
    booking_iso: str,
    leader: dict,
    members: list,
    job_cookies: dict | None = None
) -> tuple[bool, str, float, dict | None]:
    """
    Flow Semeru:
      (1) GET halaman Semeru (ambil secret, form_hash)
      (2) update_hash & validate_booking
      (3) do_booking (data ketua/leader)
      (4) loop member_update untuk tiap anggota (maks 9)
    """
    t0 = time.perf_counter()
    log.warning("Tanggal berangkat (ISO): %s", booking_iso)

    # ✅ JIT: cek kuota saat eksekusi
    cap = check_capacity(booking_iso, "semeru")
    if not cap:
        return False, f"Kuota: tanggal {booking_iso} tidak ditemukan.", time.perf_counter() - t0, None
    if cap["quota"] <= 0:
        return False, f"Kuota {cap['tanggal_cell']}: {cap['quota']} (Tidak tersedia).", time.perf_counter() - t0, None

    # Siapkan session + cookies
    sess = make_session_with_cookies(ci_session, job_cookies)

    # Siapkan URL referer
    referer = build_referer_url(SITE_PATH_SEMERU, booking_iso)
    log.warning("Referer GET: %s", referer)

    # custom headers (mobile Edge/Chromium)
    custom_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "id,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
        "Connection": "keep-alive",
        "Referer": f"https://bromotenggersemeru.id/peraturan/semeru?date_depart={booking_iso}",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": ("Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36 Edg/139.0.0.0"),
        "sec-ch-ua": '"Not;A=Brand";v="99", "Microsoft Edge";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
        "Origin": BASE,
    }
    sess.headers.update(custom_headers)

    # (1) GET halaman
    try:
        r = sess.get(referer, timeout=30)
    except Exception as e:
        return False, f"Gagal GET page: {e}", time.perf_counter() - t0, None
    if r.status_code != 200:
        return False, f"Gagal GET page: HTTP {r.status_code}", time.perf_counter() - t0, None

    # ekstrak token
    try:
        secret, form_hash, booking_obj = get_tokens_from_cnt_page(r.text, debug_name="debug_semeru.html")
        if not secret:
            return False, "Token 'secret' kosong dari cnt-page.", time.perf_counter() - t0, None
        log.info("Token OK: secret_len=%d, form_hash_len=%d", len(secret or ""), len(form_hash or ""))
    except Exception as e:
        return False, f"Gagal ekstrak token: {e}", time.perf_counter() - t0, None

    # header AJAX
    sess.headers.update({"X-Requested-With": "XMLHttpRequest", "Referer": referer, "Origin": BASE})

    # (2) update_hash & validate_booking
    try:
        _ = sess.post(ACTION_URL, data={"action": "update_hash", "secret": secret, "form_hash": form_hash or ""}, timeout=30)
        _ = sess.post(ACTION_URL, data={"action": "validate_booking", "secret": secret, "form_hash": form_hash or ""}, timeout=30)
    except Exception as e:
        return False, f"Gagal update/validate hash: {e}", time.perf_counter() - t0, None

    # (3) do_booking (arrival = H+1)
    try:
        arr_iso = (datetime.fromisoformat(booking_iso) + timedelta(days=1)).date().isoformat()
    except Exception:
        arr_iso = booking_iso

    payload = {
        "action": "do_booking",
        "secret": secret,
        "id_sector": SEMERU_SECTOR_ID,
        "date_depart": booking_iso,
        "date_arrival": arr_iso,
        "pendamping": leader.get("pendamping", "0"),
        "organisasi": leader.get("organisasi", ""),
        "name": leader.get("name", ""),
        "id_country": leader.get("id_country", "99"),
        "birthdate": leader.get("birthdate", ""),
        "leader_setuju": leader.get("leader_setuju", "0"),
        "id_gender": leader.get("id_gender", "1"),
        "id_identity": leader.get("id_identity", "1"),
        "identity_no": leader.get("identity_no", ""),
        "address": leader.get("address", ""),
        "id_province": leader.get("id_province", ""),
        "id_district": leader.get("id_district", ""),
        "hp": leader.get("hp", ""),
        "table-member_length": "10",
        "bank": leader.get("bank", "qris"),
        "termsCheckbox": "on",
        "form_hash": form_hash or "",
    }

    # (4) tambah anggota (maks 9)
    added = 0
    for idx, m in enumerate(members, start=1):
        if idx > 9:
            log.warning("Melebihi 9 anggota, sisanya di-skip.")
            break
        if not (m.get("nama") or "").strip():
            continue
        m_payload = {
            "action": "member_update",
            "id": "",
            "nama": m.get("nama", ""),
            "birthdate": m.get("birthdate", ""),
            "anggota_setuju": m.get("anggota_setuju", "0"),
            "id_gender": m.get("id_gender", "1"),
            "alamat": m.get("alamat", ""),
            "id_identity": m.get("id_identity", "1"),
            "identity_no": m.get("identity_no", ""),
            "hp_member": m.get("hp_member", ""),
            "hp_keluarga": m.get("hp_keluarga", ""),
            "id_job": m.get("id_job", "6"),
            "id_country": m.get("id_country", "99"),
            "secret": secret,
            "form_hash": form_hash or "",
        }
        try:
            resp_m = sess.post(ACTION_URL, data=m_payload, timeout=30)
            _ct = (resp_m.headers.get("Content-Type") or "").lower()
            if "json" in _ct:
                _dj = resp_m.json()
                if not _dj.get("status", True):
                    log.warning("[member %s] server warn: %s", idx, _dj)
            added += 1
        except Exception as e:
            log.warning("member_update gagal (anggota %s): %s", idx, e)

    # do_booking
    try:
        resp = sess.post(ACTION_URL, data=payload, timeout=60)
    except Exception as e:
        return False, f"Gagal do_booking: {e}", time.perf_counter() - t0, None

    ct = (resp.headers.get("Content-Type") or "").lower()
    if "json" not in ct:
        return False, f"Respon non-JSON do_booking: {resp.text[:400]}", time.perf_counter() - t0, None

    try:
        data = resp.json()
    except Exception:
        return False, f"Respon do_booking tak bisa JSON: {resp.text[:400]}", time.perf_counter() - t0, None

    if not data.get("status"):
        return False, f"Booking Semeru GAGAL {secret[:12]}...: {data.get('message') or data}", time.perf_counter() - t0, data

    elapsed = time.perf_counter() - t0
    link = data.get("booking_link") or data.get("link_redirect") or "-"
    msg = ("Booking Semeru BERHASIL.\n"
           f"Anggota ditambahkan: {added}\n"
           f"Link: {link}\n"
           f"Server: {data.get('message', '-')}")
    return True, msg, elapsed, data

# =================== COMMON FORM PARSER (Bromo) ===================
FORM_KEYS_BROMO = {
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
    "_ga": "_ga",
    "_ga_tmpvp85fkw9": "_ga_TMVP85FKW9",
    "_ga_tmvp85fkw9": "_ga_TMVP85FKW9",
    "ci_session": "ci_session",
    "ingatkan (menit)": "reminder_minutes",
}
FORM_PROMPT_BROMO = (
    "Silakan balas dalam satu pesan (BROMO) dgn format:\n\n"
    "Nama               : \n"
    "No KTP             : \n"
    "No HP              : \n"
    "Pintu Masuk (1-4)  : \n"
    "Jenis Kendaraan    : \n"
    "Jumlah Kendaraan   : \n"
    "Metode Bayar       : \n"
    "Jumlah Laki-laki   : \n"
    "Jumlah Perempuan   : \n"
    "Tanggal Lahir      : \n"
    "Alamat             : \n"
    "ID Provinsi        : \n"
    "ID Kabupaten/Kota  : \n"
    "\n# OPSIONAL (per Job):\n"
    "_ga               : \n"
    "_ga_TMVP85FKW9    : \n"
    "ci_session        : \n"
    "Ingatkan (menit)  :  (contoh: 15)\n"
    "\nKeterangan:\n"
    "- Pintu Masuk: 1=Pasuruan, 2=Malang, 3=Lumajang, 4=Probolinggo\n"
    "- Jenis Kendaraan: 1=R4, 2=R2, 3=Sepeda, 4=Kuda, 6=Jalan Kaki\n"
    "- Metode Bayar: qris / VA-Mandiri / VA-BNI\n"
    "- Field opsional boleh kosong."
)

def parse_form_block_bromo(text: str) -> tuple[dict, dict, int | None, list]:
    profile = {
        "id_country": "99", "id_gender": "1", "id_identity": "1",
        "id_gate": "2", "id_vehicle": "2", "vehicle_count": "1",
        "bank": "qris", "male": "0", "female": "0",
        "birthdate": "", "address": "", "id_province": "", "id_district": ""
    }
    cookies = {"_ga":"", "_ga_TMVP85FKW9":"", "ci_session":""}
    reminder_minutes = None
    errors = []

    for raw in text.splitlines():
        if ":" not in raw: continue
        label, value = raw.split(":", 1)
        key = label.strip().lower()
        val = value.strip()
        if key in FORM_KEYS_BROMO:
            mapped = FORM_KEYS_BROMO[key]
            if mapped in {"_ga","_ga_TMVP85FKW9","ci_session"}:
                cookies[mapped] = val
            elif mapped == "reminder_minutes":
                if val:
                    if not val.isdigit() or not (0 <= int(val) <= 120):
                        errors.append("Ingatkan (menit) harus 0..120")
                    else:
                        reminder_minutes = int(val)
            else:
                profile[mapped] = val

    if not profile["name"]: errors.append("Nama wajib.")
    if not profile["identity_no"]: errors.append("No KTP wajib.")
    if not profile["hp"]: errors.append("No HP wajib.")
    if profile["id_gate"] and profile["id_gate"] not in {"1","2","3","4"}:
        errors.append("Pintu Masuk harus 1/2/3/4.")
    if profile["id_vehicle"] and profile["id_vehicle"] not in {"1","2","3","4","6"}:
        errors.append("Jenis Kendaraan harus 1/2/3/4/6.")
    if profile["vehicle_count"] and (not profile["vehicle_count"].isdigit() or not (1 <= int(profile["vehicle_count"]) <= 20)):
        errors.append("Jumlah Kendaraan harus 1-20.")
    # normalisasi bank untuk bromo juga
    valid_banks = {"qris":"qris","va-mandiri":"VA-Mandiri","va-bni":"VA-BNI"}
    profile["bank"] = valid_banks.get((profile.get("bank") or "qris").strip().lower(), "qris")
    for fld in ["male","female"]:
        if profile[fld] and (not profile[fld].isdigit() or not (0 <= int(profile[fld]) <= 19)):
            errors.append(f"{'Laki-laki' if fld=='male' else 'Perempuan'} harus 0–19.")
    if profile["birthdate"] and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", profile["birthdate"]):
        errors.append("Tanggal Lahir harus YYYY-MM-DD.")
    return profile, cookies, reminder_minutes, errors

# =================== TELEGRAM ===================
HELP_TEXT = (
    "Perintah:\n"
    "• /start — cek bot\n"
    "• /help — lihat bantuan ini\n"
    "• /set_session <ci_session>\n"
    "• /example — Contoh Format Booking\n"
    "\n— Lookup Wilayah —\n"
    "• /prov <nama/kode>  → kode provinsi (contoh: /prov Jatim  |  /prov 35)\n"
    "• /kab <nama/kode>   → daftar kab/kota utk provinsi (contoh: /kab \"Jawa Timur\"  |  /kab 35)\n"
    "\n— BROMO —\n"
    "• /book <tgl_booking>\n"
    "  - Contoh tanggal: 2025-09-30  |  30-09-2025  |  30 September 2025\n"
    "• /schedule <tgl_booking> <tgl_eksekusi> <HH:MM[:SS]>\n"
    "  - Contoh: /schedule 2025-09-30 2025-09-29 23:59\n"
    "\n— SEMERU —\n"
    "• /book_semeru <tgl_booking>\n"
    "• /schedule_semeru <tgl_booking> <tgl_eksekusi> <HH:MM[:SS]>\n"
    "\n— Manajemen Job —\n"
    "• /jobs\n"
    "• /job_detail <job|index>\n"
    "• /job_cancel <job|index>\n"
    "• /job_edit_time <job|index> <exec_YYYY-MM-DD> <HH:MM[:SS]>\n"
    "• /job_edit_fields <job|index> key=value;...\n"
    "• /job_edit_when <job|index> <booking_YYYY-MM-DD> <exec_YYYY-MM-DD> <HH:MM[:SS]>\n"
    "• /job_update_cookies <job|index> _ga=...;_ga_TMVP85FKW9=...;ci_session=...\n"
    "\nTips:\n"
    "- ID Provinsi boleh diisi kode atau nama (mis. 35 atau Jawa Timur). Gunakan /kab untuk melihat kode kab/kota.\n"
    "- Format tanggal fleksibel seperti contoh di atas.\n"
    "- Lihat contoh lengkap isi form: /examples\n"
)

FORMAT_BROMO_EXAMPLE = (
    "[Contoh Isi Form BROMO]\n"
    "Nama               : Welldan Yogia\n"
    "No KTP             : 3517xxxxxxxxxxxx\n"
    "No HP              : 08xxxxxxxxxx\n"
    "Pintu Masuk (1-4)  : 2\n"
    "Jenis Kendaraan    : 2\n"
    "Jumlah Kendaraan   : 1\n"
    "Metode Bayar       : qris\n"
    "Jumlah Laki-laki   : 1\n"
    "Jumlah Perempuan   : 0\n"
    "Tanggal Lahir      : 2001-08-01\n"
    "Alamat             : Kunden Kedungso\n"
    "ID Provinsi        : Jawa Timur   (boleh 35)\n"
    "ID Kabupaten/Kota  : 3578         (gunakan /kab 35 untuk lihat kode)\n"
    "\n# OPSIONAL per job:\n"
    "_ga               : <isi jika ada>\n"
    "_ga_TMVP85FKW9    : <isi jika ada>\n"
    "ci_session        : <override jika perlu>\n"
    "Ingatkan (menit)  : 15\n"
)

FORMAT_SEMERU_EXAMPLE = (
    "[Contoh Isi Form SEMERU]\n"
    "[DATA KETUA]\n"
    "Nama               : Welldan Yogia\n"
    "No KTP             : 3517xxxxxxxxxxxx\n"
    "No HP              : 08xxxxxxxxxx\n"
    "Tanggal Lahir      : 2001-08-01\n"
    "Alamat             : Kunden Kedungso\n"
    "ID Provinsi        : 35            (boleh tulis Jawa Timur)\n"
    "ID Kabupaten/Kota  : 3578          (lihat /kab 35)\n"
    "Pendamping (0/1)   : 0\n"
    "Organisasi         : -\n"
    "Leader Setuju (0/1): 1\n"
    "Metode Bayar       : qris\n"
    "\n[ANGGOTA 1]\n"
    "Anggota 1 Nama       : Andi Setiawan\n"
    "Anggota 1 Tgl Lahir  : 2002-01-15\n"
    "Anggota 1 Gender     : 1\n"
    "Anggota 1 Alamat     : Surabaya\n"
    "Anggota 1 Identitas  : 1\n"
    "Anggota 1 NIK        : 3526xxxxxxxxxxxx\n"
    "Anggota 1 HP         : 08xxxxxxxxxx\n"
    "HP Keluarga 1        : 08xxxxxxxxxx\n"
    "Pekerjaan 1 (id_job) : 6\n"
    "\n# Tambahkan ANGGO TA 2..9 dengan pola yang sama jika perlu\n"
    "\n# OPSIONAL per job:\n"
    "_ga               : <isi jika ada>\n"
    "_ga_TMVP85FKW9    : <isi jika ada>\n"
    "ci_session        : <override jika perlu>\n"
    "Ingatkan (menit)  : 15\n"
)

async def examples_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(FORMAT_BROMO_EXAMPLE)
    await update.message.reply_text(FORMAT_SEMERU_EXAMPLE)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Halo! Bot siap.\n\n" + HELP_TEXT)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)

async def set_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not context.args:
        await update.message.reply_text("Format: /set_session <ci_session>")
        return
    set_ci(uid, context.args[0].strip())
    await update.message.reply_text("ci_session disimpan ✅")

# ====== Conversations ======
BOOK_ASK_FORM, BOOK_CONFIRM = range(2)
SCHED_ASK_FORM, SCHED_CONFIRM = range(2)
BOOK_ASK_FORM_SEM, BOOK_CONFIRM_SEM = range(2)
SCHED_ASK_FORM_SEM, SCHED_CONFIRM_SEM = range(2)

# ---------- utilities ----------
def require_job_queue(context: ContextTypes.DEFAULT_TYPE):
    jq = getattr(context.application, "job_queue", None)
    if jq is None:
        raise RuntimeError("JobQueue tidak aktif. Install: pip install 'python-telegram-bot[job-queue]'")
    return jq

def make_job_name(prefix: str, uid: str, leader_name: str, booking_iso: str, exec_iso: str, hhmm: str) -> str:
    slug = slugify(leader_name or "ketua")
    return f"{prefix}-{uid}-{slug}-{booking_iso}-{exec_iso}-{hhmm.replace(':','')}"

def parse_hhmmss(s: str) -> tuple[int,int,int]:
    if not re.fullmatch(r"\d{2}:\d{2}(:\d{2})?", s):
        raise ValueError("Jam harus HH:MM atau HH:MM:SS")
    parts = s.split(":")
    hh, mm = int(parts[0]), int(parts[1])
    ss = int(parts[2]) if len(parts) == 3 else 0
    if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59):
        raise ValueError("Jam di luar rentang 00:00[:00]..23:59[:59]")
    return hh, mm, ss

# ---------- BROMO ----------
async def book_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not get_ci(uid):
        await update.message.reply_text("Set dulu /set_session <ci_session> (global).")
        return ConversationHandler.END
    if not context.args:
        await update.message.reply_text("Format: /book <tgl_booking>")
        return ConversationHandler.END
    try: iso = parse_date_indo_to_iso(" ".join(context.args))
    except Exception as e:
        await update.message.reply_text(f"Format tanggal salah: {e}")
        return ConversationHandler.END
    context.user_data.clear()
    context.user_data["booking_iso"] = iso
    await update.message.reply_text(FORM_PROMPT_BROMO)  # no Markdown
    return BOOK_ASK_FORM

async def book_collect_form(update: Update, context: ContextTypes.DEFAULT_TYPE):
    profile, cookies, reminder_minutes, errors = parse_form_block_bromo(update.message.text)
    if errors:
        await update.message.reply_text("Ada masalah:\n- " + "\n- ".join(errors))
        return BOOK_ASK_FORM
    context.user_data["profile"] = profile
    context.user_data["cookies"] = cookies
    context.user_data["reminder_minutes"] = reminder_minutes

    iso = context.user_data["booking_iso"]
    total = 1 + int(profile["male"]) + int(profile["female"])
    cookie_hint = ", ".join([f"{k}={'(ada)' if cookies.get(k) else '(kosong)'}" for k in ["_ga","_ga_TMVP85FKW9","ci_session"]])
    remind_txt = f"{reminder_minutes} menit" if reminder_minutes is not None else "tidak"
    summary = (
        f"[BROMO]\nTanggal Booking: {iso}\n"
        f"Nama: {profile['name']} | KTP: {profile['identity_no']} | HP: {profile['hp']}\n"
        f"Gate: {profile['id_gate']} | Kendaraan: {profile['id_vehicle']} x {profile['vehicle_count']}\n"
        f"Bayar: {profile['bank']} | L:{profile['male']} P:{profile['female']} | Total:{total}\n"
        f"Cookies: {cookie_hint} | Reminder: {remind_txt}\n\n"
        "Catatan: Kuota akan dicek saat eksekusi.\n"
        "Ketik 'YA' untuk konfirmasi booking sekarang."
    )
    await update.message.reply_text(summary)
    return BOOK_CONFIRM

async def book_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.strip().lower() not in {"ya","y","yes"}:
        await update.message.reply_text("Dibatalkan.")
        return ConversationHandler.END
    uid = str(update.effective_user.id)
    ok, msg, elapsed_s, raw = do_booking_flow_bromo(
        get_ci(uid), context.user_data["booking_iso"], context.user_data["profile"], context.user_data.get("cookies")
    )
    extra = ""
    if raw:
        extra = f"\n\n[Server]\nmessage: {raw.get('message','-')}\nlink: {raw.get('booking_link') or raw.get('link_redirect') or '-'}"
    await update.message.reply_text(("✅ " if ok else "❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra)
    return ConversationHandler.END

async def schedule_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not get_ci(uid):
        await update.message.reply_text("Set dulu /set_session <ci_session> (global).")
        return ConversationHandler.END
    if len(context.args) < 3:
        await update.message.reply_text("Format: /schedule <tgl_booking> <tgl_eksekusi> <HH:MM[:SS]>")
        return ConversationHandler.END
    try:
        booking_iso = parse_date_indo_to_iso(context.args[0])
        exec_iso    = parse_date_indo_to_iso(context.args[1])
    except Exception as e:
        await update.message.reply_text(f"Format tanggal salah: {e}")
        return ConversationHandler.END

    hhmm = context.args[2]
    try:
        parse_hhmmss(hhmm)
    except Exception as e:
        await update.message.reply_text(str(e)); return ConversationHandler.END

    context.user_data.clear()
    context.user_data["booking_iso"] = booking_iso
    context.user_data["exec_iso"]    = exec_iso
    context.user_data["time"]        = hhmm
    await update.message.reply_text(FORM_PROMPT_BROMO)
    return SCHED_ASK_FORM

async def schedule_collect_form(update: Update, context: ContextTypes.DEFAULT_TYPE):
    profile, cookies, reminder_minutes, errors = parse_form_block_bromo(update.message.text)
    if errors:
        await update.message.reply_text("Ada masalah:\n- " + "\n- ".join(errors))
        return SCHED_ASK_FORM
    context.user_data["profile"] = profile
    context.user_data["cookies"] = cookies
    context.user_data["reminder_minutes"] = reminder_minutes

    cookie_hint = ", ".join([f"{k}={'(ada)' if cookies.get(k) else '(kosong)'}" for k in ["_ga","_ga_TMVP85FKW9","ci_session"]])
    remind_txt = f"{reminder_minutes} menit" if reminder_minutes is not None else "tidak"
    total = 1 + int(profile["male"]) + int(profile["female"])
    summary = (
        f"[Jadwal BROMO]\n"
        f"- Booking: {context.user_data['booking_iso']}\n"
        f"- Eksekusi: {context.user_data['exec_iso']} {context.user_data['time']} Asia/Jakarta\n"
        f"Leader: {profile['name']} | KTP:{profile['identity_no']} | HP:{profile['hp']}\n"
        f"Total peserta (estimasi): {total}\n"
        f"Cookies: {cookie_hint} | Reminder: {remind_txt}\n\n"
        "Catatan: Kuota akan dicek pada waktu eksekusi.\n"
        "Ketik 'YA' untuk membuat jadwal."
    )
    await update.message.reply_text(summary)
    return SCHED_CONFIRM

# ---------- SCHEDULER SHARED ----------
async def poll_capacity_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    uid = str(data["user_id"])
    site = data["site"]
    iso = data["iso"]
    prof = data["profile"]
    job_cookies = data.get("cookies") or {}
    chat_id = context.job.chat_id

    # Konfigurasi polling & notifikasi
    interval_seconds = data.get("interval_seconds", 20)     # 🕒 polling tiap 20 detik
    notify_every_ticks = data.get("notify_every_ticks", max(1, int(300 / interval_seconds)))  # 📨 tiap 5 menit (300 dtk)
    max_minutes = data.get("max_minutes", 180)              # maksimal durasi polling
    max_ticks = int((max_minutes * 60) / interval_seconds)

    # counter tick
    data["ticks"] = data.get("ticks", 0) + 1

    ci = get_ci(uid)  # fallback global
    cap = check_capacity(iso, site)

    # Belum muncul / belum ada kuota
    if (not cap) or (cap["quota"] <= 0):
        # Hanya kirim status tiap 5 menit (default)
        if data["ticks"] % notify_every_ticks == 1:
            status = (f"{iso}: tanggal tidak ditemukan"
                      if not cap else f"{cap['tanggal_cell']}\nKuota: {cap['quota']} → {cap['status']}")
            await context.bot.send_message(
                chat_id,
                text=f"[Polling {site}] {status} (percobaan {data['ticks']}, interval {interval_seconds}s)"
            )

        # Stop kalau sudah melewati durasi maksimum
        if data["ticks"] >= max_ticks:
            await context.bot.send_message(
                chat_id,
                text=f"[Polling {site}] Dihentikan setelah ~{max_minutes} menit / {data['ticks']} percobaan. "
                     f"Gunakan /job_edit_time untuk menjadwalkan ulang."
            )
            context.job.schedule_removal()
        return

    # Kuota muncul → eksekusi booking, lalu hentikan polling
    await context.bot.send_message(chat_id, text=f"[Polling {site}] Kuota tersedia: {cap['quota']} — eksekusi booking sekarang.")
    if site == "bromo":
        ok, msg, elapsed_s, raw = do_booking_flow_bromo(ci, iso, prof, job_cookies=job_cookies)
    else:
        leader = prof.get("_leader", {})
        members = prof.get("_members", [])
        ok, msg, elapsed_s, raw = do_booking_flow_semeru(ci, iso, leader, members, job_cookies=job_cookies)

    extra = ""
    if raw:
        server_msg = raw.get("message", "-")
        link = raw.get("booking_link") or raw.get("link_redirect") or "-"
        extra = f"\n[Server]\nmessage: {server_msg}\nlink: {link}"

    await context.bot.send_message(
        chat_id,
        text=("[Polling] ✅ " if ok else "[Polling] ❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra
    )
    context.job.schedule_removal()

async def scheduled_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    uid = str(data["user_id"])
    site = data["site"]            # 'bromo' | 'semeru'
    iso  = data["iso"]
    prof = data["profile"]
    job_cookies = data.get("cookies") or {}
    chat_id = context.job.chat_id

    ci = get_ci(uid)  # global fallback
    if not ci and not job_cookies.get("ci_session"):
        await context.bot.send_message(chat_id, text=f"[Jadwal {site}] ci_session kosong/expired. /set_session atau /job_update_cookies dulu.")
        return

    jq = require_jq(context)
    job_name = context.job.name or f"{site}-{uid}-{iso}"

    # ✅ cek kapasitas saat eksekusi
    cap = check_capacity(iso, site)
    if not cap or cap["quota"] <= 0:
        # info kondisi saat ini
        if not cap:
            await context.bot.send_message(chat_id, text=f"[Jadwal {site}] {iso}: tanggal tidak ditemukan.")
        else:
            await context.bot.send_message(chat_id, text=f"[Jadwal {site}] {cap['tanggal_cell']}\nKuota: {cap['quota']} → {cap['status']}")

        # aktifkan polling per menit
        poll_name = f"poll-{job_name}"
        for j in jq.get_jobs_by_name(poll_name):
            j.schedule_removal()   # pastikan tidak dobel
        jq.run_repeating(
            poll_capacity_job,
            interval=60,
            first=60,              # mulai 1 menit dari sekarang
            name=poll_name,
            data={
                "user_id": uid,
                "site": site,
                "iso": iso,
                "profile": prof,
                "cookies": job_cookies,
                # opsional kontrol notifikasi:
                "notify_every": 5,   # kirim pesan tiap 5 menit; set ke 1 kalau mau tiap menit
                "max_ticks": 180     # berhenti setelah 3 jam
            },
            chat_id=chat_id
        )
        await context.bot.send_message(chat_id, text=f"[Jadwal {site}] Polling per menit diaktifkan (max 3 jam).")
        return

    # kalau kuota tersedia langsung eksekusi seperti biasa
    await context.bot.send_message(chat_id, text=f"[Jadwal {site}] {cap['tanggal_cell']}\nKuota: {cap['quota']} → {cap['status']}")
    if site == "bromo":
        ok, msg, elapsed_s, raw = do_booking_flow_bromo(ci, iso, prof, job_cookies=job_cookies)
    else:
        leader  = prof.get("_leader", {})
        members = prof.get("_members", [])
        ok, msg, elapsed_s, raw = do_booking_flow_semeru(ci, iso, leader, members, job_cookies=job_cookies)

    extra = ""
    if raw:
        server_msg = raw.get("message","-")
        link = raw.get("booking_link") or raw.get("link_redirect") or "-"
        extra = f"\n[Server]\nmessage: {server_msg}\nlink: {link}"
    await context.bot.send_message(chat_id, text=("[Jadwal] ✅ " if ok else "[Jadwal] ❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra)

async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    uid = str(data["user_id"]); job_name = data["job_name"]
    rec = get_jobs_store(uid).get(job_name)
    if not rec: return
    chat_id = rec.get("chat_id")

    def mask(s):
        if not s: return "(kosong)"
        return s[:6] + "..." + s[-4:]
    ck = rec.get("cookies", {})
    msg = (
        f"⏰ Reminder job:\n{job_name}\n\n"
        f"Eksekusi: {rec['exec_iso']} {rec['time']} (Asia/Jakarta)\n"
        f"Booking:  {rec['booking_iso']}\n\n"
        "Update cookies jika berpotensi expired:\n"
        f"- _ga: {mask(ck.get('_ga'))}\n"
        f"- _ga_TMVP85FKW9: {mask(ck.get('_ga_TMVP85FKW9'))}\n"
        f"- ci_session: {mask(ck.get('ci_session'))}\n\n"
        f"Perintah: /job_update_cookies {job_name} _ga=<baru>;_ga_TMVP85FKW9=<baru>;ci_session=<baru>"
    )
    await context.bot.send_message(chat_id, text=msg)

def jobs_live_names(context: ContextTypes.DEFAULT_TYPE) -> set[str]:
    jq = getattr(context.application, "job_queue", None)
    live = set()
    if jq:
        for j in jq.jobs():
            if j.name: live.add(j.name)
    return live

def require_jq(context: ContextTypes.DEFAULT_TYPE):
    jq = getattr(context.application, "job_queue", None)
    if jq is None:
        raise RuntimeError("JobQueue tidak aktif. Install: pip install 'python-telegram-bot[job-queue]'")
    return jq

def resolve_job_selector(uid: str, selector: str) -> str | None:
    jobs_store = get_jobs_store(uid)
    if not jobs_store: return None
    names = sorted(jobs_store.keys())
    if selector.isdigit():
        idx = int(selector)
        return names[idx-1] if 1 <= idx <= len(names) else None
    return selector if selector in jobs_store else None

async def jobs_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    jobs_store = get_jobs_store(uid)
    if not jobs_store:
        await update.message.reply_text("Belum ada job terjadwal.")
        return
    live = jobs_live_names(context)
    names = sorted(jobs_store.keys())
    lines = []
    for i, name in enumerate(names, start=1):
        rec = jobs_store[name]
        leader = rec["profile"].get("name") or rec["profile"].get("_leader",{}).get("name","-")
        status = "AKTIF" if name in live else "TIDAK AKTIF"
        lines.append(f"{i}. {name} [{status}] Booking:{rec['booking_iso']} Eksekusi:{rec['exec_iso']} {rec['time']} Ketua:{leader}")
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
    live = job_name in jobs_live_names(context)

    def mask(s):
        if not s: return "(kosong)"
        return s[:6] + "..." + s[-4:]
    ck = rec.get("cookies", {})
    safe_ck = {k: mask(ck.get(k)) for k in ["_ga","_ga_TMVP85FKW9","ci_session"]}

    await update.message.reply_text(json.dumps({
        "job": job_name,
        "status": "AKTIF" if live else "TIDAK AKTIF",
        "booking_iso": rec["booking_iso"],
        "exec_iso": rec["exec_iso"],
        "time": rec["time"],
        "reminder_minutes": rec.get("reminder_minutes"),
        "profile": rec["profile"],
        "cookies": safe_ck,
    }, ensure_ascii=False, indent=2))

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
        jq = require_jq(context)
        for j in jq.get_jobs_by_name(job_name): j.schedule_removal()
        for j in jq.get_jobs_by_name(f"rem-{job_name}"): j.schedule_removal()
        for j in jq.get_jobs_by_name(f"poll-{job_name}"): j.schedule_removal()
    except RuntimeError:
        pass
    get_jobs_store(uid).pop(job_name, None)
    save_storage(storage)
    await update.message.reply_text(f"Job '{job_name}' dibatalkan & dihapus.")

# ====== RESCHED CORE ======
async def reschedule_job(context: ContextTypes.DEFAULT_TYPE, uid: str, old_name: str,
                         booking_iso: str, exec_iso: str, hhmm: str,
                         profile: dict, cookies: dict, reminder_minutes: int | None,
                         chat_id: int, site: str) -> str:
    jq = require_jq(context)
    for j in jq.get_jobs_by_name(old_name): j.schedule_removal()
    for j in jq.get_jobs_by_name(f"rem-{old_name}"): j.schedule_removal()
    for j in jq.get_jobs_by_name(f"poll-{old_name}"): j.schedule_removal()

    leader_name = profile.get("name") or profile.get("_leader",{}).get("name","ketua")
    new_name = make_job_name(site, uid, leader_name, booking_iso, exec_iso, hhmm)

    hh, mm, ss = parse_hhmmss(hhmm)
    y, M, d = map(int, exec_iso.split("-"))
    run_at = ASIA_JAKARTA.localize(datetime(y, M, d, hh, mm, ss))
    if run_at < datetime.now(ASIA_JAKARTA):
        raise ValueError("Waktu eksekusi baru sudah lewat di Asia/Jakarta.")

    jq.run_once(
        scheduled_job, when=run_at, name=new_name,
        data={"user_id": uid, "site": site, "iso": booking_iso, "profile": profile, "cookies": cookies},
        chat_id=chat_id
    )
    if isinstance(reminder_minutes, int) and reminder_minutes > 0:
        remind_at = run_at - timedelta(minutes=reminder_minutes)
        if remind_at > datetime.now(ASIA_JAKARTA):
            jq.run_once(reminder_job, when=remind_at, name=f"rem-{new_name}",
                        data={"user_id": uid, "job_name": new_name}, chat_id=chat_id)
    return new_name

# ====== EDIT/SUPPORT COMMANDS ======
def parse_kv_pairs(s: str) -> dict:
    out = {}
    for part in s.split(";"):
        part = part.strip()
        if not part or "=" not in part: continue
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip()
    return out

async def job_edit_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if len(context.args) < 3:
        await update.message.reply_text("Format: /job_edit_time <job|index> <exec_YYYY-MM-DD> <HH:MM[:SS]>")
        return
    selector, exec_iso_in, hhmm = context.args[0], context.args[1], context.args[2]
    try:
        exec_iso = parse_date_indo_to_iso(exec_iso_in)
        parse_hhmmss(hhmm)
    except Exception as e:
        await update.message.reply_text(f"Input tidak valid: {e}")
        return
    job_name = resolve_job_selector(uid, selector)
    if not job_name:
        await update.message.reply_text("Job tidak ditemukan."); return
    jobs = get_jobs_store(uid); rec = jobs.get(job_name)
    if not rec: await update.message.reply_text("Job tidak ditemukan."); return

    site = "semeru" if job_name.startswith("semeru-") else "bromo"
    try:
        new_name = await reschedule_job(context, uid, job_name, rec["booking_iso"], exec_iso, hhmm,
                                        rec["profile"], rec.get("cookies", {}), rec.get("reminder_minutes"),
                                        rec.get("chat_id", update.effective_chat.id), site)
    except Exception as e:
        await update.message.reply_text(f"Gagal menjadwalkan ulang: {e}"); return
    jobs.pop(job_name, None); rec["exec_iso"]=exec_iso; rec["time"]=hhmm; jobs[new_name]=rec; save_storage(storage)
    await update.message.reply_text(f"Job diubah waktunya ✅\nLama: {job_name}\nBaru: {new_name}")

async def job_update_cookies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if len(context.args) < 2:
        await update.message.reply_text("Format: /job_update_cookies <job|index> _ga=...;_ga_TMVP85FKW9=...;ci_session=...")
        return
    selector = context.args[0]; kv = parse_kv_pairs(" ".join(context.args[1:]))
    job_name = resolve_job_selector(uid, selector)
    if not job_name: await update.message.reply_text("Job tidak ditemukan."); return
    jobs = get_jobs_store(uid); rec = jobs.get(job_name)
    if not rec: await update.message.reply_text("Job tidak ditemukan."); return

    cookies = rec.get("cookies", {}).copy(); changed=[]
    for k in ["_ga","_ga_TMVP85FKW9","ci_session"]:
        if k in kv and kv[k]:
            cookies[k]=kv[k]; changed.append(k)
    if not changed: await update.message.reply_text("Tidak ada cookie yang diubah."); return

    site = "semeru" if job_name.startswith("semeru-") else "bromo"
    try:
        new_name = await reschedule_job(context, uid, job_name, rec["booking_iso"], rec["exec_iso"], rec["time"],
                                        rec["profile"], cookies, rec.get("reminder_minutes"),
                                        rec.get("chat_id", update.effective_chat.id), site)
    except Exception as e:
        await update.message.reply_text(f"Gagal menjadwalkan ulang: {e}"); return

    jobs.pop(job_name, None); rec["cookies"]=cookies; jobs[new_name]=rec; save_storage(storage)
    await update.message.reply_text(f"Cookies job diupdate ✅ ({', '.join(changed)})\nLama: {job_name}\nBaru: {new_name}")

# ---------- SEMERU booking/schedule ----------
BOOK_PREFIX_BROMO  = "bromo"
BOOK_PREFIX_SEMERU = "semeru"

async def book_semeru_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not get_ci(uid):
        await update.message.reply_text("Set dulu /set_session <ci_session> (global). Kamu bisa override di form.")
        return ConversationHandler.END
    if not context.args:
        await update.message.reply_text("Format: /book_semeru <tgl_booking>")
        return ConversationHandler.END
    try:
        booking_iso = parse_date_indo_to_iso(" ".join(context.args))
    except Exception as e:
        await update.message.reply_text(f"Format tanggal salah: {e}")
        return ConversationHandler.END
    context.user_data.clear()
    context.user_data["booking_iso"] = booking_iso
    await update.message.reply_text(FORM_PROMPT_SEMERU)  # no parse_mode
    return BOOK_ASK_FORM_SEM

async def book_semeru_collect_form(update: Update, context: ContextTypes.DEFAULT_TYPE):
    leader, members, cookies, reminder_minutes, errors = parse_form_block_semeru(update.message.text)
    if errors:
        await update.message.reply_text("Ada masalah:\n- " + "\n- ".join(errors))
        return BOOK_ASK_FORM_SEM
    context.user_data["_leader"] = leader
    context.user_data["_members"] = members
    context.user_data["cookies"] = cookies
    context.user_data["reminder_minutes"] = reminder_minutes

    iso = context.user_data["booking_iso"]
    cookie_hint = ", ".join([f"{k}={'(ada)' if cookies.get(k) else '(kosong)'}" for k in ["_ga","_ga_TMVP85FKW9","ci_session"]])
    remind_txt = f"{reminder_minutes} menit" if reminder_minutes is not None else "tidak"
    member_txt = "(tidak ada)" if not members else (", ".join([m.get('nama','?') for m in members]))
    summary = (
        f"[SEMERU]\nTanggal Booking: {iso}\n"
        f"Ketua: {leader['name']} | KTP:{leader['identity_no']} | HP:{leader['hp']}\n"
        f"Pendamping:{leader['pendamping']} | Org:'{leader['organisasi']}' | Setuju:{leader['leader_setuju']} | Bayar:{leader['bank']}\n"
        f"Anggota ({len(members)}): {member_txt}\n"
        f"Cookies: {cookie_hint} | Reminder: {remind_txt}\n\n"
        "Catatan: Kuota akan dicek saat eksekusi.\n"
        "Ketik 'YA' untuk konfirmasi booking sekarang."
    )
    await update.message.reply_text(summary)
    return BOOK_CONFIRM_SEM

async def book_semeru_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.strip().lower() not in {"ya","y","yes"}:
        await update.message.reply_text("Dibatalkan.")
        return ConversationHandler.END
    uid = str(update.effective_user.id)
    ok, msg, elapsed_s, raw = do_booking_flow_semeru(
        get_ci(uid), context.user_data["booking_iso"], context.user_data["_leader"], context.user_data["_members"],
        job_cookies=context.user_data.get("cookies")
    )
    extra = ""
    if raw:
        extra = f"\n\n[Server]\nmessage: {raw.get('message','-')}\nlink: {raw.get('booking_link') or raw.get('link_redirect') or '-'}"
    await update.message.reply_text(("✅ " if ok else "❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra)
    return ConversationHandler.END

async def schedule_semeru_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not get_ci(uid):
        await update.message.reply_text("Set dulu /set_session <ci_session> (global).")
        return ConversationHandler.END
    if len(context.args) < 3:
        await update.message.reply_text("Format: /schedule_semeru <tgl_booking> <tgl_eksekusi> <HH:MM[:SS]>")
        return ConversationHandler.END
    try:
        booking_iso = parse_date_indo_to_iso(context.args[0])
        exec_iso    = parse_date_indo_to_iso(context.args[1])
    except Exception as e:
        await update.message.reply_text(f"Format tanggal salah: {e}")
        return ConversationHandler.END
    hhmm = context.args[2]
    try:
        parse_hhmmss(hhmm)
    except Exception as e:
        await update.message.reply_text(str(e)); return ConversationHandler.END

    context.user_data.clear()
    context.user_data["booking_iso"] = booking_iso
    context.user_data["exec_iso"]    = exec_iso
    context.user_data["time"]        = hhmm
    await update.message.reply_text(FORM_PROMPT_SEMERU)  # no Markdown
    return SCHED_ASK_FORM_SEM

async def schedule_semeru_collect_form(update: Update, context: ContextTypes.DEFAULT_TYPE):
    leader, members, cookies, reminder_minutes, errors = parse_form_block_semeru(update.message.text)
    if errors:
        await update.message.reply_text("Ada masalah:\n- " + "\n- ".join(errors))
        return SCHED_ASK_FORM_SEM
    context.user_data["_leader"] = leader
    context.user_data["_members"] = members
    context.user_data["cookies"] = cookies
    context.user_data["reminder_minutes"] = reminder_minutes

    cookie_hint = ", ".join([f"{k}={'(ada)' if cookies.get(k) else '(kosong)'}" for k in ["_ga","_ga_TMVP85FKW9","ci_session"]])
    remind_txt = f"{reminder_minutes} menit" if reminder_minutes is not None else "tidak"
    member_txt = "(tidak ada)" if not members else (", ".join([m.get('nama','?') for m in members]))
    summary = (
        f"[Jadwal SEMERU]\n"
        f"- Booking: {context.user_data['booking_iso']}\n"
        f"- Eksekusi: {context.user_data['exec_iso']} {context.user_data['time']} Asia/Jakarta\n"
        f"Ketua: {leader['name']} | KTP:{leader['identity_no']} | HP:{leader['hp']}\n"
        f"Anggota ({len(members)}): {member_txt}\n"
        f"Cookies: {cookie_hint} | Reminder: {remind_txt}\n\n"
        "Catatan: Kuota akan dicek pada waktu eksekusi.\n"
        "Ketik 'YA' untuk membuat jadwal."
    )
    await update.message.reply_text(summary)
    return SCHED_CONFIRM_SEM

async def schedule_semeru_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.strip().lower() not in {"ya","y","yes"}:
        await update.message.reply_text("Dibatalkan.")
        return ConversationHandler.END
    try:
        jq = require_job_queue(context)
    except RuntimeError as e:
        await update.message.reply_text(str(e)); return ConversationHandler.END

    booking_iso = context.user_data["booking_iso"]
    exec_iso    = context.user_data["exec_iso"]
    hh, mm, ss  = parse_hhmmss(context.user_data["time"])
    y, M, d = map(int, exec_iso.split("-"))
    run_at = ASIA_JAKARTA.localize(datetime(y, M, d, hh, mm, ss))
    if run_at < datetime.now(ASIA_JAKARTA):
        await update.message.reply_text("Waktu eksekusi sudah lewat."); return ConversationHandler.END

    uid = str(update.effective_user.id)
    leader_name = context.user_data["_leader"].get("name","ketua")
    job_name = make_job_name(BOOK_PREFIX_SEMERU, uid, leader_name, booking_iso, exec_iso, context.user_data["time"])

    for j in jq.get_jobs_by_name(job_name): j.schedule_removal()
    jobs_store = get_jobs_store(uid)
    jobs_store[job_name] = {
        "booking_iso": booking_iso,
        "exec_iso": exec_iso,
        "time": context.user_data["time"],
        "profile": {"_leader": context.user_data["_leader"], "_members": context.user_data["_members"]},
        "cookies": context.user_data.get("cookies", {}),
        "reminder_minutes": context.user_data.get("reminder_minutes"),
        "created_at": datetime.now(ASIA_JAKARTA).isoformat(),
        "chat_id": update.effective_chat.id
    }
    save_storage(storage)

    jq.run_once(scheduled_job, when=run_at, name=job_name,
                data={"user_id": uid, "site": "semeru", "iso": booking_iso,
                      "profile": jobs_store[job_name]["profile"], "cookies": jobs_store[job_name]["cookies"]},
                chat_id=update.effective_chat.id)

    remind_min = context.user_data.get("reminder_minutes")
    if isinstance(remind_min, int) and remind_min > 0:
        remind_at = run_at - timedelta(minutes=remind_min)
        if remind_at > datetime.now(ASIA_JAKARTA):
            jq.run_once(reminder_job, when=remind_at, name=f"rem-{job_name}",
                        data={"user_id": uid, "job_name": job_name}, chat_id=update.effective_chat.id)

    await update.message.reply_text(
        f"Terjadwal ✅ (SEMERU)\n- Booking: {booking_iso}\n- Eksekusi: {exec_iso} {context.user_data['time']} (Asia/Jakarta)\n"
        f"- Reminder: {context.user_data.get('reminder_minutes','tidak')} menit sebelum\n"
        f"Job: {job_name}"
    )
    return ConversationHandler.END

async def prov_lookup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /prov Jatim  |  /prov 35  |  /prov Jawa Timur
    if not context.args:
        await update.message.reply_text("Pakai: /prov <nama/kode>\nContoh: /prov Jatim  |  /prov 35")
        return

    q = " ".join(context.args).strip()
    code, canon, sug = province_lookup(q)
    if code:
        await update.message.reply_text(f"Kode provinsi untuk '{q}': {code} — {canon.title()}")
    else:
        if sug:
            await update.message.reply_text(f"Tidak ditemukan untuk '{q}'. Mungkin maksud: {', '.join(sug)}")
        else:
            await update.message.reply_text(f"Tidak ditemukan untuk '{q}'.")

async def kabupaten_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /kab Jatim  |  /kab 35  |  /kab Jawa Timur
    if not context.args:
        await update.message.reply_text("Pakai: /kab <provinsi>\nContoh: /kab Jatim  |  /kab 35")
        return

    q = " ".join(context.args).strip()
    code, canon, sug = province_lookup(q)
    if not code:
        if sug:
            await update.message.reply_text(f"Tidak ditemukan untuk '{q}'. Mungkin maksud: {', '.join(sug)}")
        else:
            await update.message.reply_text(f"Tidak ditemukan untuk '{q}'.")
        return

    pairs = fetch_districts_by_province(code)
    if not pairs:
        await update.message.reply_text(f"Tidak ada data kab/kota untuk {canon or q} ({code}).")
        return

    msg = format_districts_message(code, canon or "Provinsi", pairs)
    for part in split_long_message(msg):
        await update.message.reply_text(part)


# -------- Global Error Handler ----------
async def on_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.exception("Update caused error", exc_info=context.error)
    try:
        if update and update.effective_chat:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Maaf, terjadi error tak terduga.")
    except TelegramError:
        pass

# =================== BOOT ===================
def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        token = "PASTE_TELEGRAM_BOT_TOKEN_DI_SINI"

    app = Application.builder().token(token).build()

    # basic
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("set_session", set_session))
    app.add_handler(CommandHandler("jobs", jobs_list))
    app.add_handler(CommandHandler("job_detail", job_detail))
    app.add_handler(CommandHandler("job_cancel", job_cancel))
    app.add_handler(CommandHandler("job_edit_time", job_edit_time))
    app.add_handler(CommandHandler("job_update_cookies", job_update_cookies))

    # Lookup provinsi & kabupaten/kota
    app.add_handler(CommandHandler(["prov", "provinsi"], prov_lookup_cmd))
    app.add_handler(CommandHandler(["kab", "kabupaten"], kabupaten_cmd))


    # BROMO
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("book", book_entry)],
        states={
            BOOK_ASK_FORM: [MessageHandler(filters.TEXT & ~filters.COMMAND, book_collect_form)],
            BOOK_CONFIRM:  [MessageHandler(filters.TEXT & ~filters.COMMAND, book_confirm)],
        },
        fallbacks=[CommandHandler("cancel", lambda u,c: u.message.reply_text("Dibatalkan."))],
        name="bromo_book", persistent=False
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("schedule", schedule_entry)],
        states={
            SCHED_ASK_FORM: [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_collect_form)],
            SCHED_CONFIRM:  [MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u,c: u.message.reply_text("Gunakan flow yang sudah ada (tidak dipakai di kode ini)."))],
        },
        fallbacks=[CommandHandler("cancel", lambda u,c: u.message.reply_text("Dibatalkan."))],
        name="bromo_sched", persistent=False
    ))

    # SEMERU
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("book_semeru", book_semeru_entry)],
        states={
            BOOK_ASK_FORM_SEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, book_semeru_collect_form)],
            BOOK_CONFIRM_SEM:  [MessageHandler(filters.TEXT & ~filters.COMMAND, book_semeru_confirm)],
        },
        fallbacks=[CommandHandler("cancel", lambda u,c: u.message.reply_text("Dibatalkan."))],
        name="semeru_book", persistent=False
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("schedule_semeru", schedule_semeru_entry)],
        states={
            SCHED_ASK_FORM_SEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_semeru_collect_form)],
            SCHED_CONFIRM_SEM:  [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_semeru_confirm)],
        },
        fallbacks=[CommandHandler("cancel", lambda u,c: u.message.reply_text("Dibatalkan."))],
        name="semeru_sched", persistent=False
    ))
    # Contoh format
    app.add_handler(CommandHandler("examples", examples_cmd))



    app.add_error_handler(on_error)
    app.run_polling()

if __name__ == "__main__":
    main()
