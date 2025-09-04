import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse

import pytz
import requests
from bs4 import BeautifulSoup
from difflib import get_close_matches
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
from urllib3.util.retry import Retry

from network_opt import (
    create_optimized_session,
    prewarm_session,
    short_window_aggressive,
    timed_request,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,  # bisa diganti DEBUG kalau mau lebih detail
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Load .env
load_dotenv()

# ========================= CONFIG =========================
BASE = "https://bromotenggersemeru.id"
SITE_PATH_BROMO = "/booking/site/lembah-watangan"
SITE_PATH_SEMERU = "/booking/site/semeru"
CAP_URL = f"{BASE}/website/home/get_view"
ACTION_URL = f"{BASE}/website/booking/action"
COMBO_URL = f"{BASE}/website/home/combo"
ASIA_JAKARTA = pytz.timezone("Asia/Jakarta")
# === DataTables grid endpoints (untuk lookup detail by kode) ===
GRID_MEMBER = f"{BASE}/member/booking/grid"
GRID_WEBSITE = f"{BASE}/website/booking/grid"

# --- Bromo ---
BROMO_SITE_ID = "4"
BROMO_SECTOR_ID = "1"
BROMO_SITE_LABEL = "Bromo"

# --- Semeru ---
SEMERU_SITE_ID = "8"  # id_site untuk kapasitas
SEMERU_SECTOR_ID = "3"  # sesuai dump HTML (penting!)
SEMERU_SITE_LABEL = "Semeru"

STORAGE_FILE = "storage.json"  # { "<user_id>": {"ci_session": "...", "jobs": {...}} }
log = logging.getLogger("bromo-semeru-bot")

MONTHS_ID = {
    "januari": "01", "februari": "02", "maret": "03", "april": "04", "mei": "05", "juni": "06",
    "juli": "07", "agustus": "08", "september": "09", "oktober": "10", "november": "11", "desember": "12"
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


_CANON_NAMES = {_norm(k): k for k in PROVINCE_CODES.keys()}

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

# Session cache untuk pre-warming
PREWARMED_SESSIONS: dict[str, requests.Session] = {}


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
        d, m, y = s.split("-");
        return f"{y}-{m}-{d}"
    if "," in s: s = s.split(",", 1)[1].strip()
    parts = s.split()
    if len(parts) == 3 and parts[1].lower() in MONTHS_ID:
        day = parts[0].zfill(2);
        month = MONTHS_ID[parts[1].lower()];
        year = parts[2]
        return f"{year}-{month}-{day}"
    raise ValueError("Format tanggal tidak dikenali.")


def year_month_from_iso(iso: str) -> str: return iso[:7]


def extract_int(text: str) -> int:
    m = re.findall(r"\d+", text);
    return int("".join(m)) if m else 0


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
        try:
            iso_from_cell = parse_date_indo_to_iso(tanggal_text)
        except:
            continue
        if iso_from_cell == iso_date:
            quota_text = " ".join(tds[1].stripped_strings)
            quota = extract_int(quota_text)
            status = "Tersedia" if quota > 0 else "Habis / Tidak tersedia"
            return {"tanggal_cell": tanggal_text, "quota": quota, "status": status}
    return None


def _requests_session_with_retries(total: int = 3, backoff: float = 0.5) -> requests.Session:
    """
    Session requests dengan retry & backoff:
    - retry untuk connect/read/status 429/502/503/504
    - allowed_methods: POST & GET
    """
    s = requests.Session()
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        status=total,
        backoff_factor=backoff,
        status_forcelist=[429, 502, 503, 504],
        allowed_methods={"GET", "POST"},
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def check_capacity(iso_date: str, site: str) -> dict | None:
    """
    Aman dari timeout/NetworkError: kalau gagal jaringan → return None (tidak meledak).
    site: 'bromo' | 'semeru'
    """
    try:
        year_month = iso_date[:7]
        if site == "bromo":
            site_id = "4"
        elif site == "semeru":
            site_id = "8"
        else:
            raise ValueError("site harus 'bromo' atau 'semeru'")

        payload = {"action": "kapasitas", "id_site": site_id, "year_month": year_month}

        sess = _requests_session_with_retries(total=3, backoff=0.6)
        # header ringan + UA yang sudah kamu pakai
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        }

        # Timeout tuple: (connect, read) → lebih responsif saat server lemot
        resp = sess.post(CAP_URL, data=payload, headers=headers, timeout=(7, 12))
        log.info(
            "check_capacity response (%s %s) status=%s body=%s",
            site,
            iso_date,
            resp.status_code,
            resp.text,
        )
        # Bisa saja 200 tapi body kosong → anggap gagal
        if resp.status_code != 200 or not (resp.text or "").strip():
            log.warning("check_capacity: status=%s, empty=%s, site=%s, iso=%s",
                        resp.status_code, not bool((resp.text or '').strip()), site, iso_date)
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        rows = soup.select("table.table tbody tr")
        return find_quota_for_date(rows, iso_date)
    except Exception as e:
        # Tangkap semua error jaringan/parse supaya tidak crash handler lain
        log.warning("check_capacity error (%s %s): %s", site, iso_date, e)
        return None


def make_session_with_cookies(ci_session: str, extra_cookies: dict | None = None):
    sess = create_optimized_session()
    ua = ('Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
          'AppleWebKit/537.36 (KHTML, like Gecko) '
          'Chrome/139.0.0.0 Mobile Safari/537.36 Edg/139.0.0.0')
    sess.headers.update({"User-Agent": ua, "Accept": "*/*", "Accept-Language": "id,en;q=0.9,en-GB;q=0.8,en-US;q=0.7"})
    # per-job cookies
    if extra_cookies:
        if extra_cookies.get("_ga"):
            sess.cookies.set("_ga", extra_cookies["_ga"], domain=".bromotenggersemeru.id", path="/")
        if extra_cookies.get("_ga_TMVP85FKW9"):
            sess.cookies.set("_ga_TMVP85FKW9", extra_cookies["_ga_TMVP85FKW9"], domain=".bromotenggersemeru.id",
                             path="/")
        if extra_cookies.get("ci_session"):
            sess.cookies.set("ci_session", extra_cookies["ci_session"], domain="bromotenggersemeru.id", path="/")
    # fallback global
    if ci_session and not sess.cookies.get("ci_session"):
        sess.cookies.set("ci_session", ci_session, domain="bromotenggersemeru.id", path="/")
    return sess


def fetch_districts_by_province(id_province: str, ci_session: str = "", extra_cookies: dict | None = None) -> list[
    tuple[str, str]]:
    """
    Return list [(kode_kabkota, NAMA_KAB/KOTA), ...]
    Endpoint 'combo' terkadang minta cookie + header AJAX + referer yang valid.
    Mendukung action=district (baru), fallback ke kabupaten/varian lama.
    """
    sess = make_session_with_cookies(ci_session, extra_cookies)

    # --- Preflight: hangatkan cookie & referer agar backend CI "percaya"
    iso_today = (datetime.now().date()).isoformat()
    preflight_urls = [
        f"{BASE}/",
        f"{BASE}/member/booking",
        f"{BASE}{SITE_PATH_BROMO}?date_depart={iso_today}",
        f"{BASE}{SITE_PATH_SEMERU}?date_depart={iso_today}",
        f"{BASE}/peraturan/bromo",
        f"{BASE}/peraturan/semeru",
    ]
    for url in preflight_urls:
        try:
            sess.get(url, timeout=10)
        except Exception:
            pass  # biarkan saja; tujuan hanya mengisi cookie/cache

    # --- Header ala AJAX request dari browser
    ajax_headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": BASE,
        "Connection": "keep-alive",
    }
    referers = [
        f"{BASE}/member/booking",
        f"{BASE}{SITE_PATH_BROMO}?date_depart={iso_today}",
        f"{BASE}{SITE_PATH_SEMERU}?date_depart={iso_today}",
    ]

    # --- Kandidat payload (urutkan yg paling mungkin dulu)
    candidates = [
        {"action": "district", "id_province": str(id_province)},  # sesuai network log terakhirmu
        {"action": "district", "id": str(id_province)},
        {"action": "kabupaten", "id_province": str(id_province)},  # fallback lama
        {"action": "kabupaten", "id": str(id_province)},
        {"id_province": str(id_province)},  # very-lax fallback
        {"id": str(id_province)},
        {"province": str(id_province)},
    ]

    # --- Helper parse (HTML <option> atau JSON {options:[{value,text}]})
    def parse_options(text: str) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        soup = BeautifulSoup(text or "", "lxml")
        opts = soup.select("option")
        if opts:
            for opt in opts:
                val = (opt.get("value") or "").strip()
                name = opt.get_text(strip=True)
                if not val or val == "-":  # skip placeholder
                    continue
                out.append((val, name))
            return out
        # kemungkinan JSON
        try:
            j = json.loads(text or "")
            if isinstance(j, dict) and isinstance(j.get("options"), list):
                for it in j["options"]:
                    val = (it.get("value") or "").strip()
                    name = (it.get("text") or "").strip()
                    if val and name and val != "-":
                        out.append((val, name))
        except Exception:
            pass
        return out

    last_resp = None
    # --- Coba beberapa referer x beberapa payload
    for ref in referers:
        sess.headers.update({**ajax_headers, "Referer": ref})
        for payload in candidates:
            try:
                resp = sess.post(COMBO_URL, data=payload, timeout=15)
                last_resp = resp
                # kadang server set ci_session baru → ulang sekali dgn referer sama
                if (resp.status_code != 200) or not (resp.text or "").strip():
                    # jeda kecil + retry
                    time.sleep(0.4)
                    resp = sess.post(COMBO_URL, data=payload, timeout=15)
                    last_resp = resp

                if resp.status_code == 200 and (resp.text or "").strip():
                    pairs = parse_options(resp.text)
                    if pairs:
                        return pairs
            except Exception as e:
                log.debug("combo ref=%s payload=%s error=%s", ref, payload, e)

    # --- Debug bantu bila kosong
    try:
        if last_resp is not None:
            snip = (last_resp.text or "")[:300].replace("\n", " ")
            log.warning("combo(%s) kosong. HTTP %s | referer=%s | sample=%s",
                        id_province, last_resp.status_code,
                        last_resp.request.headers.get("Referer"),
                        snip or "(<empty body>)")
        else:
            log.warning("combo(%s) tidak ada respons sama sekali.", id_province)
    except Exception:
        pass
    return []


def format_districts_message(prov_code: str, prov_name: str, pairs: list[tuple[str, str]]) -> str:
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


# ---------- Lookup by Kode Booking (Leader + Anggota via secret) ----------
from urllib.parse import unquote


def _grid_headers(ci_session: str) -> dict:
    return {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": BASE,
        "referer": f"{BASE}/member/booking",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
        "cookie": f"ci_session={ci_session}",
    }


def get_booking_by_code_api(booking_code: str, ci_session: str) -> dict:
    """
    Cari 1 row booking di /member/booking/grid pakai server-side search[value]=<kode>.
    Return row dict lengkap (berisi secret, form_hash, field ketua, dll).
    """
    payload = {
        "draw": "1",
        "columns[0][data]": "id", "columns[0][name]": "", "columns[0][searchable]": "true",
        "columns[0][orderable]": "true",
        "columns[0][search][value]": "", "columns[0][search][regex]": "false",
        "columns[1][data]": "code", "columns[1][name]": "", "columns[1][searchable]": "true",
        "columns[1][orderable]": "true",
        "columns[1][search][value]": "", "columns[1][search][regex]": "false",
        "order[0][column]": "0", "order[0][dir]": "DESC",
        "start": "0", "length": "10",
        "search[value]": booking_code,
        "search[regex]": "false",
    }
    r = requests.post(GRID_MEMBER, headers=_grid_headers(ci_session), data=payload, timeout=30)
    r.raise_for_status()
    js = r.json()
    rows = js.get("data") or js.get("aaData") or []
    if not rows:
        raise RuntimeError(f"Booking {booking_code} tidak ditemukan atau tidak terlihat oleh akunmu.")
    for rr in rows:
        if str(rr.get("code", "")).strip() == booking_code:
            return rr
    return rows[0]  # fallback kalau server mengembalikan partial


def get_leader_from_row(row: dict) -> dict:
    return {
        "leader_name": row.get("booking_leader_name"),
        "leader_hp": row.get("booking_leader_hp"),
        "leader_identity_no": row.get("booking_leader_identity_no"),
        "leader_address": row.get("booking_leader_address"),
        "leader_birthdate": row.get("booking_leader_birthdate"),
        "email": row.get("email"),
        "country": row.get("country"),
        "code": row.get("code"),
        "date_depart": row.get("date_depart"),
        "date_arrival": row.get("date_arrival"),
        "booking_status": row.get("booking_status"),
        "total_pendaki": row.get("total_pendaki"),
        "form_hash": row.get("form_hash"),
        "secret": row.get("secret"),
    }


def _build_website_grid_payload(secret_raw: str, start=0, length=100, search_value: str = ""):
    sec_val = unquote(secret_raw)
    columns = [
        ("nama", True, True, ""),
        ("country", True, True, ""),
        ("hp_member", True, True, ""),
        ("birthdate", True, True, ""),
        ("identity_no", True, True, ""),
        ("", True, False, ""),
    ]
    data = {
        "secret": sec_val,
        "draw": "2",
        "order[0][column]": "0",
        "order[0][dir]": "asc",
        "start": str(start),
        "length": str(length),
        "search[value]": search_value or "",
        "search[regex]": "false",
    }
    for idx, (col, searchable, orderable, search_val) in enumerate(columns):
        data[f"columns[{idx}][data]"] = col
        data[f"columns[{idx}][name]"] = ""
        data[f"columns[{idx}][searchable]"] = "true" if searchable else "false"
        data[f"columns[{idx}][orderable]"] = "true" if orderable else "false"
        data[f"columns[{idx}][search][value]"] = search_val
        data[f"columns[{idx}][search][regex]"] = "false"
    return data


def get_members_by_secret(secret: str, ci_session: str, page_size: int = 200, search_value: str = "") -> tuple[
    list, int]:
    """
    Ambil seluruh anggota dari /website/booking/grid menggunakan secret yang didapat dari grid member.
    Bisa difilter (search_value) dan handle paging otomatis.
    Return (list_rows, total).
    """
    headers = _grid_headers(ci_session)
    headers["referer"] = f"{BASE}/booking/site/semeru"
    r0 = requests.post(GRID_WEBSITE, headers=headers,
                       data=_build_website_grid_payload(secret, 0, page_size, search_value),
                       timeout=30)
    r0.raise_for_status()
    j0 = r0.json()
    total = int(j0.get("recordsTotal", j0.get("iTotalRecords", 0)))
    rows = list(j0.get("data") or j0.get("aaData") or [])
    start = page_size
    while start < total:
        rx = requests.post(GRID_WEBSITE, headers=headers,
                           data=_build_website_grid_payload(secret, start, page_size, search_value),
                           timeout=30)
        rx.raise_for_status()
        jx = rx.json()
        rows += (jx.get("data") or jx.get("aaData") or [])
        start += page_size
    return rows, total


# =================== BROMO FLOWS ===================
def add_or_update_members_bromo(sess: requests.Session, secret: str, male: int, female: int, id_country: str = "99"):
    if male < 0 or female < 0: return
    if male == 0 and female == 0: return
    payload = {"action": "anggota_update", "secret": secret, "id": "", "male": str(male), "female": str(female),
               "id_country": id_country}
    try:
        _ = sess.post(ACTION_URL, data=payload, timeout=30)
    except Exception as e:
        log.warning("anggota_update (Bromo) error: %s", e)


def do_booking_flow_bromo(ci_session: str, iso_date: str, profile: dict,
                          job_cookies: dict | None = None,
                          sess: requests.Session | None = None) -> tuple[bool, str, float, dict | None]:
    t0 = time.perf_counter()

    # ✅ JIT: cek kuota saat eksekusi
    cap = check_capacity(iso_date, "bromo")
    if not cap:
        return False, f"Kuota: tanggal {iso_date} tidak ditemukan.", time.perf_counter() - t0, None
    if cap["quota"] <= 0:
        return False, f"Kuota {cap['tanggal_cell']}: {cap['quota']} (Tidak tersedia).", time.perf_counter() - t0, None

    sess = sess or make_session_with_cookies(ci_session, job_cookies)
    referer = build_referer_url(SITE_PATH_BROMO, iso_date)
    r = sess.get(referer, timeout=30)
    if r.status_code != 200:
        return False, f"Gagal GET booking page: {r.status_code}", time.perf_counter() - t0, None
    try:
        secret, form_hash, _ = get_tokens_from_cnt_page(r.text, debug_name="debug_bromo.html")
    except Exception as e:
        return False, f"Gagal ekstrak token: {e}", time.perf_counter() - t0, None

    sess.headers.update({"X-Requested-With": "XMLHttpRequest", "Origin": BASE, "Referer": referer})
    try:
        _ = sess.post(ACTION_URL, data={"action": "update_hash", "secret": secret, "form_hash": form_hash}, timeout=30)
        _ = sess.post(ACTION_URL, data={"action": "validate_booking", "secret": secret, "form_hash": form_hash},
                      timeout=30)
    except Exception as e:
        return False, f"Gagal update/validate hash: {e}", time.perf_counter() - t0, None

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
        return False, f"Gagal POST do_booking: {e}", time.perf_counter() - t0, None

    elapsed = time.perf_counter() - t0
    ct = (resp.headers.get("Content-Type") or "").lower()
    if "json" in ct:
        try:
            data = resp.json()
        except Exception:
            return False, f"Respon tidak bisa dibaca JSON: {resp.text[:400]}", elapsed, None
        if data.get("status") is True:
            link = data.get("booking_link") or data.get("link_redirect") or "(tidak ada link)"
            return True, f"Booking BERHASIL.\nLink: {link}\nServer message: {data.get('message', '-')}", elapsed, data
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
    cookies = {"_ga": "", "_ga_TMVP85FKW9": "", "ci_session": ""}
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

    leader["name"] = get_ci("Nama")
    leader["identity_no"] = get_ci("No KTP")
    leader["hp"] = get_ci("No HP")
    leader["birthdate"] = get_ci("Tanggal Lahir")
    leader["address"] = get_ci("Alamat")
    leader["id_province"] = get_ci("ID Provinsi")
    leader["id_district"] = get_ci("ID Kabupaten/Kota")
    leader["pendamping"] = get_ci("Pendamping (0/1)") or "0"
    leader["organisasi"] = get_ci("Organisasi") or ""
    leader["leader_setuju"] = get_ci("Leader Setuju (0/1)") or "0"
    leader_bank_raw = (get_ci("Metode Bayar") or "qris").strip().lower()
    valid_banks = {"qris": "qris", "va-mandiri": "VA-Mandiri", "va-bni": "VA-BNI"}
    leader["bank"] = valid_banks.get(leader_bank_raw, "qris")

    # cookies + reminder
    cookies["_ga"] = get_ci("_ga")
    cookies["_ga_TMVP85FKW9"] = get_ci("_ga_TMVP85FKW9") or get_ci("_ga_tmpvp85fkw9") or get_ci("_ga_tmvp85fkw9")
    cookies["ci_session"] = get_ci("ci_session")
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
    if leader["pendamping"] not in {"0", "1"}:
        errors.append("Pendamping harus 0/1.")
    if leader["leader_setuju"] not in {"0", "1"}:
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
        if m["id_gender"] not in {"1", "2"}:
            errors.append(f"Anggota {i}: Gender harus 1/2.")
        members.append(m)

    if len(members) > 9:
        errors.append("Maksimal 9 anggota.")

    return leader, members, cookies, reminder_minutes, errors


# ======== SEMERU: helpers & 2-phase booking ========

log = logging.getLogger("semeru")


def _apply_ajax_headers(sess: requests.Session, referer_url: str):
    """Set header AJAX yang dibutuhkan endpoint /website/booking/action."""
    sess.headers.update({
        "X-Requested-With": "XMLHttpRequest",
        "Origin": BASE,
        "Referer": referer_url,
        "Accept": "*/*",
        "Connection": "keep-alive",
    })


def _post_json(sess: requests.Session, url: str, data: dict, timeout: int = 30) -> tuple[bool, dict | None, str]:
    """
    Return: (is_json, json_obj|None, raw_text)
    - is_json=True jika Content-Type JSON & parsing sukses.
    - raw_text selalu dikembalikan untuk debug/log.
    """
    r = sess.post(url, data=data, timeout=timeout)
    ct = (r.headers.get("Content-Type") or "").lower()
    txt = r.text or ""
    if "json" in ct:
        try:
            return True, r.json(), txt
        except Exception:
            return False, None, txt
    return False, None, txt


def _member_update_once(sess: requests.Session, secret: str, form_hash: str, m: dict) -> tuple[bool, str]:
    """Kirim 1 anggota. Sukses = server mengembalikan JSON dengan status=True."""
    payload = {
        "action": "member_update",
        "id": "",
        "secret": secret,
        "form_hash": form_hash or "",
        "nama": m.get("nama", ""),
        "birthdate": m.get("birthdate", ""),
        "anggota_setuju": m.get("anggota_setuju", "1"),
        "id_gender": m.get("id_gender", "1"),
        "alamat": m.get("alamat", ""),
        "id_identity": m.get("id_identity", "1"),
        "identity_no": m.get("identity_no", ""),
        "hp_member": m.get("hp_member", ""),
        "hp_keluarga": m.get("hp_keluarga", ""),
        "id_job": m.get("id_job", "6"),
        "id_country": m.get("id_country", "99"),
    }
    is_json, j, raw = _post_json(sess, ACTION_URL, payload, timeout=30)
    if is_json and isinstance(j, dict):
        ok = bool(j.get("status", True))
        msg = (j.get("message") or "OK") if ok else (j.get("message") or "Gagal (status=false)")
        return ok, str(msg)
    return False, (raw[:160] + "…")


def _add_members_batch(sess: requests.Session, secret: str, form_hash: str, members: list[dict]) -> tuple[
    int, list[str]]:
    """Tambah hingga 9 anggota. Return (jumlah_sukses, catatan_per_anggota)."""
    added = 0
    notes: list[str] = []
    for idx, m in enumerate(members, start=1):
        if idx > 9:
            notes.append("Lewati anggota > 9.")
            break
        if not (m.get("nama") or "").strip():
            notes.append(f"[{idx}] nama kosong → skip")
            continue
        ok, msg = _member_update_once(sess, secret, form_hash, m)
        if ok:
            added += 1
            notes.append(f"[{idx}] OK")
        else:
            notes.append(f"[{idx}] gagal: {msg}")
            if "maksimal 9" in msg.lower():
                break
    return added, notes


def _prime_tokens_semeru(sess: requests.Session, booking_iso: str) -> tuple[str, str, dict]:
    """
    Ambil ulang secret & form_hash dari halaman Semeru.
    Me-referer ke URL booking yang sama (date_depart=booking_iso).
    """
    referer = build_referer_url(SITE_PATH_SEMERU, booking_iso)
    r = sess.get(referer, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GET prime token gagal: HTTP {r.status_code}")
    secret, form_hash, booking_obj = get_tokens_from_cnt_page(r.text, debug_name="debug_semeru_reprime.html")
    if not secret:
        raise RuntimeError("Token 'secret' kosong saat re-prime.")
    return secret, (form_hash or ""), booking_obj


from http.cookiejar import Cookie


def has_cookie(jar: requests.cookies.RequestsCookieJar, name: str, domain: str | None = None,
               path: str | None = None) -> bool:
    for c in jar:
        if c.name == name and (domain is None or c.domain == domain) and (path is None or c.path == path):
            return True
    return False


def set_unique_cookie(jar: requests.cookies.RequestsCookieJar, name: str, value: str, domain: str, path: str = "/"):
    # hapus semua cookie existing dengan nama tsb agar tak konflik
    to_clear = []
    for c in list(jar):
        if c.name == name:
            to_clear.append((c.domain, c.path, c.name))
    for d, p, n in to_clear:
        try:
            jar.clear(d, p, n)
        except Exception:
            pass
    jar.set(name, value, domain=domain, path=path)


def extract_tokens_from_html(html: str, debug_name: str = "debug_semeru.html"):
    """
    Kembalikan (secret, form_hash, booking_obj).
    Cari di beberapa pola:
    - <div class="cnt-page">{"booking":{...}}</div>
    - <script id="cnt-page" type="application/json">...</script>
    - JSON inline di <script> yang mengandung "booking" & "secret"
    """
    soup = BeautifulSoup(html, "lxml")

    # 1) persis .cnt-page
    holder = soup.select_one(".cnt-page")
    if holder:
        txt = holder.get_text("", strip=True)
        try:
            data = json.loads(txt)
            booking = data.get("booking", {}) if isinstance(data, dict) else {}
            secret = booking.get("secret")
            form_hash = booking.get("form_hash", "")
            if secret:
                return secret, form_hash, booking
        except Exception:
            pass  # lanjut ke cara lain

    # 2) script JSON khusus
    script_json = soup.select_one('script#cnt-page[type="application/json"]')
    if script_json:
        txt = script_json.get_text("", strip=True)
        try:
            data = json.loads(txt)
            booking = data.get("booking", {}) if isinstance(data, dict) else {}
            secret = booking.get("secret")
            form_hash = booking.get("form_hash", "")
            if secret:
                return secret, form_hash, booking
        except Exception:
            pass

    # 3) fallback: scan semua <script> untuk JSON yg mengandung "booking" & "secret"
    for sc in soup.find_all("script"):
        code = (sc.string or sc.get_text() or "").strip()
        if not code:
            continue
        if "booking" in code and "secret" in code:
            # ambil blok {...} terdekat dengan "booking"
            # cari object besar yang punya kunci "booking"
            for m in re.finditer(r"\{.*?\}", code, flags=re.DOTALL):
                chunk = m.group(0)
                if '"booking"' in chunk and '"secret"' in chunk:
                    # kadang ada trailing koma/JS → coba bersihkan karakter tak valid ringan
                    cleaned = re.sub(r",\s*}", "}", chunk)
                    try:
                        data = json.loads(cleaned)

                        # fleksibel: kadang langsung {"booking":{...}}, kadang {"some":{"booking":{...}}}
                        def deep_get_booking(obj):
                            if isinstance(obj, dict):
                                if "booking" in obj and isinstance(obj["booking"], dict):
                                    return obj["booking"]
                                for v in obj.values():
                                    res = deep_get_booking(v)
                                    if res is not None:
                                        return res
                            return None

                        booking = deep_get_booking(data) or {}
                        secret = booking.get("secret")
                        form_hash = booking.get("form_hash", "")
                        if secret:
                            return secret, form_hash, booking
                    except Exception:
                        continue

    # 4) simpan debug lalu gagal
    try:
        with open(debug_name, "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass
    raise RuntimeError(f"Elemen/JSON booking tidak ditemukan. HTML disimpan ke {debug_name}")


def _prepare_sem_sess(ci_session: str, job_cookies: dict | None) -> requests.Session:
    sess = requests.Session()
    ua = ('Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36 Edg/139.0.0.0')
    sess.headers.update({
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "id,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
        "Connection": "keep-alive",
    })

    COOKIE_DOMAIN = ".bromotenggersemeru.id"

    # gunakan helper aman dari patch sebelumnya:
    # set_unique_cookie(...) & has_cookie(...)
    if job_cookies:
        for ck, val in job_cookies.items():
            if val:
                set_unique_cookie(sess.cookies, ck, val, domain=COOKIE_DOMAIN)

    if ci_session and not has_cookie(sess.cookies, "ci_session"):
        set_unique_cookie(sess.cookies, "ci_session", ci_session, domain=COOKIE_DOMAIN)

    return sess


def _preflight_sem(sess: requests.Session):
    # 1) homepage → kadang set beberapa cookie/cache
    try:
        r0 = sess.get(f"{BASE}/", timeout=20)
        r0.raise_for_status()
    except Exception as e:
        log.debug("Preflight step 1 gagal (home): %s", e)

    # 2) peraturan semeru → sering jadi jalur normal sebelum booking
    try:
        r1 = sess.get(f"{BASE}/peraturan/semeru", timeout=20)
        r1.raise_for_status()
    except Exception as e:
        log.debug("Preflight step 2 gagal (peraturan): %s", e)


# === SEMERU: list & delete existing members ===
def semeru_list_members(sess: requests.Session, booking_iso: str) -> list[dict]:
    """
    Ambil daftar anggota yg sudah tersimpan di server (per sesi/secret & tanggal).
    Return list of rows (id, identity_no, nama, secret, date_depart, dll).
    """
    try:
        # Banyak implementasi CI/DataTables cukup pakai draw/start/length.
        # Kita kirim minimal param + tanggal biar server “tahu konteks”.
        payload = {
            "draw": "1",
            "start": "0",
            "length": "200",
            "date_depart": booking_iso,  # sering dipakai sebagai filter server-side
        }
        r = sess.post(f"{BASE}/website/booking/grid", data=payload, timeout=30,
                      headers={"X-Requested-With": "XMLHttpRequest"})
        r.raise_for_status()
        j = r.json()
        data = j.get("data", [])
        # normalisasi key penting
        out = []
        for row in data:
            out.append({
                "id": str(row.get("id") or ""),
                "identity_no": str(row.get("identity_no") or ""),
                "nama": str(row.get("nama") or ""),
                "secret": str(row.get("secret") or ""),
                "date_depart": str(row.get("date_depart") or ""),
                "date_arrival": str(row.get("date_arrival") or ""),
            })
        return out
    except Exception as e:
        log.warning("semeru_list_members error: %s", e)
        return []


def semeru_member_delete(sess: requests.Session, secret: str, member_id: str) -> tuple[bool, str]:
    """
    Hapus satu anggota by id (row.id dari grid) menggunakan secret yg relevan.
    """
    payload = {
        "action": "member_delete",
        "secret": secret,
        "id": member_id,
    }
    is_json, j, raw = _post_json(sess, ACTION_URL, payload, timeout=30)
    if is_json and isinstance(j, dict):
        return bool(j.get("status", False)), str(j.get("message") or "-")
    return False, (raw[:160] + "…")


def do_booking_flow_semeru(
    ci_session: str,
    booking_iso: str,
    leader: dict,
    members: list,
    job_cookies: dict | None = None,
    sess: requests.Session | None = None
) -> tuple[bool, str, float, dict | None]:
    t0 = time.perf_counter()
    logger = globals().get("log") or logging.getLogger("booking-semeru")
    logger.warning("Tanggal berangkat (ISO): %s", booking_iso)

    safe_members = [m for m in (members or []) if (m.get("nama") or "").strip()]
    if len(safe_members) == 0:
        return False, "Form SEMERU wajib minimal 1 anggota (ketua + 1).", time.perf_counter()-t0, None

    # ——— Cek kuota
    cap = check_capacity(booking_iso, "semeru")
    if not cap:
        return False, f"Kuota: tanggal {booking_iso} tidak ditemukan.", time.perf_counter()-t0, None
    if cap["quota"] <= 0:
        return False, f"Kuota {cap['tanggal_cell']}: {cap['quota']} (Tidak tersedia).", time.perf_counter()-t0, None

    # ——— Session & cookies (fresh jar)
    def _new_session():
        s = create_optimized_session()
        ua = ('Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
              'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36 Edg/139.0.0.0')
        s.headers.update({
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "id,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
            "Connection": "keep-alive",
        })
        dom = ".bromotenggersemeru.id"
        def _set(name, val):
            if val:
                s.cookies.set(name, val, domain=dom, path="/")
        if job_cookies:
            _set("_ga", job_cookies.get("_ga"))
            _set("_ga_TMVP85FKW9", job_cookies.get("_ga_TMVP85FKW9"))
            _set("ci_session", job_cookies.get("ci_session"))
        if ci_session and not (job_cookies or {}).get("ci_session"):
            _set("ci_session", ci_session)
        return s

    sess = sess or _new_session()

    def _prime_secret(sess_obj: requests.Session) -> tuple[str, str]:
        # preflight ringan
        for url in (f"{BASE}/", f"{BASE}/peraturan/semeru"):
            try: sess_obj.get(url, timeout=15)
            except Exception: pass
        # cache-busting
        ts = int(time.time()*1000)
        referer = f"{BASE}{SITE_PATH_SEMERU}?date_depart={booking_iso}&t={ts}"
        r = sess_obj.get(
            referer, timeout=30,
            headers={
                "Referer": f"{BASE}/peraturan/semeru?date_depart={booking_iso}",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
        )
        if r.status_code != 200:
            raise RuntimeError(f"Gagal GET page: HTTP {r.status_code}")
        secret, form_hash, _ = extract_tokens_from_html(r.text, debug_name="debug_semeru.html")
        # siapkan AJAX headers utk POST
        sess_obj.headers.update({
            "X-Requested-With": "XMLHttpRequest",
            "Origin": BASE,
            "Referer": referer,
        })
        # update_hash + validate
        sess_obj.post(ACTION_URL, data={"action":"update_hash","secret":secret,"form_hash":form_hash or ""}, timeout=30)
        sess_obj.post(ACTION_URL, data={"action":"validate_booking","secret":secret,"form_hash":form_hash or ""}, timeout=30)
        return secret, (form_hash or "")

    def _add_member(sess_obj: requests.Session, secret: str, form_hash: str, idx: int, m: dict) -> tuple[bool, str]:
        payload = {
            "action": "member_update",
            "id": "",
            "secret": secret,
            "form_hash": form_hash or "",
            "nama": m.get("nama",""),
            "birthdate": m.get("birthdate",""),
            "anggota_setuju": m.get("anggota_setuju","1"),
            "id_gender": m.get("id_gender","1"),
            "alamat": m.get("alamat",""),
            "id_identity": m.get("id_identity","1"),
            "identity_no": m.get("identity_no",""),
            "hp_member": m.get("hp_member",""),
            "hp_keluarga": m.get("hp_keluarga",""),
            "id_job": m.get("id_job","6"),
            "id_country": m.get("id_country","99"),
        }
        r = sess_obj.post(ACTION_URL, data=payload, timeout=30)
        ct = (r.headers.get("Content-Type") or "").lower()
        if "json" in ct:
            try:
                dj = r.json()
            except Exception:
                return False, "Respon member_update bukan JSON"
            ok = bool(dj.get("status", False))
            return ok, str(dj.get("message") or "-")
        return False, "Respon member_update non-JSON"

    def _do_booking(sess_obj: requests.Session, secret: str, form_hash: str) -> tuple[bool, dict | None, str]:
        try:
            arr_iso = (datetime.fromisoformat(booking_iso) + timedelta(days=1)).date().isoformat()
        except Exception:
            arr_iso = booking_iso
        bank_norm = {"qris":"qris","va-mandiri":"VA-Mandiri","va-bni":"VA-BNI"} \
            .get((leader.get("bank") or "qris").strip().lower(), "qris")
        bp = {
            "action": "do_booking",
            "secret": secret,
            "form_hash": form_hash or "",
            "id_sector": SEMERU_SECTOR_ID,
            "id_site":   SEMERU_SITE_ID,
            "site":      SEMERU_SITE_LABEL,
            "date_depart": booking_iso,
            "date_arrival": arr_iso,
            "pendamping":     leader.get("pendamping","0"),
            "organisasi":     leader.get("organisasi",""),
            "name":           leader.get("name",""),
            "id_country":     leader.get("id_country","99"),
            "birthdate":      leader.get("birthdate",""),
            "leader_setuju":  leader.get("leader_setuju","1"),
            "id_gender":      leader.get("id_gender","1"),
            "id_identity":    leader.get("id_identity","1"),
            "identity_no":    leader.get("identity_no",""),
            "address":        leader.get("address",""),
            "id_province":    leader.get("id_province",""),
            "id_district":    leader.get("id_district",""),
            "hp":             leader.get("hp",""),
            "table-member_length": "10",
            "bank": bank_norm,
            "termsCheckbox": "on",
        }
        r = sess_obj.post(ACTION_URL, data=bp, timeout=60)
        ct = (r.headers.get("Content-Type") or "").lower()
        if "json" not in ct:
            return False, None, f"Respon non-JSON do_booking: {r.text[:400]}"
        try:
            dj = r.json()
        except Exception:
            return False, None, f"Respon do_booking tak bisa JSON: {r.text[:400]}"
        return bool(dj.get("status")), dj, str(dj.get("message") or "-")

    # ——— PRIME secret pertama
    try:
        secret, form_hash = _prime_secret(sess)
        logger.info("Token OK: secret_len=%d, form_hash_len=%d", len(secret or ""), len(form_hash or ""))
    except Exception as e:
        return False, f"Gagal ekstrak token: {e}", time.perf_counter()-t0, None

    # === Bersihkan anggota yang sudah terdaftar di secret/tanggal ini ===
    try:
        existing = semeru_list_members(sess, booking_iso)
        to_del = [row for row in existing if row.get("date_depart") == booking_iso]
        if to_del:
            logger.info("Ditemukan %d anggota existing → hapus dulu", len(to_del))
            for row in to_del:
                row_secret = row.get("secret") or secret
                okdel, msgdel = semeru_member_delete(sess, row_secret, row["id"])
                logger.info("Del member id=%s (%s) → %s (%s)", row["id"], row.get("nama"), "OK" if okdel else "FAIL", msgdel)
                time.sleep(0.15)
            try:
                sess.post(ACTION_URL, data={"action":"validate_booking","secret":secret,"form_hash":form_hash or ""}, timeout=20)
            except Exception:
                pass
    except Exception as e:
        logger.warning("Cleanup existing members gagal: %s", e)

    # ——— Coba tambah 1 anggota dulu
    first_add_msg = ""
    ok_first, msg_first = _add_member(sess, secret, form_hash, 1, safe_members[0])
    if ok_first:
        first_add_msg = "OK"
    else:
        first_add_msg = msg_first
        logger.warning("[member 1] server warn: %s", msg_first)

    # ——— Jika langsung "Maksimal 9 anggota" → re-prime secret sekali
    if (not ok_first) and ("maksimal 9" in msg_first.lower()):
        try:
            sess = _new_session()
            secret, form_hash = _prime_secret(sess)
            ok_first, msg_first = _add_member(sess, secret, form_hash, 1, safe_members[0])
            logger.warning("Re-prime secret → add member 1: %s (%s)", "OK" if ok_first else "FAIL", msg_first)
        except Exception as e:
            logger.warning("Re-prime gagal: %s", e)

    added = 0
    fail_msgs: list[str] = []

    if ok_first:
        added += 1
        # Jalur A: tambah sisa anggota (2..9) lalu do_booking
        for i, m in enumerate(safe_members[1:9], start=2):
            ok_m, msg_m = _add_member(sess, secret, form_hash, i, m)
            if ok_m:
                added += 1
            else:
                fail_msgs.append(f"#{i}: {msg_m}")
                logger.warning("[member %s] server warn: %s", i, msg_m)
                if "maksimal 9" in msg_m.lower():
                    break
            time.sleep(0.2)
        try:
            sess.post(ACTION_URL, data={"action": "validate_booking", "secret": secret, "form_hash": form_hash or ""}, timeout=20)
        except Exception:
            pass
        ok_do, data_do, msg_do = _do_booking(sess, secret, form_hash)
    else:
        # Jalur B: do_booking dulu (ketua + Anggota 1), baru tambah sisa
        ok_do, data_do, msg_do = _do_booking(sess, secret, form_hash)
        if not ok_do and "minimal 2" in msg_do.lower():
            ok_retry, msg_retry = _add_member(sess, secret, form_hash, 1, safe_members[0])
            logger.warning("Fallback add first member → %s (%s)", "OK" if ok_retry else "FAIL", msg_retry)
            ok_do, data_do, msg_do = _do_booking(sess, secret, form_hash)
        if ok_do:
            for i, m in enumerate(safe_members[1:9], start=2):
                ok_m, msg_m = _add_member(sess, secret, form_hash, i, m)
                if ok_m:
                    added += 1
                else:
                    fail_msgs.append(f"#{i}: {msg_m}")
                    logger.warning("[member %s] server warn: %s", i, msg_m)
                    if "maksimal 9" in msg_m.lower():
                        break
                time.sleep(0.2)
            try:
                sess.post(ACTION_URL, data={"action": "validate_booking", "secret": secret, "form_hash": form_hash or ""}, timeout=20)
            except Exception:
                pass

    # ——— Error handling khusus duplikat identitas
    if not ok_do:
        if "nomor identitas ganda" in msg_do.lower():
            logger.warning("Deteksi duplikat identitas → cleanup & retry sekali")
            try:
                existing = semeru_list_members(sess, booking_iso)
                for row in existing:
                    if row.get("date_depart") == booking_iso:
                        row_secret = row.get("secret") or secret
                        semeru_member_delete(sess, row_secret, row["id"])
                        time.sleep(0.1)
                sess.post(ACTION_URL, data={"action": "validate_booking", "secret": secret, "form_hash": form_hash or ""}, timeout=20)
            except Exception as e:
                logger.warning("Cleanup on duplicate fail: %s", e)
            ok_do, data_do, msg_do = _do_booking(sess, secret, form_hash)

        if not ok_do:
            return False, f"Booking Semeru GAGAL {secret[:12]}...: {msg_do}", time.perf_counter() - t0, (data_do or None)

    # ——— SUKSES → susun pesan dengan KODE BOOKING
    elapsed = time.perf_counter() - t0
    link = (data_do or {}).get("booking_link") or (data_do or {}).get("link_redirect") or "-"

    # coba tebak kode booking dari JSON / link
    def _guess_booking_code(data_obj: dict | None, link_text: str) -> str | None:
        # 1) field langsung
        for k in ("code", "booking_code", "bookingCode"):
            v = (data_obj or {}).get(k)
            if isinstance(v, str) and "-" in v:
                return v.strip()
        # 2) nested obj
        for node in ("booking", "data", "result"):
            sub = (data_obj or {}).get(node)
            if isinstance(sub, dict):
                for k in ("code", "booking_code"):
                    v = sub.get(k)
                    if isinstance(v, str) and "-" in v:
                        return v.strip()
        # 3) dari link: ?code=... atau segmen path
        if link_text and isinstance(link_text, str):
            m = re.search(r"[?&]code=([A-Z0-9\-]+)", link_text)
            if m:
                return m.group(1)
            m = re.search(r"([A-Z]{2,}-[0-9\-]{6,})", link_text)
            if m:
                return m.group(1)
            parts = [p for p in link_text.split("/") if p]
            for p in reversed(parts):
                if re.match(r"^[A-Z]{2,}-[0-9\-]{6,}$", p):
                    return p
        return None

    booking_code = _guess_booking_code(data_do, link)

    extra_note = ""
    if fail_msgs:
        extra_note = "\nCatatan anggota gagal:\n- " + "\n- ".join(fail_msgs[:5])
        if len(fail_msgs) > 5:
            extra_note += f"\n- (+{len(fail_msgs)-5} error lainnya)"

    # TAMPILKAN kode booking + shortcut command detail
    # ganti '/booking_detail' jika command-mu bernama lain
    cmd_hint = f"\n• Detail cepat: <code>/booking_detail {booking_code}</code>" if booking_code else ""
    msg = (
        "✅ Booking Semeru BERHASIL.\n"
        f"• Kode Booking: <code>{booking_code or '-'}</code>\n"
        f"• Link: {link}\n"
        f"• Anggota berhasil ditambahkan: {added} (di luar ketua)\n"
        f"• Pesan server: {(data_do or {}).get('message','-')}"
        f"{cmd_hint}"
        f"{extra_note}"
    )

    return True, msg, elapsed, data_do

async def quota_semeru_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /quota_semeru <YYYY-MM-DD>
    Cek kuota pendakian semeru pada tanggal tsb.
    """
    if not context.args:
        await update.message.reply_text("Format: /quota_semeru <YYYY-MM-DD>")
        return

    date_arg = context.args[0].strip()
    try:
        # normalize ke ISO date
        iso_date = datetime.fromisoformat(date_arg).date().isoformat()
    except Exception:
        await update.message.reply_text("Tanggal tidak valid. Format: YYYY-MM-DD, mis. /quota_semeru 2025-09-30")
        return

    try:
        cap = check_capacity(iso_date, "semeru")
    except Exception as e:
        await update.message.reply_text(f"Gagal cek kuota: {e}")
        return

    if not cap:
        await update.message.reply_text(f"Tanggal {iso_date} tidak ditemukan di server.")
        return

    quota = cap.get("quota", "-")
    tanggal_label = cap.get("tanggal_cell", iso_date)
    msg = (
        f"📅 Kuota Semeru {tanggal_label}\n"
        f"🎟️ Tersisa: {quota}"
    )
    await update.message.reply_text(msg)
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
    cookies = {"_ga": "", "_ga_TMVP85FKW9": "", "ci_session": ""}
    reminder_minutes = None
    errors = []

    for raw in text.splitlines():
        if ":" not in raw: continue
        label, value = raw.split(":", 1)
        key = label.strip().lower()
        val = value.strip()
        if key in FORM_KEYS_BROMO:
            mapped = FORM_KEYS_BROMO[key]
            if mapped in {"_ga", "_ga_TMVP85FKW9", "ci_session"}:
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
    if profile["id_gate"] and profile["id_gate"] not in {"1", "2", "3", "4"}:
        errors.append("Pintu Masuk harus 1/2/3/4.")
    if profile["id_vehicle"] and profile["id_vehicle"] not in {"1", "2", "3", "4", "6"}:
        errors.append("Jenis Kendaraan harus 1/2/3/4/6.")
    if profile["vehicle_count"] and (
            not profile["vehicle_count"].isdigit() or not (1 <= int(profile["vehicle_count"]) <= 20)):
        errors.append("Jumlah Kendaraan harus 1-20.")
    # normalisasi bank untuk bromo juga
    valid_banks = {"qris": "qris", "va-mandiri": "VA-Mandiri", "va-bni": "VA-BNI"}
    profile["bank"] = valid_banks.get((profile.get("bank") or "qris").strip().lower(), "qris")
    for fld in ["male", "female"]:
        if profile[fld] and (not profile[fld].isdigit() or not (0 <= int(profile[fld]) <= 19)):
            errors.append(f"{'Laki-laki' if fld == 'male' else 'Perempuan'} harus 0–19.")
    if profile["birthdate"] and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", profile["birthdate"]):
        errors.append("Tanggal Lahir harus YYYY-MM-DD.")
    return profile, cookies, reminder_minutes, errors


# =================== TELEGRAM ===================
HELP_TEXT = (
    "📖 <b>Panduan Perintah</b>\n\n"
    "🔹 <b>Dasar</b>\n"
    "   • /start — cek bot\n"
    "   • /help — tampilkan bantuan ini\n"
    "   • /set_session <ci_session>\n"
    "   • /examples — contoh format booking\n\n"

    "🌍 <b>Lookup Wilayah</b>\n"
    "   • /prov <nama/kode>\n"
    "     └─ contoh: <code>/prov Jatim</code> | <code>/prov 35</code>\n"
    "   • /kab <nama/kode>\n"
    "     └─ contoh: <code>/kab \"Jawa Timur\"</code> | <code>/kab 35</code>\n\n"

    "⛰️ <b>BROMO</b>\n"
    "   • /book <tgl_booking>\n"
    "     └─ contoh: <code>2025-09-30</code> | <code>30-09-2025</code> | <code>30 September 2025</code>\n"
    "   • /schedule <tgl_booking> <tgl_eksekusi> <HH:MM[:SS]>\n"
    "     └─ contoh: <code>/schedule 2025-09-30 2025-09-29 23:59</code>\n\n"

    "🏔️ <b>SEMERU</b>\n"
    "   • /book_semeru <tgl_booking>\n"
    "     └─ contoh: <code>2025-09-30</code> | <code>30-09-2025</code> | <code>30 September 2025</code>\n"
    "   • /schedule_semeru <tgl_booking> <tgl_eksekusi> <HH:MM[:SS]>\n"
    "     └─ contoh: <code>/schedule_semeru 2025-09-30 2025-09-29 23:59</code>\n\n"

    "🗂️ <b>Manajemen Job</b>\n"
    "   • /jobs — daftar job\n"
    "   • /job_detail <job|index>\n"
    "   • /job_cancel <job|index>\n"
    "   • /job_edit_time <job|index> <exec_YYYY-MM-DD> <HH:MM[:SS]>\n"
    "   • /job_edit_fields <job|index> key=value;...\n"
    "   • /job_edit_when <job|index> <booking_YYYY-MM-DD> <exec_YYYY-MM-DD> <HH:MM[:SS]>\n"
    "   • /job_update_cookies <job|index> _ga=...;_ga_TMVP85FKW9=...;ci_session=...\n\n"

    "🔎 <b>Lookup Booking</b>\n"
    "   • /booking_detail <KODE_BOOKING> [filter]\n"
    "     └─ tampilkan ketua + semua anggota (opsional filter nama/NIK/HP)\n\n"

    "💡 <b>Tips</b>\n"
    "   • ID Provinsi bisa pakai kode atau nama (mis. 35 atau Jawa Timur)\n"
    "   • Gunakan /kab untuk lihat daftar kab/kota dari provinsi\n"
    "   • Format tanggal fleksibel (YYYY-MM-DD, DD-MM-YYYY, atau '30 September 2025')\n"
    "   • Lihat contoh lengkap isi form: /examples\n"
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
    await update.message.reply_text(
        "Halo! Bot siap.\n\n" + HELP_TEXT,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        HELP_TEXT,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


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
    return f"{prefix}-{uid}-{slug}-{booking_iso}-{exec_iso}-{hhmm.replace(':', '')}"


def parse_hhmmss(s: str) -> tuple[int, int, int]:
    if not re.fullmatch(r"\d{2}:\d{2}(:\d{2})?", s):
        raise ValueError("Jam harus HH:MM atau HH:MM:SS")
    parts = s.split(":")
    hh, mm = int(parts[0]), int(parts[1])
    ss = int(parts[2]) if len(parts) == 3 else 0
    if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59):
        raise ValueError("Jam di luar rentang 00:00[:00]..23:59[:59]")
    return hh, mm, ss


# Simpan index -> job_name per user agar callback_data pendek
def _ensure_job_index(context: ContextTypes.DEFAULT_TYPE, uid: str, jobs_store: dict) -> dict[int, str]:
    idxmap_all = context.bot_data.setdefault("jobs_index", {})
    idxmap = {}
    # urutkan konsisten
    for i, name in enumerate(sorted(jobs_store.keys()), start=1):
        idxmap[i] = name
    idxmap_all[uid] = idxmap
    return idxmap


def _get_job_name_by_idx(context: ContextTypes.DEFAULT_TYPE, uid: str, idx: int) -> str | None:
    idxmap_all = context.bot_data.get("jobs_index", {})
    idxmap = idxmap_all.get(uid) or {}
    return idxmap.get(idx)


# ---------- BROMO ----------
async def book_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if not get_ci(uid):
        await update.message.reply_text("Set dulu /set_session <ci_session> (global).")
        return ConversationHandler.END
    if not context.args:
        await update.message.reply_text("Format: /book <tgl_booking>")
        return ConversationHandler.END
    try:
        iso = parse_date_indo_to_iso(" ".join(context.args))
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
    cookie_hint = ", ".join(
        [f"{k}={'(ada)' if cookies.get(k) else '(kosong)'}" for k in ["_ga", "_ga_TMVP85FKW9", "ci_session"]])
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
    if update.message.text.strip().lower() not in {"ya", "y", "yes"}:
        await update.message.reply_text("Dibatalkan.")
        return ConversationHandler.END
    uid = str(update.effective_user.id)
    ok, msg, elapsed_s, raw = do_booking_flow_bromo(
        get_ci(uid), context.user_data["booking_iso"], context.user_data["profile"], context.user_data.get("cookies")
    )
    extra = ""
    if raw:
        extra = f"\n\n[Server]\nmessage: {raw.get('message', '-')}\nlink: {raw.get('booking_link') or raw.get('link_redirect') or '-'}"
    await update.message.reply_text(
        ("✅ " if ok else "❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
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
        exec_iso = parse_date_indo_to_iso(context.args[1])
    except Exception as e:
        await update.message.reply_text(f"Format tanggal salah: {e}")
        return ConversationHandler.END

    hhmm = context.args[2]
    try:
        parse_hhmmss(hhmm)
    except Exception as e:
        await update.message.reply_text(str(e));
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data["booking_iso"] = booking_iso
    context.user_data["exec_iso"] = exec_iso
    context.user_data["time"] = hhmm
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

    cookie_hint = ", ".join(
        [f"{k}={'(ada)' if cookies.get(k) else '(kosong)'}" for k in ["_ga", "_ga_TMVP85FKW9", "ci_session"]])
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
    data = context.job.data or {}
    uid = str(data["user_id"])
    site = data["site"]
    iso = data["iso"]
    prof = data["profile"]
    job_cookies = data.get("cookies") or {}
    chat_id = context.job.chat_id

    # --- DETEKSI INTERVAL AKTUAL DARI JOB (PTB v21: timedelta) ---
    actual_interval = 60
    try:
        if getattr(context.job, "interval", None):
            iv = context.job.interval  # timedelta
            actual_interval = int(iv.total_seconds())
    except Exception:
        pass

    # --- PRIORITAS: pakai nilai yang dikirim lewat data, fallback ke interval aktual, lalu default ---
    interval_seconds = int(data.get("interval_seconds") or actual_interval or 60)

    # Kompatibilitas lama: kalau ada "notify_every" (dalam menit), konversi ke ticks
    if "notify_every_ticks" in data:
        notify_every_ticks = max(1, int(data["notify_every_ticks"]))
    elif "notify_every" in data:
        # menit -> tick
        notify_every_ticks = max(1, int((int(data["notify_every"]) * 60) / interval_seconds))
    else:
        # default: kirim tiap 5 menit
        notify_every_ticks = max(1, int(300 / interval_seconds))

    # Batas durasi
    if "max_ticks" in data:
        max_ticks = int(data["max_ticks"])
    else:
        max_minutes = int(data.get("max_minutes", 180))
        max_ticks = int((max_minutes * 60) / interval_seconds)

    # Counter tick
    data["ticks"] = data.get("ticks", 0) + 1

    ci = get_ci(uid)  # fallback global
    cap = check_capacity(iso, site)

    # Belum ada kuota
    if (not cap) or (cap["quota"] <= 0):
        # kirim status sesuai jadwal notifikasi
        if data["ticks"] % notify_every_ticks == 1:
            status = (f"{iso}: tanggal tidak ditemukan"
                      if not cap else f"{cap['tanggal_cell']}\nKuota: {cap['quota']} → {cap['status']}")
            await context.bot.send_message(
                chat_id,
                text=f"[Polling {site}] {status} (percobaan {data['ticks']}, interval {interval_seconds}s)"
            )

        # Stop bila mencapai batas tick
        if data["ticks"] >= max_ticks:
            total_minutes = int((data["ticks"] * interval_seconds) / 60)
            await context.bot.send_message(
                chat_id,
                text=f"[Polling {site}] Dihentikan setelah ~{total_minutes} menit / {data['ticks']} percobaan. "
                     f"Gunakan /job_edit_time untuk menjadwalkan ulang."
            )
            context.job.schedule_removal()
        return

    # Kuota ada → eksekusi booking dan hentikan polling
    await context.bot.send_message(chat_id,
                                   text=f"[Polling {site}] Kuota tersedia: {cap['quota']} — eksekusi booking sekarang.")
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
        text=("[Polling] ✅ " if ok else "[Polling] ❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    context.job.schedule_removal()


async def prewarm_session_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    job_name = data.get("job_name")
    ci = data.get("ci_session", "")
    cookies = data.get("cookies") or {}
    sess = make_session_with_cookies(ci, cookies)
    prewarm_session(sess, BASE)
    PREWARMED_SESSIONS[job_name] = sess


async def poll_get_view_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    job_name = data.get("job_name")
    end_at = data.get("end_at")
    site = data.get("site")
    iso = data.get("iso")
    prof = data.get("profile") or {}
    cookies = data.get("cookies") or {}
    uid = str(data.get("user_id"))
    chat_id = data.get("chat_id") or context.job.chat_id

    sess = PREWARMED_SESSIONS.get(job_name)
    if not sess:
        ci = get_ci(uid)
        sess = make_session_with_cookies(ci, cookies)
        PREWARMED_SESSIONS[job_name] = sess

    resp, _, _ = timed_request(sess, "GET", CAP_URL, timeout=10)
    html = resp.text
    last_html = data.get("last_html")

    if last_html and html != last_html:
        jq = require_jq(context)
        for j in jq.get_jobs_by_name(job_name):
            j.schedule_removal()
        for j in jq.get_jobs_by_name(f"rem-{job_name}"):
            j.schedule_removal()
        context.job.schedule_removal()

        ci = get_ci(uid)
        def attempt():
            if site == "bromo":
                return do_booking_flow_bromo(ci, iso, prof, job_cookies=cookies, sess=sess)
            else:
                leader = prof.get("_leader", {})
                members = prof.get("_members", [])
                return do_booking_flow_semeru(ci, iso, leader, members, job_cookies=cookies, sess=sess)

        ok, msg, elapsed_s, raw = short_window_aggressive(attempt, attempts=3)
        extra = ""
        if raw:
            server_msg = raw.get("message", "-")
            link = raw.get("booking_link") or raw.get("link_redirect") or "-"
            extra = f"\n[Server]\nmessage: {server_msg}\nlink: {link}"
        await context.bot.send_message(
            chat_id,
            text=("[Watch] ✅ " if ok else "[Watch] ❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        PREWARMED_SESSIONS.pop(job_name, None)
        return

    data["last_html"] = html
    context.job.interval = timedelta(seconds=random.uniform(3, 7))
    if end_at and datetime.now(ASIA_JAKARTA) > end_at:
        context.job.schedule_removal()

async def scheduled_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    uid = str(data["user_id"])
    site = data["site"]  # 'bromo' | 'semeru'
    iso = data["iso"]
    prof = data["profile"]
    job_cookies = data.get("cookies") or {}
    chat_id = context.job.chat_id

    ci = get_ci(uid)  # global fallback
    if not ci and not job_cookies.get("ci_session"):
        await context.bot.send_message(chat_id,
                                       text=f"[Jadwal {site}] ci_session kosong/expired. /set_session atau /job_update_cookies dulu.")
        return

    jq = require_jq(context)
    job_name = context.job.name or f"{site}-{uid}-{iso}"
    for j in jq.get_jobs_by_name(f"view-{job_name}"):
        j.schedule_removal()

    # ✅ cek kapasitas saat eksekusi
    cap = check_capacity(iso, site)
    if not cap or cap["quota"] <= 0:
        # info kondisi saat ini
        if not cap:
            await context.bot.send_message(chat_id, text=f"[Jadwal {site}] {iso}: tanggal tidak ditemukan.")
        else:
            await context.bot.send_message(chat_id,
                                           text=f"[Jadwal {site}] {cap['tanggal_cell']}\nKuota: {cap['quota']} → {cap['status']}")

        # aktifkan polling per menit
        poll_name = f"poll-{job_name}"
        for j in jq.get_jobs_by_name(poll_name):
            j.schedule_removal()  # pastikan tidak dobel
        jq.run_repeating(
            poll_capacity_job,
            interval=60,
            first=60,
            name=poll_name,
            data={
                "user_id": uid,
                "site": site,
                "iso": iso,
                "profile": prof,
                "cookies": job_cookies,

                # Gunakan kunci yang benar:
                "interval_seconds": 60,  # sinkron dgn interval run_repeating
                "notify_every_ticks": 5,  # tiap 5 tick = 5 menit karena interval 60s
                "max_minutes": 180  # hard stop 3 jam
                # (opsional) kalau tetap mau gaya lama:
                # "notify_every": 5,          # menit (handler akan konversi ke ticks)
                # "max_ticks": 180            # kalau kamu ingin batas tick absolut
            },
            chat_id=chat_id
        )

        await context.bot.send_message(chat_id, text=f"[Jadwal {site}] Polling per menit diaktifkan (max 3 jam).")
        return

    # kalau kuota tersedia langsung eksekusi seperti biasa
    await context.bot.send_message(chat_id,
                                   text=f"[Jadwal {site}] {cap['tanggal_cell']}\nKuota: {cap['quota']} → {cap['status']}")
    sess = PREWARMED_SESSIONS.pop(job_name, None)
    if site == "bromo":
        ok, msg, elapsed_s, raw = do_booking_flow_bromo(ci, iso, prof, job_cookies=job_cookies, sess=sess)
    else:
        leader = prof.get("_leader", {})
        members = prof.get("_members", [])
        ok, msg, elapsed_s, raw = do_booking_flow_semeru(ci, iso, leader, members, job_cookies=job_cookies, sess=sess)

    extra = ""
    if raw:
        server_msg = raw.get("message", "-")
        link = raw.get("booking_link") or raw.get("link_redirect") or "-"
        extra = f"\n[Server]\nmessage: {server_msg}\nlink: {link}"
    await context.bot.send_message(
        chat_id,
        text=("[Jadwal] ✅ " if ok else "[Jadwal] ❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    uid = str(data["user_id"]);
    job_name = data["job_name"]
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
        return names[idx - 1] if 1 <= idx <= len(names) else None
    return selector if selector in jobs_store else None


def _fmt_len(s: str, n: int) -> str:
    s = str(s)
    return s[:n].ljust(n)


def _detect_site(job_name: str) -> str:
    return "SEMERU" if job_name.startswith("semeru-") else "BROMO"


def _exec_dt_str(rec: dict) -> str:
    # format singkat eksekusi: YYYY-MM-DD HH:MM
    t = (rec.get("time") or "00:00")[:5]
    return f"{rec.get('exec_iso', '????-??-??')} {t}"


def _participants(rec: dict) -> int:
    prof = rec.get("profile", {})
    # Bromo: 1 leader + male + female
    if "name" in prof:
        try:
            m = int(prof.get("male", "0") or 0)
            f = int(prof.get("female", "0") or 0)
            return 1 + m + f
        except Exception:
            return 1
    # Semeru: leader + jumlah anggota
    mem = prof.get("_members", [])
    return 1 + (len(mem) if isinstance(mem, list) else 0)


def _cookies_badge(rec: dict) -> str:
    ck = rec.get("cookies", {}) or {}
    marks = []
    marks.append("🔐" if ck.get("ci_session") else "⚠️")
    if ck.get("_ga") or ck.get("_ga_TMVP85FKW9"):
        marks.append("🍪")
    return "".join(marks)


def _status_badge(name: str, live: set[str]) -> str:
    return "🟢" if name in live else "⚪"


def _sort_key(item: tuple[str, dict]) -> tuple[str, str]:
    name, rec = item
    return (rec.get("exec_iso", "9999-99-99"), rec.get("time", "99:99:99"))


def _render_jobs_table(jobs_store: dict, live: set[str]) -> list[str]:
    if not jobs_store:
        return ["Belum ada job terjadwal."]

    header = (
            _fmt_len("#", 3) + " " +
            _fmt_len("ST", 2) + " " +
            _fmt_len("SITE", 6) + " " +
            _fmt_len("BOOKING", 10) + " " +
            _fmt_len("EKSEKUSI", 16) + " " +
            _fmt_len("LEADER", 16) + " " +
            _fmt_len("PAX", 3) + " " +
            "COOK " +
            "JOB"
    )
    sep = "—" * len(header)

    items = sorted(jobs_store.items(), key=_sort_key)

    lines = [header, sep]
    for idx, (name, rec) in enumerate(items, start=1):
        leader = (rec.get("profile", {}).get("name")
                  or rec.get("profile", {}).get("_leader", {}).get("name")
                  or "-")
        row = (
                _fmt_len(idx, 3) + " " +
                _fmt_len(_status_badge(name, live), 2) + " " +
                _fmt_len(_detect_site(name), 6) + " " +
                _fmt_len(rec.get("booking_iso", "-"), 10) + " " +
                _fmt_len(_exec_dt_str(rec), 16) + " " +
                _fmt_len(leader, 16) + " " +
                _fmt_len(_participants(rec), 3) + " " +
                _fmt_len(_cookies_badge(rec), 4) + " " +
                name
        )
        lines.append(row)

    body = "\n".join(lines)
    # pecah pesan kalau > 3900 char
    chunks, cur, limit = [], "", 3900
    for line in body.splitlines():
        add = (("\n" if cur else "") + line)
        if len(cur) + len(add) > limit:
            chunks.append(cur)
            cur = line
        else:
            cur += add
    if cur:
        chunks.append(cur)

    return [f"<pre>{c}</pre>" for c in chunks]


async def jobs_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    jobs_store = get_jobs_store(uid)
    live = jobs_live_names(context)

    parts = _render_jobs_table(jobs_store, live)
    title = f"📋 Daftar Job ({len(jobs_store)})"
    await update.message.reply_text(title)
    for chunk in parts:
        await update.message.reply_text(chunk, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    if not jobs_store:
        return

    # Buat index mapping untuk callback (idx -> job_name)
    idxmap = _ensure_job_index(context, uid, jobs_store)

    # Kirim daftar ringkas + tombol (batasi 20 agar tidak spam)
    MAX_ROWS = 20
    rows = sorted(jobs_store.items(), key=_sort_key)[:MAX_ROWS]
    for i, (name, rec) in enumerate(rows, start=1):
        leader = (rec.get("profile", {}).get("name")
                  or rec.get("profile", {}).get("_leader", {}).get("name")
                  or "-")
        text = f"{i}. {name}\n• Leader: {leader}\n• Eksekusi: {_exec_dt_str(rec)}"

        kb = [
            [
                InlineKeyboardButton("ℹ️ Detail", callback_data=f"job:detail:{i}"),
                InlineKeyboardButton("✏️ Edit", callback_data=f"job:edit:{i}"),
                InlineKeyboardButton("🗑️ Cancel", callback_data=f"job:cancel:{i}"),
            ]
        ]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb))


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
    safe_ck = {k: mask(ck.get(k)) for k in ["_ga", "_ga_TMVP85FKW9", "ci_session"]}

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
        for j in jq.get_jobs_by_name(f"prewarm-{job_name}"): j.schedule_removal()
        for j in jq.get_jobs_by_name(f"view-{job_name}"): j.schedule_removal()
    except RuntimeError:
        pass
    PREWARMED_SESSIONS.pop(job_name, None)
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
    for j in jq.get_jobs_by_name(f"prewarm-{old_name}"): j.schedule_removal()
    for j in jq.get_jobs_by_name(f"view-{old_name}"): j.schedule_removal()
    PREWARMED_SESSIONS.pop(old_name, None)

    leader_name = profile.get("name") or profile.get("_leader", {}).get("name", "ketua")
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

    pre_at = run_at - timedelta(minutes=2)
    jq.run_once(prewarm_session_job, when=pre_at, name=f"prewarm-{new_name}",
                data={"job_name": new_name, "ci_session": get_ci(uid), "cookies": cookies},
                chat_id=chat_id)
    poll_start = run_at - timedelta(minutes=5)
    poll_end = run_at + timedelta(minutes=15)
    jq.run_repeating(poll_get_view_job, interval=timedelta(seconds=5), first=poll_start,
                     name=f"view-{new_name}",
                     data={"job_name": new_name, "user_id": uid, "site": site,
                           "iso": booking_iso, "profile": profile, "cookies": cookies,
                           "end_at": poll_end, "chat_id": chat_id},
                     chat_id=chat_id)
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
        await update.message.reply_text("Job tidak ditemukan.");
        return
    jobs = get_jobs_store(uid);
    rec = jobs.get(job_name)
    if not rec: await update.message.reply_text("Job tidak ditemukan."); return

    site = "semeru" if job_name.startswith("semeru-") else "bromo"
    try:
        new_name = await reschedule_job(context, uid, job_name, rec["booking_iso"], exec_iso, hhmm,
                                        rec["profile"], rec.get("cookies", {}), rec.get("reminder_minutes"),
                                        rec.get("chat_id", update.effective_chat.id), site)
    except Exception as e:
        await update.message.reply_text(f"Gagal menjadwalkan ulang: {e}");
        return
    jobs.pop(job_name, None);
    rec["exec_iso"] = exec_iso;
    rec["time"] = hhmm;
    jobs[new_name] = rec;
    save_storage(storage)
    await update.message.reply_text(f"Job diubah waktunya ✅\nLama: {job_name}\nBaru: {new_name}")


async def job_update_cookies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if len(context.args) < 2:
        await update.message.reply_text(
            "Format: /job_update_cookies <job|index> _ga=...;_ga_TMVP85FKW9=...;ci_session=...")
        return
    selector = context.args[0];
    kv = parse_kv_pairs(" ".join(context.args[1:]))
    job_name = resolve_job_selector(uid, selector)
    if not job_name: await update.message.reply_text("Job tidak ditemukan."); return
    jobs = get_jobs_store(uid);
    rec = jobs.get(job_name)
    if not rec: await update.message.reply_text("Job tidak ditemukan."); return

    cookies = rec.get("cookies", {}).copy();
    changed = []
    for k in ["_ga", "_ga_TMVP85FKW9", "ci_session"]:
        if k in kv and kv[k]:
            cookies[k] = kv[k];
            changed.append(k)
    if not changed: await update.message.reply_text("Tidak ada cookie yang diubah."); return

    site = "semeru" if job_name.startswith("semeru-") else "bromo"
    try:
        new_name = await reschedule_job(context, uid, job_name, rec["booking_iso"], rec["exec_iso"], rec["time"],
                                        rec["profile"], cookies, rec.get("reminder_minutes"),
                                        rec.get("chat_id", update.effective_chat.id), site)
    except Exception as e:
        await update.message.reply_text(f"Gagal menjadwalkan ulang: {e}");
        return

    jobs.pop(job_name, None);
    rec["cookies"] = cookies;
    jobs[new_name] = rec;
    save_storage(storage)
    await update.message.reply_text(
        f"Cookies job diupdate ✅ ({', '.join(changed)})\nLama: {job_name}\nBaru: {new_name}")


# ---------- SEMERU booking/schedule ----------
BOOK_PREFIX_BROMO = "bromo"
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
    cookie_hint = ", ".join(
        [f"{k}={'(ada)' if cookies.get(k) else '(kosong)'}" for k in ["_ga", "_ga_TMVP85FKW9", "ci_session"]])
    remind_txt = f"{reminder_minutes} menit" if reminder_minutes is not None else "tidak"
    member_txt = "(tidak ada)" if not members else (", ".join([m.get('nama', '?') for m in members]))
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
    if update.message.text.strip().lower() not in {"ya", "y", "yes"}:
        await update.message.reply_text("Dibatalkan.")
        return ConversationHandler.END
    uid = str(update.effective_user.id)
    ok, msg, elapsed_s, raw = do_booking_flow_semeru(
        get_ci(uid), context.user_data["booking_iso"], context.user_data["_leader"], context.user_data["_members"],
        job_cookies=context.user_data.get("cookies")
    )
    extra = ""
    if raw:
        extra = f"\n\n[Server]\nmessage: {raw.get('message', '-')}\nlink: {raw.get('booking_link') or raw.get('link_redirect') or '-'}"
    await update.message.reply_text(
        ("✅ " if ok else "❌ ") + msg + f"\n\nWaktu proses: {elapsed_s:.2f} detik" + extra,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
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
        exec_iso = parse_date_indo_to_iso(context.args[1])
    except Exception as e:
        await update.message.reply_text(f"Format tanggal salah: {e}")
        return ConversationHandler.END
    hhmm = context.args[2]
    try:
        parse_hhmmss(hhmm)
    except Exception as e:
        await update.message.reply_text(str(e));
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data["booking_iso"] = booking_iso
    context.user_data["exec_iso"] = exec_iso
    context.user_data["time"] = hhmm
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

    cookie_hint = ", ".join(
        [f"{k}={'(ada)' if cookies.get(k) else '(kosong)'}" for k in ["_ga", "_ga_TMVP85FKW9", "ci_session"]])
    remind_txt = f"{reminder_minutes} menit" if reminder_minutes is not None else "tidak"
    member_txt = "(tidak ada)" if not members else (", ".join([m.get('nama', '?') for m in members]))
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
    if update.message.text.strip().lower() not in {"ya", "y", "yes"}:
        await update.message.reply_text("Dibatalkan.")
        return ConversationHandler.END
    try:
        jq = require_job_queue(context)
    except RuntimeError as e:
        await update.message.reply_text(str(e));
        return ConversationHandler.END

    booking_iso = context.user_data["booking_iso"]
    exec_iso = context.user_data["exec_iso"]
    hh, mm, ss = parse_hhmmss(context.user_data["time"])
    y, M, d = map(int, exec_iso.split("-"))
    run_at = ASIA_JAKARTA.localize(datetime(y, M, d, hh, mm, ss))
    if run_at < datetime.now(ASIA_JAKARTA):
        await update.message.reply_text("Waktu eksekusi sudah lewat.");
        return ConversationHandler.END

    uid = str(update.effective_user.id)
    leader_name = context.user_data["_leader"].get("name", "ketua")
    job_name = make_job_name(BOOK_PREFIX_SEMERU, uid, leader_name, booking_iso, exec_iso, context.user_data["time"])
    for j in jq.get_jobs_by_name(job_name): j.schedule_removal()
    for j in jq.get_jobs_by_name(f"prewarm-{job_name}"): j.schedule_removal()
    for j in jq.get_jobs_by_name(f"view-{job_name}"): j.schedule_removal()
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

    pre_at = run_at - timedelta(minutes=2)
    jq.run_once(prewarm_session_job, when=pre_at, name=f"prewarm-{job_name}",
                data={"job_name": job_name, "ci_session": get_ci(uid),
                      "cookies": jobs_store[job_name]["cookies"]},
                chat_id=update.effective_chat.id)
    poll_start = run_at - timedelta(minutes=5)
    poll_end = run_at + timedelta(minutes=15)
    jq.run_repeating(poll_get_view_job, interval=timedelta(seconds=5), first=poll_start,
                     name=f"view-{job_name}",
                     data={"job_name": job_name, "user_id": uid, "site": "semeru",
                           "iso": booking_iso, "profile": jobs_store[job_name]["profile"],
                           "cookies": jobs_store[job_name]["cookies"],
                           "end_at": poll_end, "chat_id": update.effective_chat.id},
                     chat_id=update.effective_chat.id)

    remind_min = context.user_data.get("reminder_minutes")
    if isinstance(remind_min, int) and remind_min > 0:
        remind_at = run_at - timedelta(minutes=remind_min)
        if remind_at > datetime.now(ASIA_JAKARTA):
            jq.run_once(reminder_job, when=remind_at, name=f"rem-{job_name}",
                        data={"user_id": uid, "job_name": job_name}, chat_id=update.effective_chat.id)

    await update.message.reply_text(
        f"Terjadwal ✅ (SEMERU)\n- Booking: {booking_iso}\n- Eksekusi: {exec_iso} {context.user_data['time']} (Asia/Jakarta)\n"
        f"- Reminder: {context.user_data.get('reminder_minutes', 'tidak')} menit sebelum\n"
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

    # gunakan ci_session global user, kalau ada
    uid = str(update.effective_user.id)
    ci = get_ci(uid)

    pairs = fetch_districts_by_province(code, ci_session=ci)
    if not pairs:
        await update.message.reply_text(f"Tidak ada data kab/kota untuk {canon or q} ({code}).")
        return

    msg = format_districts_message(code, canon or "Provinsi", pairs)
    for part in split_long_message(msg):
        await update.message.reply_text(part)


def _mask_cookie(s: str | None) -> str:
    if not s: return "(kosong)"
    return s[:6] + "..." + s[-4:]


async def on_jobs_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)

    try:
        _, action, idx_str = (q.data or "").split(":", 2)
        idx = int(idx_str)
    except Exception:
        await q.edit_message_text("Callback tidak valid.")
        return

    name = _get_job_name_by_idx(context, uid, idx)
    if not name:
        await q.edit_message_text("Job tidak ditemukan (index kedaluwarsa). Jalankan /jobs lagi.")
        return

    jobs = get_jobs_store(uid)
    rec = jobs.get(name)
    if not rec:
        await q.edit_message_text("Job tidak ditemukan.")
        return

    if action == "detail":
        live = name in jobs_live_names(context)
        ck = rec.get("cookies", {})
        site = "SEMERU" if name.startswith("semeru-") else "BROMO"
        leader = (rec.get("profile", {}).get("name")
                  or rec.get("profile", {}).get("_leader", {}).get("name")
                  or "-")
        msg = (
            f"🔎 <b>Detail Job</b>\n"
            f"Nama   : <code>{name}</code>\n"
            f"Status : {'AKTIF 🟢' if live else 'TIDAK AKTIF ⚪'}\n"
            f"Site   : {site}\n"
            f"Booking: {rec.get('booking_iso', '-')}\n"
            f"Eksekusi: {_exec_dt_str(rec)} (Asia/Jakarta)\n"
            f"Leader : {leader}\n"
            f"Cookies:\n"
            f"  • _ga: {_mask_cookie(ck.get('_ga'))}\n"
            f"  • _ga_TMVP85FKW9: {_mask_cookie(ck.get('_ga_TMVP85FKW9'))}\n"
            f"  • ci_session: {_mask_cookie(ck.get('ci_session'))}\n"
        )
        await q.edit_message_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    elif action == "cancel":
        # batalkan semua job dengan nama ini (utama, reminder, polling)
        try:
            jq = require_jq(context)
            for j in jq.get_jobs_by_name(name): j.schedule_removal()
            for j in jq.get_jobs_by_name(f"rem-{name}"): j.schedule_removal()
            for j in jq.get_jobs_by_name(f"poll-{name}"): j.schedule_removal()
            for j in jq.get_jobs_by_name(f"prewarm-{name}"): j.schedule_removal()
            for j in jq.get_jobs_by_name(f"view-{name}"): j.schedule_removal()
        except RuntimeError:
            pass
        jobs.pop(name, None)
        save_storage(storage)
        PREWARMED_SESSIONS.pop(name, None)
        await q.edit_message_text(f"✅ Job <code>{name}</code> dibatalkan & dihapus.", parse_mode=ParseMode.HTML)

    elif action == "edit":
        # Sederhana: beri template command untuk diedit user
        tmpl1 = f"/job_edit_time {idx} {rec.get('exec_iso', 'YYYY-MM-DD')} {rec.get('time', 'HH:MM')}"
        tmpl2 = f"/job_update_cookies {idx} _ga=...;_ga_TMVP85FKW9=...;ci_session=..."
        msg = (
            "✏️ <b>Edit Job</b>\n"
            "Gunakan perintah berikut (salin & sesuaikan):\n\n"
            f"• Ubah waktu eksekusi:\n<code>{tmpl1}</code>\n\n"
            f"• Update cookies:\n<code>{tmpl2}</code>\n"
            "\nCatatan: index pada perintah merujuk ke urutan terakhir di /jobs."
        )
        await q.edit_message_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    else:
        await q.edit_message_text("Aksi tidak dikenali.")


def _mask(s: str | None, head: int = 6, tail: int = 4) -> str:
    if not s: return "-"
    if len(s) <= head + tail: return s
    return s[:head] + "…" + s[-tail:]


async def booking_detail_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /booking_detail <KODE_BOOKING> [filter]
    Ambil detail ketua (grid member) + semua anggota (website grid via secret).
    """
    uid = str(update.effective_user.id)
    ci = get_ci(uid)
    if not ci:
        await update.message.reply_text("Set dulu /set_session <ci_session>.")
        return

    if not context.args:
        await update.message.reply_text("Format: /booking_detail <KODE_BOOKING> [filter]")
        return

    booking_code = context.args[0].strip()
    filter_q = " ".join(context.args[1:]).strip() if len(context.args) > 1 else ""

    try:
        row = get_booking_by_code_api(booking_code, ci)
    except Exception as e:
        await update.message.reply_text(f"Gagal ambil booking: {e}")
        return

    leader = get_leader_from_row(row)
    secret = leader.get("secret") or row.get("secret")
    if not secret:
        await update.message.reply_text("Field 'secret' tidak ditemukan di grid.")
        return

    try:
        members, total = get_members_by_secret(secret, ci_session=ci, page_size=200, search_value=filter_q)
    except Exception as e:
        await update.message.reply_text(f"Gagal ambil anggota: {e}")
        return

    head = (
        f"📦 <b>Booking</b> <code>{leader.get('code', '-')}</code>\n"
        f"Status: {leader.get('booking_status', '-')} | Tgl: {leader.get('date_depart', '-')} → {leader.get('date_arrival', '-')}\n"
        f"Total pendaki (server): {leader.get('total_pendaki', '-')}\n"
        f"Secret: <code>{_mask(secret, 8, 6)}</code>\n"
        "\n👤 <b>Ketua</b>\n"
        f"• Nama: {leader.get('leader_name', '-')}\n"
        f"• NIK: {_mask(leader.get('leader_identity_no'))}\n"
        f"• HP: {_mask(leader.get('leader_hp'))}\n"
        f"• Lahir: {leader.get('leader_birthdate', '-')}\n"
        f"• Email: {leader.get('email', '-')} | Country: {leader.get('country', '-')}\n"
        f"• Alamat: {leader.get('leader_address', '-')}\n"
    )

    lines = [head, "\n👥 <b>Anggota</b> (" + str(total) + (f", filter='{filter_q}'" if filter_q else "") + ")\n"]
    if not members:
        lines.append("(kosong)")
    else:
        for i, m in enumerate(members, start=1):
            nama = m.get("nama") or "-"
            nik = m.get("identity_no") or "-"
            hp = m.get("hp_member") or "-"
            bday = m.get("birthdate") or "-"
            lines.append(f"{i}. {nama} — NIK:{_mask(nik)} — HP:{_mask(hp)} — Lahir:{bday}")

    # pecah jika kepanjangan
    msg = "\n".join(lines)

    def split_long_message(msg: str, limit: int = 3900) -> list[str]:
        if len(msg) <= limit:
            return [msg]
        parts, cur = [], ""
        for line in msg.splitlines():
            add = (("\n" if cur else "") + line)
            if len(cur) + len(add) > limit:
                parts.append(cur);
                cur = line
            else:
                cur += add
        if cur: parts.append(cur)
        return parts

    for part in split_long_message(msg):
        await update.message.reply_text(part, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


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
    app.add_handler(CallbackQueryHandler(on_jobs_callback, pattern=r"^job:"))

    # Lookup provinsi & kabupaten/kota
    app.add_handler(CommandHandler(["prov", "provinsi"], prov_lookup_cmd))
    app.add_handler(CommandHandler(["kab", "kabupaten"], kabupaten_cmd))

    # Lookup booking code
    # NEW: lookup booking detail
    app.add_handler(CommandHandler("booking_detail", booking_detail_cmd))

    # BROMO
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("book", book_entry)],
        states={
            BOOK_ASK_FORM: [MessageHandler(filters.TEXT & ~filters.COMMAND, book_collect_form)],
            BOOK_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, book_confirm)],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: u.message.reply_text("Dibatalkan."))],
        name="bromo_book", persistent=False
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("schedule", schedule_entry)],
        states={
            SCHED_ASK_FORM: [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_collect_form)],
            SCHED_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u, c: u.message.reply_text(
                "Gunakan flow yang sudah ada (tidak dipakai di kode ini)."))],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: u.message.reply_text("Dibatalkan."))],
        name="bromo_sched", persistent=False
    ))

    # SEMERU
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("book_semeru", book_semeru_entry)],
        states={
            BOOK_ASK_FORM_SEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, book_semeru_collect_form)],
            BOOK_CONFIRM_SEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, book_semeru_confirm)],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: u.message.reply_text("Dibatalkan."))],
        name="semeru_book", persistent=False
    ))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("schedule_semeru", schedule_semeru_entry)],
        states={
            SCHED_ASK_FORM_SEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_semeru_collect_form)],
            SCHED_CONFIRM_SEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_semeru_confirm)],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: u.message.reply_text("Dibatalkan."))],
        name="semeru_sched", persistent=False
    ))

    app.add_handler(CommandHandler("quota_semeru", quota_semeru_cmd))
    # Contoh format
    app.add_handler(CommandHandler("examples", examples_cmd))

    app.add_error_handler(on_error)
    app.run_polling()


if __name__ == "__main__":
    main()
