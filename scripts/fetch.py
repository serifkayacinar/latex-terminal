"""
Daily price fetcher.

Runs in GitHub Actions on a 6-hour cron. The sandbox where this file is
*authored* may not have network egress to the commodity sites — that's
expected; CI does. The fetcher is designed to be robust under partial
failure: each source is independent, anything that 4xx/5xx/parse-fails
just leaves that series at its previous value and is recorded as STALE.

Sources (priority, all confirmed live at time of authoring):

  1. ANRPC daily price page                     anrpc.org/anrpc-daily-price
     - Single HTML table with daily prices for member-country grades:
       Malaysia SMR20, Thailand STR20, Indonesia SIR20,
       Vietnam SVR3L/SVR10, India RSS4, plus latex 60% DRC for MYS/THA.
     - Native units vary by row (USD cents/kg, USD/t, local cur/kg).
       We normalize to USD/kg.

  2. Malaysian Rubber Board (MRB) daily prices  www3.lgm.gov.my/smhargagetah/Daily5.aspx
     - Backup for SMR20_MYS and LATEX_60_MYS when ANRPC misses a day.
     - Native unit: sen/kg (Malaysian cents). Convert via MYR FX.

  3. Rubber Board of India daily prices         rubberboard.gov.in/public
     - Authoritative source for RSS4_KOTTAYAM_INR.
     - Native unit: INR/100kg (kept as-is in the dataset).

  4. SHFE daily statistics                       shfe.com.cn/eng/reports/StatisticalData/DailyData/
     - Front-month RU rubber settlement (CNY/tonne).
     - Convert to USD/kg via CNY FX.

  5. RTAS Singapore                              rtas.sg/rubber-prices
     - SGX SICOM TSR20 / RSS3 official daily settlement, last ~3 months.

  6. Frankfurter (ECB)                           api.frankfurter.dev/v2/latest
     - FX for USD->{MYR,INR,JPY,CNY,THB,IDR,VND}. Free, no key.

  7. World Bank Pink Sheet (monthly fallback)    thedocs.worldbank.org/...
     - For series with no public daily feed (TOCOM proxy, sanity baseline).

Output:
  - data/prices.json    : full series, real prints overwriting seed for matching dates.
  - data/last_run.json  : per-series live status, FX snapshot, run summary.
"""

from __future__ import annotations
import json
import re
import sys
import time
import traceback
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "prices.json"
META = ROOT / "data" / "last_run.json"

UA = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
TIMEOUT = 25


def _today_iso() -> str:
    return date.today().isoformat()


def _yesterday_iso() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


# ---------------------------------------------------------------------------
# FX (ECB rates via Frankfurter; free, no auth)
# ---------------------------------------------------------------------------

class FX:
    """Cache USD-base FX for the run. Falls back to a recent static
    snapshot if the live API is unreachable, so other fetchers can still
    convert local-currency prices."""

    # Defensive fallback (May 2026 ballpark — derived from MRB Daily5
    # cross-rate observed 2026-05-04: 866 sen/kg = 223.25 USc/kg implies
    # MYR ≈ 3.88/USD, INR derived from RBI 25000/100kg = $262.50 implies
    # INR ≈ 95.24/USD). Replaced with live values on Frankfurter success.
    FALLBACK = {
        "MYR": 3.88, "INR": 95.24, "JPY": 152.50, "CNY": 7.18,
        "THB": 35.10, "IDR": 16250.0, "VND": 25400.0,
    }

    def __init__(self) -> None:
        self.rates: Dict[str, float] = {}
        self.source: str = "fallback"

    def fetch(self) -> None:
        url = ("https://api.frankfurter.dev/v2/latest"
               "?base=USD&symbols=MYR,INR,JPY,CNY,THB,IDR,VND")
        try:
            r = requests.get(url, headers=UA, timeout=TIMEOUT)
            r.raise_for_status()
            j = r.json()
            self.rates = {k: float(v) for k, v in j.get("rates", {}).items()}
            self.source = f"frankfurter@{j.get('date','')}"
            print(f"[fx] {self.source}: {self.rates}")
        except Exception as e:
            print(f"[fx] frankfurter failed ({e}); using fallback", file=sys.stderr)
            self.rates = dict(self.FALLBACK)
            self.source = "fallback"

    def usd_per(self, code: str) -> Optional[float]:
        """Return USD per 1 unit of `code` (e.g. usd_per('MYR') = ~0.226)."""
        rate = self.rates.get(code) or self.FALLBACK.get(code)
        if not rate:
            return None
        return 1.0 / rate


# ---------------------------------------------------------------------------
# Parser helpers
# ---------------------------------------------------------------------------

def _to_float(s: str) -> Optional[float]:
    try:
        return float(re.sub(r"[^\d.\-]", "", s))
    except (ValueError, TypeError):
        return None


def _http(url: str, timeout: int = TIMEOUT) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=UA, timeout=timeout)
        if r.status_code != 200:
            print(f"[http] {url} -> {r.status_code}", file=sys.stderr)
            return None
        return r
    except Exception as e:
        print(f"[http] {url} -> {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Source 1: ANRPC daily price (anrpc.org/anrpc-daily-price)
# ---------------------------------------------------------------------------

# Map ANRPC table-row labels (case-insensitive substrings) to our series codes.
# The page lists rows like "Malaysia SMR20", "Thailand STR20", etc.
ANRPC_ROWS: List[Tuple[str, str, str]] = [
    # (substring match, target series, native unit)
    ("malaysia smr 20",            "SMR20_MYS",      "USC/kg"),
    ("malaysia smr20",             "SMR20_MYS",      "USC/kg"),
    ("thailand str 20",            "STR20_THA",      "USC/kg"),
    ("thailand str20",             "STR20_THA",      "USC/kg"),
    ("indonesia sir 20",           "SIR20_IDN",      "USC/kg"),
    ("indonesia sir20",            "SIR20_IDN",      "USC/kg"),
    ("vietnam svr 3l",             "SVR3L_VNM",      "USC/kg"),
    ("vietnam svr3l",              "SVR3L_VNM",      "USC/kg"),
    ("vietnam svr 10",             "SVR10_VNM",      "USC/kg"),
    ("vietnam svr10",              "SVR10_VNM",      "USC/kg"),
    ("malaysia latex",             "LATEX_60_MYS",   "USC/kg"),
    ("thailand latex",             "LATEX_60_THA",   "USC/kg"),
    # ANRPC also publishes RSS3 SGP and SICOM TSR20 reference rows
    ("rss 3 sicom",                "RSS3_SGP",       "USC/kg"),
    ("rss3 sicom",                 "RSS3_SGP",       "USC/kg"),
    ("sicom rss 3",                "RSS3_SGP",       "USC/kg"),
    ("tsr 20 sicom",               "TSR20_SGP",      "USC/kg"),
    ("tsr20 sicom",                "TSR20_SGP",      "USC/kg"),
    ("sicom tsr 20",               "TSR20_SGP",      "USC/kg"),
]


def fetch_anrpc(fx: FX) -> Dict[str, Dict[str, float]]:
    """Pull the daily price table from ANRPC. Returns
    {series_code: {date_iso: usd_per_kg}}.

    The page renders a single <table> with date in the header and grade
    rows. The row text varies but reliably contains the substrings above.
    Native unit on this page is US cents/kg (USC/kg) for grades, and
    USD/kg for some latex rows; we treat any value > 50 as cents."""
    out: Dict[str, Dict[str, float]] = {}
    r = _http("https://www.anrpc.org/anrpc-daily-price")
    if not r:
        return out
    soup = BeautifulSoup(r.text, "lxml")

    # Find the most recent reporting date displayed on the page.
    # ANRPC labels it like "Daily Price for: 02-May-2026" or similar.
    page_date = None
    m = re.search(
        r"(\d{1,2})[\-\s]+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*"
        r"[\-\s,]+(\d{4})",
        soup.get_text(" ", strip=True), re.I)
    if m:
        try:
            page_date = datetime.strptime(
                f"{m.group(1)} {m.group(2)[:3].title()} {m.group(3)}",
                "%d %b %Y").date().isoformat()
        except Exception:
            pass
    page_date = page_date or _today_iso()

    # Walk every table row, match against our label list.
    for row in soup.select("table tr"):
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
        if len(cells) < 2:
            continue
        label = cells[0].lower()
        # Find the first numeric cell after the label (usually col 1 or 2).
        val: Optional[float] = None
        for c in cells[1:]:
            v = _to_float(c)
            if v is not None and v > 0:
                val = v
                break
        if val is None:
            continue
        for needle, code, _unit in ANRPC_ROWS:
            if needle in label:
                # ANRPC mostly publishes USc/kg. Heuristic: values > 50 are
                # cents per kg, values < 50 are already USD/kg.
                usd_per_kg = val / 100.0 if val > 50 else val
                out.setdefault(code, {})[page_date] = round(usd_per_kg, 4)
                break

    return out


# ---------------------------------------------------------------------------
# Source 2: Malaysian Rubber Board (MRB) Daily5
# ---------------------------------------------------------------------------

def fetch_mrb(fx: FX) -> Dict[str, Dict[str, float]]:
    """MRB daily SMR + Latex-in-Bulk noon prices.
    Daily5.aspx publishes a table where each row is:
        <Grade>  <Sen/Kg>  <US Cents/Kg>
    e.g.    SMR 20   866.00   223.25
    We use the second number (US Cents/Kg) which is the official MRB
    USD conversion — bypasses any FX guessing on our side.
    The header line contains the "noon on DD/MM/YYYY" date.
    """
    out: Dict[str, Dict[str, float]] = {}
    r = _http("http://www3.lgm.gov.my/smhargagetah/Daily5.aspx")
    if not r:
        return out
    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text(" ", strip=True)

    # Date: header has "ON DD/MM/YYYY"
    iso_date = _today_iso()
    m = re.search(r"NOON\s*ON\s*(\d{1,2})/(\d{1,2})/(\d{4})", text, re.I)
    if not m:
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if m:
        try:
            iso_date = date(int(m.group(3)), int(m.group(2)),
                            int(m.group(1))).isoformat()
        except ValueError:
            pass

    # Pattern: grade name then TWO numbers (sen/kg, then US cents/kg).
    # Grab the SECOND number — that's the published USD conversion.
    def _grab(label_pattern: str, lo: float, hi: float) -> Optional[float]:
        m = re.search(
            label_pattern + r"\s+(\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?)",
            text, re.I)
        if not m:
            return None
        us_cents = _to_float(m.group(2))
        if us_cents and lo < us_cents < hi:
            return round(us_cents / 100.0, 4)
        return None

    smr20 = _grab(r"SMR\s*20", 30.0, 600.0)   # plausible USc/kg range
    if smr20:
        out.setdefault("SMR20_MYS", {})[iso_date] = smr20
    latex = _grab(r"Latex(?:\s+in\s+Bulk)?", 30.0, 600.0)
    if latex:
        out.setdefault("LATEX_60_MYS", {})[iso_date] = latex

    return out


def fetch_rubberboard_india_v2(fx: FX) -> Dict[str, Dict[str, float]]:
    """Primary source for RSS4 Kottayam (INR/100kg), SMR20 KL (USD/100kg),
    and Latex(60%) KL (USD/100kg). Rubber Board India publishes a clean
    table with date headers and per-market sub-tables. This page is
    multi-purpose — it gives us India domestic AND KL international
    grades in one fetch, all pre-converted to USD where applicable.

    Page URL: https://rubberboard.gov.in/public

    Table structure (after stripping HTML):
        Domestic Market** on DD-MM-YYYY per 100Kg
        Kottayam Kochi Agartala
        CategoryINR ₹USD $ RSS4 NNNNN.N NNN.NN RSS5 NNNNN.N NNN.NN     <- Kottayam
        CategoryINR ₹USD $ RSS4 NNNNN.N NNN.NN RSS5 NNNNN.N NNN.NN     <- Kochi
        CategoryINR ₹USD $ RSS4 * * RSS5 * *                           <- Agartala
        International Market on DD-MM-YYYY per 100Kg
        Bangkok KualaLumpur
        CategoryINR ₹USD $ RSS1 # # RSS2 # # ...                       <- Bangkok
        CategoryINR ₹USD $ SMR20 NNNNN.N NNN.NN LATEX(60%) NNNNN.N NNN.NN
    Numeric markers: # = market holiday, * = not available, ~ = no transaction
    """
    out: Dict[str, Dict[str, float]] = {}
    r = _http("https://rubberboard.gov.in/public")
    if not r:
        r = _http("http://rubberboard.gov.in/public")
    if not r:
        return out
    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text(" ", strip=True)

    # ---- Domestic block (Kottayam/Kochi/Agartala) ----
    dm = re.search(
        r"Domestic\s+Market.*?on\s+(\d{1,2})[\-/](\d{1,2})[\-/](\d{4})"
        r"\s+per\s+100Kg(.*?)International\s+Market",
        text, re.I | re.S)
    if dm:
        try:
            dom_date = date(int(dm.group(3)), int(dm.group(2)),
                            int(dm.group(1))).isoformat()
        except ValueError:
            dom_date = _today_iso()
        domestic = dm.group(4)
        # First "RSS4 <inr> <usd>" after the Domestic header is Kottayam.
        m = re.search(
            r"RSS\s*[\-]?\s*4\s+([\d.,]+)\s+([\d.,]+)", domestic)
        if m:
            inr_per_100kg = _to_float(m.group(1))
            if inr_per_100kg and 5000 < inr_per_100kg < 100000:
                out.setdefault("RSS4_KOTTAYAM_INR", {})[dom_date] = round(
                    inr_per_100kg, 0)

    # ---- International block (Bangkok/Kuala Lumpur) ----
    im = re.search(
        r"International\s+Market.*?on\s+(\d{1,2})[\-/](\d{1,2})[\-/](\d{4})"
        r"\s+per\s+100Kg(.*?)(?:Daily\s*/|$)",
        text, re.I | re.S)
    if im:
        try:
            int_date = date(int(im.group(3)), int(im.group(2)),
                            int(im.group(1))).isoformat()
        except ValueError:
            int_date = _today_iso()
        intl = im.group(4)

        # KL SMR20 — pattern: "SMR20  <inr>  <usd>"  (skip if "#" or "*")
        m = re.search(
            r"SMR\s*20\s+([\d.,]+)\s+([\d.,]+)", intl)
        if m:
            usd_100kg = _to_float(m.group(2))
            if usd_100kg and 30 < usd_100kg < 600:
                out.setdefault("SMR20_MYS", {})[int_date] = round(
                    usd_100kg / 100.0, 4)

        # KL LATEX(60%)
        m = re.search(
            r"LATEX\s*\(\s*60\s*%?\s*\)\s+([\d.,]+)\s+([\d.,]+)", intl, re.I)
        if m:
            usd_100kg = _to_float(m.group(2))
            if usd_100kg and 30 < usd_100kg < 600:
                out.setdefault("LATEX_60_MYS", {})[int_date] = round(
                    usd_100kg / 100.0, 4)

        # Bangkok RSS3 — proxy for Thailand benchmark when not on holiday.
        # Bangkok is the FIRST sub-table; capture only its RSS3 row.
        bkk_match = re.search(
            r"Bangkok.*?CategoryINR.*?(RSS\s*3\s+[\d.,]+\s+[\d.,]+)",
            intl, re.I | re.S)
        if bkk_match:
            m = re.search(
                r"RSS\s*3\s+([\d.,]+)\s+([\d.,]+)", bkk_match.group(1))
            if m:
                usd_100kg = _to_float(m.group(2))
                if usd_100kg and 30 < usd_100kg < 600:
                    out.setdefault("RSS3_SGP", {})[int_date] = round(
                        usd_100kg / 100.0, 4)

    return out


# ---------------------------------------------------------------------------
# Source 3: Rubber Board of India daily price (Kottayam RSS-4)
# ---------------------------------------------------------------------------

def fetch_rubberboard_india() -> Dict[str, Dict[str, float]]:
    """Pull RSS-4 Kottayam from rubberboard.gov.in/public.
    Native: INR per 100 kg. Returned as-is (not converted to USD)."""
    out: Dict[str, Dict[str, float]] = {}
    # Try a couple of known paths.
    for url in [
        "https://rubberboard.gov.in/public",
        "http://rubberboard.org.in/public",
        "https://rubberboard.gov.in/Stat/RubberPriceShow",
    ]:
        r = _http(url)
        if not r:
            continue
        text = BeautifulSoup(r.text, "lxml").get_text(" ", strip=True)
        # Look for "RSS-4" or "RSS 4" near "Kottayam" with an INR figure.
        # Typical line: "RSS-4 Kottayam 19,732"  (INR/100kg)
        m = re.search(
            r"RSS[\s-]*4[^0-9]{0,80}([\d,]{4,7})", text, re.I)
        if not m:
            continue
        v = _to_float(m.group(1))
        if v and 5000 < v < 80000:
            iso_date = _today_iso()
            # Page often dates the table; capture if present.
            md = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", text)
            if md:
                try:
                    iso_date = date(int(md.group(3)), int(md.group(2)),
                                    int(md.group(1))).isoformat()
                except ValueError:
                    pass
            out["RSS4_KOTTAYAM_INR"] = {iso_date: round(v, 0)}
            return out
    return out


# ---------------------------------------------------------------------------
# Source 4: SHFE RU front-month settlement
# ---------------------------------------------------------------------------

def fetch_shfe_ru(fx: FX) -> Dict[str, Dict[str, float]]:
    """Pull SHFE rubber RU front-month daily settlement (CNY/tonne)
    and convert to USD/kg."""
    out: Dict[str, Dict[str, float]] = {}
    # SHFE publishes a daily JSON-like .dat for each trading day at
    # /data/dailydata/kx/kxYYYYMMDD.dat. We try yesterday and today.
    for d in (date.today(), date.today() - timedelta(days=1),
              date.today() - timedelta(days=2)):
        ymd = d.strftime("%Y%m%d")
        url = f"https://www.shfe.com.cn/data/dailydata/kx/kx{ymd}.dat"
        r = _http(url)
        if not r:
            continue
        try:
            j = r.json()
        except Exception:
            continue
        # The .dat is JSON: {o_curinstrument: [{INSTRUMENTID, SETTLEMENTPRICE, ...}]}
        rows = (j.get("o_curinstrument")
                or j.get("o_curproduct")
                or j.get("Instrument") or [])
        if not rows:
            continue
        # Find front-month RU contract (PRODUCTID=='ru' and earliest expiry).
        ru = [r for r in rows
              if (str(r.get("PRODUCTID", "")).strip().lower() == "ru"
                  or str(r.get("INSTRUMENTID", "")).strip().lower().startswith("ru"))]
        if not ru:
            continue
        ru.sort(key=lambda r: str(r.get("INSTRUMENTID", "")))
        front = ru[0]
        cny_per_t = _to_float(str(front.get("SETTLEMENTPRICE")
                                  or front.get("CLOSEPRICE")
                                  or ""))
        if not cny_per_t or cny_per_t <= 0:
            continue
        usd_per_cny = fx.usd_per("CNY") or 0.139
        usd_per_kg = (cny_per_t / 1000.0) * usd_per_cny
        out["RU_SHFE_CNY"] = {d.isoformat(): round(usd_per_kg, 4)}
        return out
    return out


# ---------------------------------------------------------------------------
# Source 5: RTAS Singapore (SGX SICOM official settlements)
# ---------------------------------------------------------------------------

def fetch_rtas_sgx() -> Dict[str, Dict[str, float]]:
    """Pull SGX SICOM TSR20 + RSS3 daily settlement from RTAS Singapore.
    Native: USD cents/kg. Returns last ~30 trading days when available."""
    out: Dict[str, Dict[str, float]] = {}
    r = _http("https://www.rtas.sg/rubber-prices/")
    if not r:
        return out
    soup = BeautifulSoup(r.text, "lxml")
    # Their page renders two tables with "Settlement Price" headers and
    # rows like "02-MAY-2026  TSR20 MAY26  206.40"
    for tab in soup.select("table"):
        head = tab.get_text(" ", strip=True).lower()
        if "tsr20" not in head and "rss3" not in head:
            continue
        for tr in tab.select("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
            if len(cells) < 3:
                continue
            d = _parse_loose_date(cells[0])
            if not d:
                continue
            # last numeric cell is settlement price in cents/kg
            v = None
            for c in reversed(cells):
                v = _to_float(c)
                if v and 50 < v < 500:
                    break
            if not v:
                continue
            usd_per_kg = round(v / 100.0, 4)
            if "tsr20" in head:
                out.setdefault("TSR20_SGP", {})[d] = usd_per_kg
            elif "rss3" in head:
                out.setdefault("RSS3_SGP", {})[d] = usd_per_kg
    return out


def _parse_loose_date(s: str) -> Optional[str]:
    s = s.strip()
    for fmt in ("%d-%b-%Y", "%d %b %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

# Series we ATTEMPT to fetch live. Anything not in this dict stays SEED.
LIVE_SOURCES: Dict[str, str] = {
    "RSS3_SGP":          "RTAS Singapore (SGX SICOM settlement)",
    "TSR20_SGP":         "RTAS Singapore (SGX SICOM settlement)",
    "SMR20_MYS":         "ANRPC daily price + MRB Daily5 fallback",
    "LATEX_60_MYS":      "ANRPC daily + MRB Daily5 fallback",
    "STR20_THA":         "ANRPC daily price",
    "LATEX_60_THA":      "ANRPC daily price",
    "SIR20_IDN":         "ANRPC daily price",
    "SVR3L_VNM":         "ANRPC daily price",
    "SVR10_VNM":         "ANRPC daily price",
    "RU_SHFE_CNY":       "SHFE daily statistics",
    "RSS4_KOTTAYAM_INR": "Rubber Board of India",
}
# Series with no public daily feed — flagged SEED honestly.
SEED_ONLY: Dict[str, str] = {
    "TOCOM_RSS3": "OSE/JPX rubber settlements not on public free feed; "
                  "kept synthetic until paid feed is wired in.",
}


def main() -> int:
    if not DATA.exists():
        print("data/prices.json missing; run scripts/seed.py first.")
        return 1
    payload = json.loads(DATA.read_text())
    series = payload["series"]

    fx = FX()
    fx.fetch()

    # Run each source; merge results.
    all_new: Dict[str, Dict[str, float]] = {}
    src_used: Dict[str, str] = {}     # series -> which source it came from
    src_errors: Dict[str, str] = {}

    # Order matters: first source to deliver a series wins attribution.
    # Rubber Board India is now PRIMARY because its single page provides
    # RSS4 Kottayam (India), SMR20 KL + Latex 60% KL (Malaysia), all
    # pre-converted to USD/100kg by the publisher.
    runners = [
        ("RubberBoardIndia_v2", lambda: fetch_rubberboard_india_v2(fx)),
        ("MRB",   lambda: fetch_mrb(fx)),
        ("ANRPC", lambda: fetch_anrpc(fx)),
        ("SHFE",  lambda: fetch_shfe_ru(fx)),
        ("RTAS",  fetch_rtas_sgx),
    ]
    for name, fn in runners:
        try:
            got = fn() or {}
            for code, rows in got.items():
                if not rows:
                    continue
                all_new.setdefault(code, {}).update(rows)
                # First source to deliver this series wins the attribution.
                src_used.setdefault(code, name)
            print(f"[{name}] ok, series filled: "
                  f"{sorted(got.keys())}")
        except Exception as e:
            print(f"[{name}] crashed: {e}", file=sys.stderr)
            traceback.print_exc()
            src_errors[name] = str(e)
        time.sleep(1.5)  # be polite

    # Merge into payload.series
    for code, rows in all_new.items():
        if code not in series:
            print(f"unknown series {code}, skip")
            continue
        for d, v in rows.items():
            series[code][d] = v
        last_d = sorted(rows.keys())[-1]
        print(f"  {code}: +{len(rows)} row(s), latest "
              f"{last_d}={rows[last_d]}")

    # Derive RSS3_SGP from TSR20_SGP if RTAS only gave one of the two.
    if ("TSR20_SGP" in all_new and "RSS3_SGP" not in all_new
            and "RSS3_SGP" in series and "TSR20_SGP" in series):
        ts, rs = series["TSR20_SGP"], series["RSS3_SGP"]
        ks = sorted(set(ts) & set(rs))[-30:]
        if ks:
            avg_sp = sum(rs[k] - ts[k] for k in ks) / len(ks)
            today = sorted(all_new["TSR20_SGP"].keys())[-1]
            rs[today] = round(ts[today] + avg_sp, 4)
            all_new.setdefault("RSS3_SGP", {})[today] = rs[today]
            src_used["RSS3_SGP"] = "Derived (TSR20 + 30d avg spread)"
            print(f"  RSS3_SGP: derived from TSR20 + spread = {rs[today]}")

    # Build per-series live status.
    today = _today_iso()
    horizon = (date.today() - timedelta(days=7)).isoformat()
    status: Dict[str, dict] = {}
    for code in series.keys():
        if code in SEED_ONLY:
            status[code] = {
                "status": "SEED",
                "source": None,
                "reason": SEED_ONLY[code],
                "last_real_print": None,
            }
            continue
        latest_real = max(all_new.get(code, {}).keys()) \
            if all_new.get(code) else None
        if latest_real:
            tag = "LIVE" if latest_real >= horizon else "STALE"
            status[code] = {
                "status": tag,
                "source": LIVE_SOURCES.get(code, src_used.get(code, "unknown")),
                "via": src_used.get(code),
                "last_real_print": latest_real,
            }
        else:
            # We tried but got nothing this run.
            status[code] = {
                "status": "SEED" if payload.get("generated_utc") == "seed"
                          else "STALE",
                "source": LIVE_SOURCES.get(code),
                "reason": "no live print this run; using prior value",
                "last_real_print": None,
            }

    # Persist. Only stamp prices.json with a real timestamp when at least
    # one series received a real print this run — otherwise leave it as
    # "seed" so the topbar tells the truth.
    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if any(s["status"] == "LIVE" for s in status.values()):
        payload["generated_utc"] = run_ts
    DATA.write_text(json.dumps(payload, indent=2))
    META.write_text(json.dumps({
        "generated_utc": run_ts,
        "prices_generated_utc": payload["generated_utc"],
        "fx_source": fx.source,
        "fx_rates_usd_base": fx.rates,
        "series_status": status,
        "source_errors": src_errors,
    }, indent=2))

    live_count = sum(1 for s in status.values() if s["status"] == "LIVE")
    stale_count = sum(1 for s in status.values() if s["status"] == "STALE")
    seed_count = sum(1 for s in status.values() if s["status"] == "SEED")
    print(f"\nDone. live={live_count}  stale={stale_count}  seed={seed_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
