"""Scrape a bio for every NBA/ABA/BAA player on basketball-reference into
data/nba_bios.parquet, keyed by bbref's player_id (e.g. dickgr01 from
/players/d/dickgr01.html) so same-name players stay distinct.

Incremental and time-boxed, because a full pull is ~5,000 player pages and
Sports Reference allows 20 requests/minute:

- The A–Z player index (26 pages) is always re-read: it is the list of ids.
- A player page is fetched when the id is new or the player is recent (played in
  the last three seasons, or has a current team): team / experience change, and
  unsigned players can return. Older players' bios are fetched once. There is
  no scraped-at stamp, so the file only changes when a bio does — the monthly
  commit is skipped otherwise and git history stays small.
- Stops after --budget-minutes and writes what it has; the next run resumes.
  Checkpoints every 100 players so a killed run keeps its progress.

Usage:
    python scripts/bbref_bios.py --budget-minutes 320
    python scripts/bbref_bios.py --ids dickgr01 jokicni01 --out /tmp/bios.parquet   # test
"""
from __future__ import annotations

import argparse
import html
import re
import string
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://www.basketball-reference.com"
OUT = Path("data/nba_bios.parquet")
DELAY_S = 3.5          # ~17 req/min, under the 20/min limit
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"}

_last_request = 0.0


def fetch(url: str) -> requests.Response | None:
    """GET with a global rate limit; honours 429 Retry-After. None on 404."""
    global _last_request
    for attempt in range(4):
        wait = DELAY_S - (time.time() - _last_request)
        if wait > 0:
            time.sleep(wait)
        _last_request = time.time()
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            hold = int(r.headers.get("retry-after", "3600"))
            print(f"429 on {url}; holding {hold}s", flush=True)
            time.sleep(hold + 10)
            continue
        if r.status_code >= 500:
            time.sleep(30 * (attempt + 1))
            continue
        r.raise_for_status()
        r.encoding = "utf-8"
        return r
    raise RuntimeError(f"gave up on {url}")


def load_index() -> pd.DataFrame:
    rows = []
    for letter in string.ascii_lowercase:
        r = fetch(f"{BASE}/players/{letter}/")
        if r is None:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        table = soup.find("table", id="players")
        if table is None:
            continue
        for tr in table.tbody.find_all("tr"):
            th = tr.find("th", attrs={"data-stat": "player"})
            if th is None or not th.get("data-append-csv"):
                continue
            cell = {td["data-stat"]: td for td in tr.find_all("td")}
            rows.append({
                "player_id": th["data-append-csv"],
                "index_name": th.get_text(strip=True).rstrip("*"),
                "first_season": _int(cell["year_min"].get_text()),
                "last_season": _int(cell["year_max"].get_text()),
                "pos": cell["pos"].get_text(strip=True) or None,
            })
    print(f"index: {len(rows):,} players", flush=True)
    return pd.DataFrame(rows)


def _int(s: str | None) -> int | None:
    s = (s or "").strip().replace(",", "")
    return int(s) if s.isdigit() else None


def _text(el) -> str:
    t = re.sub(r"\s+", " ", html.unescape(el.get_text(" ")))
    return re.sub(r"\s+([,;)])", r"\1", t).replace("( ", "(").strip()


ORDINAL = r"(\d+)(?:st|nd|rd|th)"


def parse_bio(page: str, player_id: str) -> dict:
    soup = BeautifulSoup(page, "lxml")
    meta = soup.find(id="meta")
    bio: dict = {"player_id": player_id}
    h1 = meta.find("h1")
    bio["name"] = _text(h1) if h1 else None
    img = meta.find("img")
    bio["headshot_url"] = img["src"] if img and img.get("src") else None

    for p in meta.find_all("p"):
        t = _text(p)
        label = p.find("strong")
        key = _text(label).rstrip(":").strip() if label else ""

        if t.startswith("Pronunciation"):
            bio["pronunciation"] = t.split(":", 1)[1].strip()
        elif t.startswith("(") and t.endswith(")"):
            bio["nicknames"] = t[1:-1].strip()
        elif key.startswith("Position"):
            m = re.search(r"Position:\s*(.*?)\s*(?:▪|$)", t)
            bio["position"] = m.group(1).strip() if m else None
            m = re.search(r"Shoots:\s*(\w+)", t)
            bio["shoots"] = m.group(1) if m else None
        elif re.match(r"^\d+-\d+,", t) or re.search(r"\d+lb", t):
            m = re.search(r"(\d+)-(\d+)", t)
            if m:
                bio["height"] = f"{m.group(1)}-{m.group(2)}"
                bio["height_in"] = int(m.group(1)) * 12 + int(m.group(2))
            m = re.search(r"(\d+)lb", t)
            bio["weight_lb"] = int(m.group(1)) if m else None
            m = re.search(r"(\d+)cm", t)
            bio["height_cm"] = int(m.group(1)) if m else None
            m = re.search(r"(\d+)kg", t)
            bio["weight_kg"] = int(m.group(1)) if m else None
        elif key == "Team":
            bio["current_team"] = t.split(":", 1)[1].strip()
            a = p.find("a", href=re.compile(r"^/teams/"))
            bio["current_team_id"] = a["href"].split("/")[2] if a else None
        elif key.startswith("Born"):
            span = p.find(id="necro-birth")
            bio["birth_date"] = span["data-birth"] if span and span.get("data-birth") else None
            m = re.search(r"\bin\s+(.+?)(?:\s+[a-z]{2})?$", t)
            if m:
                place = m.group(1).strip()
                city, _, region = place.rpartition(",")
                bio["birth_city"] = city.strip() or None
                bio["birth_region"] = region.strip() or None
            flag = p.find("span", class_=re.compile(r"\bf-i\b"))
            bio["birth_country_code"] = _text(flag).upper() if flag else None
        elif key.startswith("Died"):
            span = p.find(id="necro-death")
            bio["death_date"] = span["data-death"] if span and span.get("data-death") else _date(t.split(":", 1)[1])
        elif key.startswith("Relatives"):
            bio["relatives"] = t.split(":", 1)[1].strip()
        elif key.startswith("College"):
            bio["colleges"] = t.split(":", 1)[1].strip()
        elif key.startswith("High School"):
            bio["high_schools"] = t.split(":", 1)[1].strip()
        elif key.startswith("Recruiting Rank"):
            m = re.search(r"(\d{4})\s*\((\d+)\)", t)
            if m:
                bio["recruiting_class"], bio["recruiting_rank"] = int(m.group(1)), int(m.group(2))
        elif key.startswith("Draft"):
            bio["draft"] = t.split(":", 1)[1].strip()
            a = p.find("a", href=re.compile(r"/draft\.html$"))
            bio["draft_team_id"] = a["href"].split("/")[2] if a else None
            bio["draft_team"] = _text(a) if a else None
            m = re.search(ORDINAL + r" round", t)
            bio["draft_round"] = int(m.group(1)) if m else None
            m = re.search(ORDINAL + r" pick", t)
            bio["draft_pick"] = int(m.group(1)) if m else None
            m = re.search(ORDINAL + r" overall", t)
            bio["draft_overall"] = int(m.group(1)) if m else None
            m = re.search(r"(\d{4}) (NBA|ABA|BAA)", t)
            if m:
                bio["draft_year"], bio["draft_league"] = int(m.group(1)), m.group(2)
        elif re.match(r"(NBA|ABA|BAA) Debut", key):
            # Two-league players get "NBA Debut: <date> ▪ ABA Debut: <date>" in one line.
            links = p.find_all("a", href=re.compile(r"^/boxscores/"))
            for part in t.split("▪"):
                m = re.match(r"\s*(NBA|ABA|BAA) Debut:\s*(.+)", part)
                if not m:
                    continue
                prefix = "aba_debut" if m.group(1) == "ABA" else "debut"
                bio[f"{prefix}_date"] = _date(m.group(2))
                a = next((a for a in links if _text(a) in part), None)
                bio[f"{prefix}_game_id"] = a["href"].split("/")[-1].replace(".html", "") if a else None
        elif key.startswith("Hall of Fame"):
            bio["hall_of_fame"] = re.sub(r"\(\s*Full List\s*\)", "", t.split(":", 1)[1]).strip()
        elif key.startswith("Experience") or key.startswith("Career Length"):
            if key.startswith("Experience"):  # "Career Length" is used once retired
                bio["is_active"] = True
            m = re.search(r"(\d+)\s+year", t)
            bio["years_experience"] = int(m.group(1)) if m else (0 if "Rookie" in t else None)
        elif label and "▪" in t or (label and p.find("a", href=re.compile(r"instagram|twitter|x\.com"))):
            # "<full name> ▪ Instagram: handle" — or just the full legal name.
            bio["full_name"] = _text(label)
            for a in p.find_all("a", href=True):
                if "instagram.com" in a["href"]:
                    bio["instagram"] = _text(a)
                elif "twitter.com" in a["href"] or "x.com" in a["href"]:
                    bio["twitter"] = _text(a)
        elif label and not key.endswith(":") and "full_name" not in bio and p.find("strong") and ":" not in t:
            bio["full_name"] = t

    bio["is_active"] = bio.get("is_active", False) or "current_team" in bio
    return bio


def _date(s: str) -> str | None:
    try:
        return datetime.strptime(s.strip(), "%B %d, %Y").date().isoformat()
    except ValueError:
        return None


COLUMNS = [
    "player_id", "name", "full_name", "nicknames", "pronunciation", "is_active", "first_season", "last_season",
    "pos", "position", "shoots", "height", "height_in", "weight_lb", "height_cm", "weight_kg",
    "current_team", "current_team_id", "years_experience",
    "birth_date", "birth_city", "birth_region", "birth_country_code", "death_date",
    "colleges", "high_schools", "recruiting_class", "recruiting_rank",
    "draft", "draft_team", "draft_team_id", "draft_year", "draft_league", "draft_round", "draft_pick", "draft_overall",
    "debut_date", "debut_game_id", "aba_debut_date", "aba_debut_game_id", "hall_of_fame", "relatives",
    "instagram", "twitter", "headshot_url",
]


def write(bios: dict[str, dict], index: pd.DataFrame, out: Path) -> None:
    df = pd.DataFrame(list(bios.values()))
    df = df.drop(columns=[c for c in ("first_season", "last_season", "pos") if c in df.columns])
    df = df.merge(index[["player_id", "first_season", "last_season", "pos"]], on="player_id", how="left")
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[COLUMNS].sort_values("player_id").reset_index(drop=True)
    for c in ("birth_date", "death_date", "debut_date", "aba_debut_date"):
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    for c in ("height_in", "weight_lb", "height_cm", "weight_kg", "years_experience", "recruiting_class",
              "recruiting_rank", "draft_year", "draft_round", "draft_pick", "draft_overall",
              "first_season", "last_season"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    # bbref shows "Experience" (not "Career Length") for anyone not formally retired,
    # which includes players out of the league for years. Active here means rostered
    # now or played in the latest season.
    df["is_active"] = df["current_team"].notna() | (df["last_season"] == df["last_season"].max())
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    df.to_parquet(tmp, index=False, compression="zstd")
    tmp.replace(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget-minutes", type=float, default=320)
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--ids", nargs="*", help="scrape only these ids (testing)")
    args = ap.parse_args()
    deadline = time.time() + args.budget_minutes * 60

    bios: dict[str, dict] = {}
    if args.out.exists():
        bios = {r["player_id"]: r for r in pd.read_parquet(args.out).to_dict("records")}

    if args.ids:
        index = pd.DataFrame({"player_id": args.ids, "first_season": None, "last_season": None, "pos": None})
        todo = list(args.ids)
    else:
        index = load_index()
        new = [pid for pid in index.player_id if pid not in bios]
        recent_ids = set(index.loc[index.last_season >= index.last_season.max() - 2, "player_id"])
        recent = [pid for pid, b in bios.items() if pid in recent_ids or b.get("current_team")]
        todo = new + recent
        print(f"todo: {len(new):,} new, {len(recent):,} recent to refresh ({len(bios):,} on file)", flush=True)

    done = failed = 0
    for pid in todo:
        if time.time() > deadline:
            print("time budget reached — the next run resumes", flush=True)
            break
        r = fetch(f"{BASE}/players/{pid[0]}/{pid}.html")
        if r is None:
            failed += 1
            continue
        try:
            bio = parse_bio(r.text, pid)
        except Exception as exc:  # one odd page shouldn't sink the run
            print(f"parse failed for {pid}: {exc}", flush=True)
            failed += 1
            continue
        bios[pid] = bio
        done += 1
        if done % 100 == 0:
            write(bios, index, args.out)
            print(f"  {done:,}/{len(todo):,} …", flush=True)

    if bios:
        write(bios, index, args.out)
    remaining = len(todo) - done - failed
    print(f"scraped {done:,}, failed {failed}, remaining {max(remaining, 0):,}; {len(bios):,} bios in {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
