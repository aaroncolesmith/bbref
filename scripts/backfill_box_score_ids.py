"""One-off: add bbref's player_id (e.g. dickgr01) to every existing row of
data/nba_box_scores.parquet, so players who share a name stay distinct.
(New games get player_id straight from the box score page in bbref.py.)

Source of truth is each season's per-player totals page
(/leagues/NBA_<season>_totals.html), which lists every player's id per team.
A box score row is matched on (season, team, name); failing that, on an
accent/punctuation/suffix-normalised name within the same season+team (bbref
renames players — "J.J. Redick" became "JJ Redick"); then on a name that
belongs to exactly one player that season, or in all of bbref (playoff-only
stints are missing from the regular-season totals page); then by elimination
within a season+team (one leftover box-score name, one leftover roster id —
nickname renames like "Kenyon Martin Jr." -> "KJ Martin"). Whatever is still
unmatched is read straight off its box score page, the authoritative source.

Usage:
    python scripts/backfill_box_score_ids.py --cache /tmp/bbref_totals          # dry run: match report
    python scripts/backfill_box_score_ids.py --cache /tmp/bbref_totals --write  # rewrite the parquet
"""
from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup

from bbref_bios import BASE, fetch, load_index

BOX = Path("data/nba_box_scores.parquet")
GAMES = Path("data/nba_games.parquet")
MULTI_TEAM = {"TOT", "2TM", "3TM", "4TM", "5TM"}


def strict(name: str) -> str:
    """Accents, case and punctuation only — keeps Jr./II, so a son never matches his father."""
    s = unicodedata.normalize("NFKD", name.replace("ё", "e").replace("Ё", "E")).encode("ascii", "ignore").decode()
    s = re.sub(r"[.'\-]", "", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def norm(name: str) -> str:
    """Also drops Jr./Sr./II — only safe within one season+team roster."""
    return re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", strict(name))


def season_rosters(seasons: list[int], cache: Path) -> pd.DataFrame:
    cache.mkdir(parents=True, exist_ok=True)
    rows = []
    for season in seasons:
        path = cache / f"NBA_{season}_totals.html"
        if not path.exists():
            r = fetch(f"{BASE}/leagues/NBA_{season}_totals.html")
            if r is None:
                print(f"no totals page for {season}")
                continue
            path.write_text(r.text)
        soup = BeautifulSoup(path.read_text(), "lxml")
        table = soup.find("table", id=re.compile("totals"))
        for tr in table.tbody.find_all("tr"):
            cell = tr.find(attrs={"data-append-csv": True})
            team = tr.find("td", attrs={"data-stat": re.compile(r"^team_(id|name_abbr)$")})
            if cell is None or team is None:
                continue
            team_id = team.get_text(strip=True)
            if team_id in MULTI_TEAM:
                continue
            rows.append({"season": season, "team": team_id, "player_id": cell["data-append-csv"],
                         "name": cell.get_text(strip=True).rstrip("*")})
    df = pd.DataFrame(rows).drop_duplicates()
    print(f"rosters: {len(df):,} player-team-seasons across {df.season.nunique()} seasons")
    return df


def assign_ids(box: pd.DataFrame, games: pd.DataFrame, rosters: pd.DataFrame, index: pd.DataFrame) -> pd.Series:
    season = box["game_id"].map(games.drop_duplicates("game_id").set_index("game_id")["season"])
    keys = pd.DataFrame({"season": season, "team": box["team"], "player": box["player"]})
    keys["norm"] = keys["player"].map(norm)
    keys["strict"] = keys["player"].map(strict)
    rosters = rosters.assign(norm=rosters["name"].map(norm), strict=rosters["name"].map(strict))

    def unique_lookup(frame: pd.DataFrame, cols: list[str]) -> pd.Series:
        g = frame.groupby(cols)["player_id"].agg(lambda s: s.iloc[0] if s.nunique() == 1 else None)
        return g.dropna()

    exact = unique_lookup(rosters, ["season", "team", "name"])
    fuzzy = unique_lookup(rosters, ["season", "team", "norm"])
    by_season = unique_lookup(rosters, ["season", "strict"])

    ids = pd.Series(pd.MultiIndex.from_frame(keys[["season", "team", "player"]]).map(exact.to_dict().get), index=box.index)
    miss = ids.isna()
    ids[miss] = pd.MultiIndex.from_frame(keys.loc[miss, ["season", "team", "norm"]]).map(fuzzy.to_dict().get)
    miss = ids.isna()
    ids[miss] = pd.MultiIndex.from_frame(keys.loc[miss, ["season", "strict"]]).map(by_season.to_dict().get)
    everyone = unique_lookup(index.assign(strict=index["index_name"].map(strict)), ["strict"])
    miss = ids.isna()
    ids[miss] = keys.loc[miss, "player"].map(strict).map(everyone)

    # Elimination: one unmatched name and one unused roster id on the same season+team.
    miss = ids.isna()
    used = set(zip(keys.loc[~miss, "season"], keys.loc[~miss, "team"], ids[~miss]))
    open_ids = (rosters[[(s, t, p) not in used for s, t, p in zip(rosters.season, rosters.team, rosters.player_id)]]
                .groupby(["season", "team"])["player_id"].agg(list))
    leftover = keys[miss].groupby(["season", "team"])["player"].agg(lambda s: sorted(set(s)))
    for (season, team), names in leftover.items():
        cands = open_ids.get((season, team), [])
        if len(names) == 1 and len(cands) == 1:
            ids[miss & (keys.season == season) & (keys.team == team) & (keys.player == names[0])] = cands[0]

    # Renamed mid-season (the id is already used under the new name): same words in any
    # order ("Yongxi Cui" / "Cui Yongxi"), else same surname + first initial ("Gregory
    # Jackson" / "GG Jackson", not Jaren Jackson Jr.), else same surname — each only
    # when exactly one roster player on that team-season fits.
    # Positional: the box-score frame's index isn't unique (bbref.py saves pandas' index).
    miss_pos = np.flatnonzero(ids.isna().to_numpy())
    roster_by_team = rosters.groupby(["season", "team"])
    for (season, team, player), pos in keys.iloc[miss_pos].groupby(["season", "team", "player"]).indices.items():
        if (season, team) not in roster_by_team.groups:
            continue
        roster = roster_by_team.get_group((season, team))
        want = norm(player).split()
        same_surname = [(pid, n) for pid, n in zip(roster.player_id, roster.norm) if n.split()[-1] == want[-1]]
        hits = {pid for pid, n in zip(roster.player_id, roster.norm) if sorted(n.split()) == sorted(want)}
        hits = hits or {pid for pid, n in same_surname if n[0] == want[0][0]}
        hits = hits or {pid for pid, _ in same_surname}  # "Carlton" -> "Bub" Carrington
        if len(hits) == 1:
            ids.iloc[miss_pos[pos]] = hits.pop()
    return ids


def ids_from_box_pages(box: pd.DataFrame, ids: pd.Series, cache: Path) -> pd.Series:
    """Last resort: read player ids straight off each still-unmatched game's page.

    Matched per team on name, then points, then minutes — a team can field two
    players with one name (the 1988-89 Bullets had two Charles Joneses)."""
    ids = ids.copy()
    for game_id in box.loc[ids.isna(), "game_id"].unique():
        path = cache / f"box_{game_id}.html"
        if not path.exists():
            r = fetch(f"{BASE}/boxscores/{game_id}.html")
            if r is None:
                continue
            path.write_text(r.text)
        soup = BeautifulSoup(path.read_text(), "lxml")
        played = []  # (team, strict name, norm name, id, pts, minutes)
        for table in soup.find_all("table", id=re.compile(r"^box-[A-Z0-9]+-game-basic$")):
            team = table["id"].split("-")[1]
            for tr in table.tbody.find_all("tr"):
                th = tr.find("th", attrs={"data-append-csv": True})
                mp = tr.find("td", attrs={"data-stat": "mp"})
                if th is None or mp is None:  # header rows, Did Not Play
                    continue
                pts = tr.find("td", attrs={"data-stat": "pts"})
                name = th.get_text(strip=True)
                played.append((team, strict(name), norm(name), th["data-append-csv"],
                               _num(pts.get_text()) if pts else None, _minutes(mp.get_text())))

        for pos in np.flatnonzero((ids.isna() & (box["game_id"] == game_id)).to_numpy()):
            row = box.iloc[pos]
            cands = [p for p in played if p[0] == row["team"] and p[1] == strict(row["player"])]
            cands = cands or [p for p in played if p[0] == row["team"] and p[2] == norm(row["player"])]
            if len(cands) > 1:
                cands = [p for p in cands if p[4] == _num(row.get("pts"))] or cands
            if len(cands) > 1:
                cands = [p for p in cands if p[5] is not None and abs(p[5] - (_num(row.get("mp")) or -99)) < 0.02]
            if len(cands) == 1:
                ids.iloc[pos] = cands[0][3]
    return ids


def _num(v) -> float | None:
    try:
        return None if v is None or pd.isna(v) else float(v)
    except (TypeError, ValueError):
        return None


def _minutes(s: str) -> float | None:
    m = re.match(r"(\d+):(\d+)", s.strip())
    if m:
        return round(int(m.group(1)) + int(m.group(2)) / 60, 2)
    return _num(s.strip() or None)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", type=Path, required=True, help="dir to cache season totals pages")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    box = pd.read_parquet(BOX)
    games = pd.read_parquet(GAMES)
    rosters = season_rosters(sorted(games["season"].unique().tolist()), args.cache)
    index_path = args.cache / "index.parquet"
    if index_path.exists():
        index = pd.read_parquet(index_path)
    else:
        index = load_index()
        index.to_parquet(index_path)
    ids = assign_ids(box, games, rosters, index)
    print(f"from rosters: {ids.notna().sum():,} rows; fetching {box.loc[ids.isna(), 'game_id'].nunique()} box score pages for the rest")
    ids = ids_from_box_pages(box, ids, args.cache)

    unmatched = box.loc[ids.isna()]
    print(f"matched {ids.notna().sum():,} / {len(box):,} rows ({ids.notna().mean():.4%})")
    if len(unmatched):
        print("unmatched (player, team, rows):")
        print(unmatched.groupby(["player", "team"]).size().sort_values(ascending=False).head(25).to_string())

    shared = box.assign(player_id=ids).groupby("player")["player_id"].nunique()
    print(f"names now split across >1 player_id: {(shared > 1).sum()}")

    if args.write:
        box.insert(1, "player_id", ids)
        # Same writer as bbref.py, so the file's layout doesn't change between scrapes.
        pq.write_table(pa.Table.from_pandas(box), BOX, compression="BROTLI")
        print(f"wrote {BOX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
