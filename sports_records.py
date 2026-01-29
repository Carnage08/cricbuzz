"""
Sports Match Records - Refined Dynamic Web Scraper
Fetches international cricket match data from Cricbuzz with deep parsing.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time
from contextlib import contextmanager
from typing import List, Dict, Optional


class SportsMatchScraper:
    """Enhanced scraper for international cricket match data"""
    
    BASE_URL = "https://www.cricbuzz.com"
    RECENT_MATCHES_URL = f"{BASE_URL}/cricket-match/live-scores/recent-matches"
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    
    # International patterns
    INTL_PATTERNS = ["tour-of", "t20i", "odi", "test-"]
    
    # Exclude patterns
    EXCLUDE_PATTERNS = [
        "premier-league", "super-smash", "big-bash", "psl", "ipl", "bpl", "cpl", "sa20", 
        "hundred", "ranji", "u19", "women", "domestic", "first-class"
    ]

    # Recognized International Teams
    TEAM_MAP = {
        "IND": "India", "NZ": "New Zealand", "AUS": "Australia", "ENG": "England", 
        "RSA": "South Africa", "SA": "South Africa", "PAK": "Pakistan", "WI": "West Indies", 
        "SL": "Sri Lanka", "BAN": "Bangladesh", "AFG": "Afghanistan", "ZIM": "Zimbabwe",
        "IRE": "Ireland", "ITA": "Italy", "SCO": "Scotland", "NED": "Netherlands"
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse HTML"""
        try:
            time.sleep(0.5) # Be gentle
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"❌ Error fetching {url}: {e}")
            return None

    def clean_text(self, text: str) -> str:
        """Clean and shorten extracted text"""
        if not text: return ""
        # Remove extra whitespaces and newlines
        text = re.sub(r"\s+", " ", text).strip()
        # Limit length to avoid DB bloat
        return text[:100]

    def extract_teams_from_slug(self, slug: str) -> str:
        parts = slug.upper().split("-")
        if "VS" in parts:
            idx = parts.index("VS")
            t1 = self.TEAM_MAP.get(parts[idx-1], parts[idx-1].title()) if idx > 0 else "Unknown"
            t2 = self.TEAM_MAP.get(parts[idx+1], parts[idx+1].title()) if idx < len(parts)-1 else "Unknown"
            return f"{t1} vs {t2}"
        return "Unknown vs Unknown"

    def get_match_details(self, match_id: str, slug: str, teams: str) -> Dict:
        """Visit detail pages for high-fidelity data"""
        details = {"winner": None, "venue": None, "match_name": None, "format": "Unknown"}
        team_parts = [t.strip().lower() for t in teams.split(" vs ")]
        
        # 1. Get Venue from Live Score page
        live_url = f"{self.BASE_URL}/live-cricket-scores/{match_id}/{slug}"
        soup = self.fetch_page(live_url)
        if soup:
            venue_el = soup.select_one('a[href*="/venues/"]')
            if venue_el:
                details["venue"] = self.clean_text(venue_el.get_text())
            
            # Extract match name from h1 tag
            h1_tag = soup.find("h1")
            if h1_tag:
                match_name = h1_tag.get_text().strip()
                # Remove common suffixes
                for suffix in [" - Live Cricket Score", " Live Score", " - Scorecard"]:
                    match_name = match_name.replace(suffix, "")
                details["match_name"] = match_name.strip()
                
                # Extract format
                if "T20I" in match_name.upper(): details["format"] = "T20I"
                elif "ODI" in match_name.upper(): details["format"] = "ODI"
                elif "TEST" in match_name.upper(): details["format"] = "Test"
            
            winner_el = soup.select_one('#sticky-mcomplete div div')
            if winner_el:
                txt = self.clean_text(winner_el.get_text())
                # Validate winner text contains one of the teams
                if any(team in txt.lower() for team in team_parts):
                    details["winner"] = txt

        # 2. Match Facts (Venue, Winner fallback, and Officials)
        facts_url = f"{self.BASE_URL}/cricket-match-facts/{match_id}/{slug}"
        soup = self.fetch_page(facts_url)
        if soup:
            if not details["venue"]:
                venue_el = soup.select_one('a[href*="/venues/"]') or soup.select_one('a[href*="/cricket-grounds/"]')
                if venue_el:
                    details["venue"] = self.clean_text(venue_el.get_text())
            
            if not details["winner"]:
                candidate = soup.find(string=re.compile(r"won by|Match tied|No result", re.I))
                if candidate:
                    res_text = self.clean_text(candidate.parent.get_text())
                    if any(team in res_text.lower() for team in team_parts):
                        details["winner"] = res_text

            # --- Extract Officials ---
            details.update({"umpire_1": None, "umpire_2": None, "tv_umpire": None, "match_referee": None})
            # Try multiple selectors as Cricbuzz uses different layouts
            info_rows = soup.select(".cb-mtch-info-itm, .facts-row-grid, .cb-col-100.cb-col")
            
            for row in info_rows:
                txt = row.get_text(separator=" ", strip=True)
                if txt.startswith("Umpires"):
                    # Clean label and split names
                    val = re.sub(r"^Umpires:?\s*", "", txt).strip()
                    names = [n.strip() for n in val.split(",") if n.strip()]
                    if len(names) >= 1: details["umpire_1"] = names[0]
                    if len(names) >= 2: details["umpire_2"] = names[1]
                elif txt.startswith("3rd Umpire"):
                    details["tv_umpire"] = re.sub(r"^3rd Umpire:?\s*", "", txt).strip()
                elif txt.startswith("Referee"):
                    details["match_referee"] = re.sub(r"^Referee:?\s*", "", txt).strip()

        return details

    def scrape(self) -> List[Dict]:
        print("🔍 Scanning recent international matches...")
        soup = self.fetch_page(self.RECENT_MATCHES_URL)
        if not soup: return []

        matches = []
        seen_ids = set()
        
        links = soup.find_all("a", href=re.compile(r"/live-cricket-scores/\d+/"))
        for link in links:
            href = link.get("href", "")
            match_id_match = re.search(r"/(\d+)/", href)
            if not match_id_match: continue
            match_id = match_id_match.group(1)
            
            if match_id in seen_ids: continue
            
            # Filtering Logic
            url_lower = href.lower()
            if any(p in url_lower for p in self.EXCLUDE_PATTERNS): continue
            if not any(p in url_lower for p in self.INTL_PATTERNS): continue
            
            seen_ids.add(match_id)
            slug = href.split("/")[-1]
            teams = self.extract_teams_from_slug(slug)
            
            print(f"📊 Processing ID {match_id}: {teams}...")
            details = self.get_match_details(match_id, slug, teams)
            
            # Final check to filter placeholders/upcoming
            if not details["winner"]:
                print(f"   ⏩ Skipping upcoming/no-result: {teams}")
                continue
            
            matches.append({
                "match_id": match_id,
                "teams": teams,
                "match_name": details["match_name"] or teams,
                "format": details["format"],
                "winner": details["winner"],
                "venue": details["venue"] or "Unknown",
                "officials": {
                    "umpire_1": details.get("umpire_1"),
                    "umpire_2": details.get("umpire_2"),
                    "tv_umpire": details.get("tv_umpire"),
                    "match_referee": details.get("match_referee")
                }
            })
            
        return matches


class SportsMatchRecords:
    def __init__(self, db_path: str = "cricbuzz.db"):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sports_match_records (
                    match_id TEXT PRIMARY KEY,
                    teams TEXT NOT NULL,
                    match_name TEXT,
                    format TEXT,
                    winner TEXT,
                    venue TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS match_officials (
                    match_id TEXT PRIMARY KEY,
                    umpire_1 TEXT,
                    umpire_2 TEXT,
                    tv_umpire TEXT,
                    match_referee TEXT,
                    FOREIGN KEY (match_id) REFERENCES sports_match_records(match_id)
                )
            """)

    def save_matches(self, matches: List[Dict]):
        with self._get_conn() as conn:
            conn.execute("DELETE FROM sports_match_records")
            conn.execute("DELETE FROM match_officials")
            
            for m in matches:
                # Store match metadata
                conn.execute("""
                    INSERT INTO sports_match_records (match_id, teams, match_name, format, winner, venue)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (m['match_id'], m['teams'], m['match_name'], m['format'], m['winner'], m['venue']))
                
                # Store officials
                off = m['officials']
                conn.execute("""
                    INSERT INTO match_officials (match_id, umpire_1, umpire_2, tv_umpire, match_referee)
                    VALUES (?, ?, ?, ?, ?)
                """, (m['match_id'], off['umpire_1'], off['umpire_2'], off['tv_umpire'], off['match_referee']))

    def display(self):
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM sports_match_records").fetchall()
            print("\n" + "="*140)
            print(f"{'ID':<10} {'FORMAT':<10} {'MATCH NAME':<50} {'WINNER':<30} {'VENUE'}")
            print("-" * 140)
            for r in rows:
                print(f"{r['match_id']:<10} {r['format']:<10} {r['match_name']:<50} {r['winner']:<30} {r['venue']}")
            print("="*140)


if __name__ == "__main__":
    scraper = SportsMatchScraper()
    data = scraper.scrape()
    
    db = SportsMatchRecords()
    db.save_matches(data)
    db.display()
