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
        details = {"winner": None, "venue": None}
        team_parts = [t.strip().lower() for t in teams.split(" vs ")]
        
        # 1. Get Venue from Live Score page
        live_url = f"{self.BASE_URL}/live-cricket-scores/{match_id}/{slug}"
        soup = self.fetch_page(live_url)
        if soup:
            venue_el = soup.select_one('a[href*="/venues/"]')
            if venue_el:
                details["venue"] = self.clean_text(venue_el.get_text())
            
            winner_el = soup.select_one('#sticky-mcomplete div div')
            if winner_el:
                txt = self.clean_text(winner_el.get_text())
                # Validate winner text contains one of the teams
                if any(team in txt.lower() for team in team_parts):
                    details["winner"] = txt

        # 2. Fallback to Match Facts
        if not details["venue"] or not details["winner"]:
            facts_url = f"{self.BASE_URL}/cricket-match-facts/{match_id}/{slug}"
            soup = self.fetch_page(facts_url)
            if soup:
                if not details["venue"]:
                    venue_el = soup.select_one('a[href*="/venues/"]') or soup.select_one('a[href*="/cricket-grounds/"]')
                    if venue_el:
                        details["venue"] = self.clean_text(venue_el.get_text())
                
                if not details["winner"]:
                    # Look specifically for the result banner in match facts
                    # Re-trying patterns that are likely to be specific to this match
                    # Often in a cb-toss-sts class or similar
                    candidate = soup.find(string=re.compile(r"won by|Match tied|No result", re.I))
                    if candidate:
                        res_text = self.clean_text(candidate.parent.get_text())
                        if any(team in res_text.lower() for team in team_parts):
                            details["winner"] = res_text

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
                "winner": details["winner"],
                "venue": details["venue"] or "Unknown"
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
                    winner TEXT,
                    venue TEXT
                )
            """)

    def save_matches(self, matches: List[Dict]):
        with self._get_conn() as conn:
            conn.execute("DELETE FROM sports_match_records") # Fresh start
            conn.executemany("""
                INSERT INTO sports_match_records (match_id, teams, winner, venue)
                VALUES (:match_id, :teams, :winner, :venue)
            """, matches)

    def display(self):
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM sports_match_records").fetchall()
            print("\n" + "="*90)
            print(f"{'ID':<10} {'TEAMS':<30} {'WINNER':<20} {'VENUE'}")
            print("-" * 90)
            for r in rows:
                print(f"{r['match_id']:<10} {r['teams']:<30} {r['winner']:<20} {r['venue']}")
            print("="*90)


if __name__ == "__main__":
    scraper = SportsMatchScraper()
    data = scraper.scrape()
    
    db = SportsMatchRecords()
    db.save_matches(data)
    db.display()
