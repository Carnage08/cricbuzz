
import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re

# List of matches provided by the user
MATCH_IDS = [
    116441, 121389, 121400, 121406, 133000, 133011, 133017, 
    137826, 137831, 140537, 140548, 140559
]

DB_PATH = "cricbuzz.db"

# Known Roles to check for suffix
# Longer matches first
KNOWN_ROLES = [
    "Batting Allrounder", 
    "Bowling Allrounder", 
    "WK-Batter", 
    "Batter", 
    "Bowler",
    "Head Coach",
    "Assistant coach",
    "Fielding Coach",
    "Batting Coach",
    "Bowling Coach",
    "Coach"
]

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Drop to schema change
    cursor.execute("DROP TABLE IF EXISTS players")
    cursor.execute("DROP TABLE IF EXISTS match_squads")
    
    # Create Players Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS players (
        player_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT
    )
    """)
    
    # Create Match Squads Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS match_squads (
        squad_id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id TEXT NOT NULL,
        player_id INTEGER NOT NULL,
        team TEXT NOT NULL,
        FOREIGN KEY (match_id) REFERENCES sports_match_records(match_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
    """)
    
    conn.commit()
    conn.close()

def extract_teams_from_title(title):
    try:
        if " vs " in title:
            parts = title.split(" vs ")
            team1 = parts[0].strip()
            remainder = parts[1]
            separators = [",", "Squads", "Scorecard", "Live", "Match", "1st", "2nd", "3rd", "4th", "5th", "T20I", "ODI", "Test"]
            idx = len(remainder)
            for sep in separators:
                if sep in remainder:
                    i = remainder.find(sep)
                    if i != -1 and i < idx:
                        idx = i
            team2 = remainder[:idx].strip()
            return team1, team2
    except:
        pass
    return "Unknown A", "Unknown B"

def parse_name_role(full_text):
    """
    Separates 'Kristian ClarkeBowler' -> 'Kristian Clarke', 'Bowler'
    """
    full_text = full_text.strip()
    
    # Check for roles at the end of the string
    found_role = None
    name_part = full_text
    
    for role in KNOWN_ROLES:
        # Check if text ends with this role
        # Case insensitive check? Or exact? 
        # The text usually matches case e.g. "Batter"
        if full_text.endswith(role):
            found_role = role
            # Remove role from end
            name_part = full_text[:-len(role)].strip()
            break
            
    return name_part, found_role

def scrape_squads():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    for match_id in MATCH_IDS:
        print(f"Processing Match ID: {match_id}...")
        
        url = f"https://www.cricbuzz.com/cricket-match-squads/{match_id}/squads"
        
        try:
            r = requests.get(url, headers=headers)
            if r.status_code != 200:
                print(f"❌ Failed to fetch page. Status: {r.status_code}")
                continue
                
            soup = BeautifulSoup(r.text, "html.parser")
            
            title = soup.title.string if soup.title else ""
            t1_name, t2_name = extract_teams_from_title(title)
            
            # Clean names just in case
            t1_name = t1_name.replace("Cricket match squads | ", "")
            t2_name = t2_name.replace("Cricket match squads | ", "")
            
            cols = soup.find_all("div", class_="w-1/2")
            
            if len(cols) < 2:
                continue
            
            def process_col(col, team_name):
                count = 0
                links = col.find_all("a", href=re.compile(r"/profiles/"))
                
                for i, link in enumerate(links):
                    if i >= 11: break
                    
                    href = link['href']
                    full_text = link.get_text().strip()
                    
                    name, role = parse_name_role(full_text)
                    
                    # Debug print occasionally
                    if i == 0:
                        print(f"   Sample: '{full_text}' -> Name: '{name}', Role: '{role}'")
                    
                    m = re.search(r"/profiles/(\d+)/", href)
                    if m:
                        p_id = int(m.group(1))
                        
                        # Insert Player (Update role if missing or changed)
                        cursor.execute("""
                            INSERT INTO players (player_id, name, role) 
                            VALUES (?, ?, ?)
                            ON CONFLICT(player_id) DO UPDATE SET role=excluded.role, name=excluded.name
                        """, (p_id, name, role))
                        
                        # Insert Squad
                        cursor.execute("""
                            INSERT OR IGNORE INTO match_squads (match_id, player_id, team)
                            VALUES (?, ?, ?)
                        """, (str(match_id), p_id, team_name))
                        count += 1
                return count

            process_col(cols[0], t1_name)
            process_col(cols[1], t2_name)
            
            print(f"   ✅ Processed {t1_name} & {t2_name}")
            
            conn.commit()
            time.sleep(1.0)
            
        except Exception as e:
            print(f"❌ Error processing {match_id}: {e}")

    conn.close()
    print("Done.")

if __name__ == "__main__":
    scrape_squads()
