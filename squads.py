
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
    
    # Create Players Table with extended personal info
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS players (
        player_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT,
        birth_date TEXT,
        birth_place TEXT,
        nickname TEXT,
        height TEXT,
        batting_style TEXT,
        bowling_style TEXT
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

def fetch_player_profile(player_id, headers):
    """
    Fetches a player's profile page and extracts personal info.
    Returns a dict with: birth_date, birth_place, nickname, height, batting_style, bowling_style
    """
    profile_url = f"https://www.cricbuzz.com/profiles/{player_id}/player"
    
    profile_info = {
        "birth_date": None,
        "birth_place": None,
        "nickname": None,
        "height": None,
        "batting_style": None,
        "bowling_style": None
    }
    
    try:
        r = requests.get(profile_url, headers=headers)
        if r.status_code != 200:
            print(f"     ⚠️ Could not fetch profile for player {player_id}")
            return profile_info
        
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Find all divs that could contain personal info
        # The structure is typically: label div followed by value div
        # Labels include: Born, Birth Place, Nickname, Height, Role, Batting Style, Bowling Style
        
        # Map of field labels to keys in our dict
        field_mapping = {
            "Born": "birth_date",
            "Birth Place": "birth_place",
            "Nickname": "nickname",
            "Height": "height",
            "Batting Style": "batting_style",
            "Bowling Style": "bowling_style"
        }
        
        # Find all text on page and look for label patterns
        all_divs = soup.find_all("div")
        
        for div in all_divs:
            text = div.get_text(strip=True)
            
            # Check if this div contains exactly one of our labels
            for label, key in field_mapping.items():
                if text == label:
                    # The value is typically in a sibling div
                    parent = div.parent
                    if parent:
                        children = parent.find_all("div", recursive=False)
                        for i, child in enumerate(children):
                            if child.get_text(strip=True) == label and i + 1 < len(children):
                                value = children[i + 1].get_text(strip=True)
                                if value and value != label:
                                    profile_info[key] = value
                                    break
                        # Also check if value is in next sibling
                        if not profile_info[key] and div.next_sibling:
                            next_el = div.find_next_sibling("div")
                            if next_el:
                                value = next_el.get_text(strip=True)
                                if value and value not in field_mapping.keys():
                                    profile_info[key] = value
                    break
        
    except Exception as e:
        print(f"     ⚠️ Error fetching profile for player {player_id}: {e}")
    
    return profile_info

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
                        
                        # Check if player already exists in database
                        cursor.execute("SELECT player_id FROM players WHERE player_id = ?", (p_id,))
                        existing = cursor.fetchone()
                        
                        if not existing:
                            # New player - fetch profile details
                            print(f"     📥 Fetching profile for {name} (ID: {p_id})...")
                            profile = fetch_player_profile(p_id, headers)
                            time.sleep(0.5)  # Rate limiting for profile fetches
                            
                            # Insert Player with full profile info
                            cursor.execute("""
                                INSERT INTO players (
                                    player_id, name, role, 
                                    birth_date, birth_place, nickname, 
                                    height, batting_style, bowling_style
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                p_id, name, role,
                                profile["birth_date"], profile["birth_place"], profile["nickname"],
                                profile["height"], profile["batting_style"], profile["bowling_style"]
                            ))
                        else:
                            # Player exists - just update name and role if needed
                            cursor.execute("""
                                UPDATE players SET name = ?, role = ?
                                WHERE player_id = ?
                            """, (name, role, p_id))
                        
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
