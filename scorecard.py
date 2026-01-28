import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re

MATCH_IDS = [
    116441, 121389, 121400, 121406, 133000, 133011, 133017, 
    137826, 137831, 140537, 140548, 140559
]

DB_PATH = "cricbuzz.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Batter Scorecard Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS batter_scorecard (
        match_id TEXT,
        player_id INTEGER,
        player_name TEXT,
        R INTEGER,
        B INTEGER,
        fours INTEGER,
        sixes INTEGER,
        SR REAL,
        FOREIGN KEY (match_id) REFERENCES sports_match_records(match_id)
    )
    """)
    
    # Bowler Scorecard Table
    # Requested columns: O,M,R,W,NB,WB,ECO
    # Using 'WB' for wides as requested
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bowler_scorecard (
        match_id TEXT,
        player_id INTEGER,
        player_name TEXT,
        O REAL,
        M INTEGER,
        R INTEGER,
        W INTEGER,
        NB INTEGER,
        WB INTEGER,
        ECO REAL,
        FOREIGN KEY (match_id) REFERENCES sports_match_records(match_id)
    )
    """)
    
    conn.commit()
    conn.close()

def clean_float(val):
    try:
        return float(val)
    except:
        return 0.0

def clean_int(val):
    try:
        return int(val)
    except:
        return 0

def scrape_scorecards():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    for match_id in MATCH_IDS:
        print(f"Details for Match ID: {match_id}...")
        
        url = f"https://www.cricbuzz.com/live-cricket-scorecard/{match_id}/match"
        
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                print(f"❌ Failed to fetch page. Status: {r.status_code}")
                # Try fallback just in case
                url2 = f"https://www.cricbuzz.com/live-cricket-scorecard/{match_id}/scorecard"
                r = requests.get(url2, headers=headers, timeout=15)
                if r.status_code != 200: continue
                
            soup = BeautifulSoup(r.text, "html.parser")
            
            # Clear existing data
            cursor.execute("DELETE FROM batter_scorecard WHERE match_id=?", (str(match_id),))
            cursor.execute("DELETE FROM bowler_scorecard WHERE match_id=?", (str(match_id),))
            
            # The new layout uses "grid" classes.
            # We search for rows directly.
            
            # --- BATTING ---
            bat_rows = soup.find_all("div", class_=re.compile(r"scorecard-bat-grid"))
            bat_count = 0
            for row in bat_rows:
                # Skip header row (contains "Batter")
                if "Batter" in row.get_text():
                    continue
                
                # Each row follows a flexible grid structure
                # We can rely on recursive children or direct children.
                # Since utility classes clutter things, let's grab all text nodes or specific children.
                # However, identifying columns by position is safer if we just grab direct children divs.
                
                cols = row.find_all("div", recursive=False)
                # Structure:
                # 0: Name + Dismissal (Nested)
                # 1: R
                # 2: B
                # 3: 4s
                # 4: 6s
                # 5: SR
                # 6+: Icon etc.
                
                if len(cols) < 6: continue
                
                # Check for Name
                name_col = cols[0]
                link = name_col.find("a", href=re.compile(r"/profiles/"))
                if not link: continue # Probably Extras or Total row
                
                p_name = link.get_text().strip()
                href = link['href']
                m = re.search(r"/profiles/(\d+)/", href)
                p_id = int(m.group(1)) if m else 0
                
                # Extract numbers
                # Text usually inside these cols
                r_val = clean_int(cols[1].get_text())
                b_val = clean_int(cols[2].get_text())
                fours = clean_int(cols[3].get_text())
                sixes = clean_int(cols[4].get_text())
                sr = clean_float(cols[5].get_text())
                
                cursor.execute("""
                    INSERT INTO batter_scorecard (match_id, player_id, player_name, R, B, fours, sixes, SR)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (str(match_id), p_id, p_name, r_val, b_val, fours, sixes, sr))
                bat_count += 1

            # --- BOWLING ---
            bowl_rows = soup.find_all("div", class_=re.compile(r"scorecard-bowl-grid"))
            bowl_count = 0
            for row in bowl_rows:
                if "Bowler" in row.get_text():
                    continue
                
                # Bowling rows have 'a' tag as direct child for name, then 'divs' for stats
                # So we get all direct children regardless of tag type
                cols = row.find_all(recursive=False)
                
                # Structure:
                # 0: Name (Link)
                # 1: O
                # 2: M
                # 3: R
                # 4: W
                # 5: NB
                # 6: WD
                # 7: ECO
                # 8+: Icon
                
                if len(cols) < 8: continue
                
                name_col = cols[0]
                # If name_col is the 'a' tag itself
                if name_col.name == 'a':
                    link = name_col
                else:
                     # Fallback in case it's wrapped
                    link = name_col.find("a", href=re.compile(r"/profiles/"))
                
                if not link: continue
                
                p_name = link.get_text().strip()
                href = link['href']
                m = re.search(r"/profiles/(\d+)/", href)
                p_id = int(m.group(1)) if m else 0
                
                o_val = clean_float(cols[1].get_text())
                m_val = clean_int(cols[2].get_text())
                r_val = clean_int(cols[3].get_text())
                w_val = clean_int(cols[4].get_text())
                nb_val = clean_int(cols[5].get_text())
                wd_val = clean_int(cols[6].get_text()) # WB
                eco_val = clean_float(cols[7].get_text())
                
                cursor.execute("""
                    INSERT INTO bowler_scorecard (match_id, player_id, player_name, O, M, R, W, NB, WB, ECO)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (str(match_id), p_id, p_name, o_val, m_val, r_val, w_val, nb_val, wd_val, eco_val))
                bowl_count += 1
            
            conn.commit()
            print(f"   ✅ {match_id}: Batters={bat_count}, Bowlers={bowl_count}")
            time.sleep(1.0)
            
        except Exception as e:
            print(f"❌ Error processing {match_id}: {e}")

    conn.close()
    print("Done.")

if __name__ == "__main__":
    scrape_scorecards()
