
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

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create Match Awards Table without Foreign Keys for now
    # to avoid issues if the referenced match/player doesn't exist in our partial DB
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS match_awards (
        match_id TEXT,
        player_id INTEGER,
        player_name TEXT,
        award_name TEXT DEFAULT 'Player of the Match'
    )
    """)
    
    conn.commit()
    conn.close()

def get_player_of_the_match(soup):
    """
    Robust strategy to find Player of the Match.
    Look for "PLAYER OF THE MATCH" text, then find the nearest profile link.
    """
    # Strategy 1: Search for the text directly
    # This text is usually in a label or span
    pom_text_nodes = soup.find_all(string=re.compile(r"PLAYER OF THE MATCH", re.I))
    
    for node in pom_text_nodes:
        # The node is a NavigableString. We want to check its container and nearby elements.
        # Usually structure is:
        # <div class="cb-mo-ply-id">
        #    <span class="cb-text-gray">PLAYER OF THE MATCH</span>
        #    <a href="/profiles/123/name" class="cb-link-undrln">Name</a>
        # </div>
        
        # Check parent container
        parent = node.parent
        if not parent: continue
        
        # Traverse up a few levels to find a container that might hold the link
        # Usually it's the direct parent or grandparent
        container = parent
        for _ in range(3): # Check parent, grandparent, great-grandparent
            if not container: break
            
            link = container.find("a", href=re.compile(r"/profiles/"))
            if link:
                return link
            container = container.parent
            
    return None

def scrape_awards():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    for match_id in MATCH_IDS:
        print(f"Processing Match ID: {match_id}...")
        
        url = f"https://www.cricbuzz.com/live-cricket-scores/{match_id}/match"
        
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200:
                print(f"❌ Failed to fetch page. Status: {r.status_code}")
                # Try fallback url structure just in case redirect fails
                # url = f"https://www.cricbuzz.com/cricket-scores/{match_id}/match"
                # r = requests.get(url, headers=headers, timeout=10)
                continue
                
            soup = BeautifulSoup(r.text, "html.parser")
            
            link = get_player_of_the_match(soup)
            
            if link:
                href = link['href']
                p_name = link.get_text().strip()
                
                # Extract ID from /profiles/123/name
                m = re.search(r"/profiles/(\d+)/", href)
                p_id = int(m.group(1)) if m else None
                
                if p_id:
                    print(f"   ✅ Found: {p_name} ({p_id})")
                    
                    # Clean up existing entry for this match/award
                    cursor.execute("""
                        DELETE FROM match_awards 
                        WHERE match_id=? AND award_name='Player of the Match'
                    """, (str(match_id),))
                    
                    cursor.execute("""
                        INSERT INTO match_awards (match_id, player_id, player_name)
                        VALUES (?, ?, ?)
                    """, (str(match_id), p_id, p_name))
                    
                    conn.commit()
                else:
                    print(f"   ⚠️ Found name {p_name} but could not extract ID from {href}")
            else:
                print(f"   ⚠️ Match {match_id}: 'Player of the Match' NOT found in page.")
            
            time.sleep(1.0)
            
        except Exception as e:
            print(f"❌ Error processing {match_id}: {e}")

    conn.close()
    print("Done.")

if __name__ == "__main__":
    scrape_awards()
