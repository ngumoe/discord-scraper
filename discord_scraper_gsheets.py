import aiohttp
import asyncio
import json
import time
import os
from typing import List, Dict, Optional
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_dotenv()

class DiscordScraper:
    def __init__(self, auth_token: str):
        self.auth_token = auth_token
        self.base_url = "https://discord.com/api/v9"
        self.rate_limit_remaining = 5
        self.rate_limit_reset = 0
        
    async def fetch_messages(self, channel_id: str, max_messages: Optional[int] = None) -> List[Dict]:
        """Fetches messages from a specific channel with rate limit handling"""
        messages = []
        before_id = None
        url = f"{self.base_url}/channels/{channel_id}/messages"
        
        while True:
            if self.rate_limit_remaining <= 0:
                current_time = time.time()
                sleep_time = max(0, self.rate_limit_reset - current_time)
                if sleep_time > 0:
                    print(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                    await asyncio.sleep(sleep_time)
            
            params = {"limit": 100}
            if before_id:
                params["before"] = before_id
                
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": self.auth_token}
                try:
                    async with session.get(url, params=params, headers=headers) as response:
                        self.rate_limit_remaining = int(response.headers.get("X-RateLimit-Remaining", 5))
                        reset_after = response.headers.get("X-RateLimit-Reset-After")
                        if reset_after:
                            self.rate_limit_reset = time.time() + float(reset_after)
                        
                        if response.status == 200:
                            new_messages = await response.json()
                            if not new_messages:
                                break
                                
                            messages.extend(new_messages)
                            before_id = new_messages[-1]["id"]
                            
                            print(f"Fetched {len(new_messages)} messages from channel {channel_id}")
                            
                            if max_messages and len(messages) >= max_messages:
                                messages = messages[:max_messages]
                                break
                        elif response.status == 429:
                            retry_after = float(response.headers.get("Retry-After", 5))
                            print(f"Rate limited, retrying after {retry_after} seconds")
                            await asyncio.sleep(retry_after)
                            continue
                        else:
                            print(f"Error fetching messages: {response.status}")
                            break
                except Exception as e:
                    print(f"Request failed: {e}")
                    break
        
        return messages

class KeywordFilter:
    def __init__(self, keywords: List[str]):
        self.keywords = [k.lower() for k in keywords]
        
    def filter_messages(self, messages: List[Dict]) -> List[Dict]:
        """Filters messages based on predefined keywords"""
        filtered_messages = []
        
        for message in messages:
            content = message.get("content", "").lower()
            if any(keyword in content for keyword in self.keywords):
                matched_keywords = [k for k in self.keywords if k in content]
                message["matched_keywords"] = matched_keywords
                filtered_messages.append(message)
                print(f"DEBUG: Found message with keywords: {content[:100]}...")
                
        return filtered_messages

class GoogleSheetsManager:
    def __init__(self, credentials_json: str, sheet_id: str):
        try:
            print("DEBUG: Initializing Google Sheets connection...")
            # Parse the credentials from environment variable
            creds_dict = json.loads(credentials_json)
            creds = Credentials.from_service_account_info(creds_dict)
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open_by_key(sheet_id).sheet1
            print("DEBUG: Successfully connected to Google Sheets")
            
            # Ensure headers exist
            if not self.sheet.get_all_records():
                headers = ["Message ID", "Timestamp", "Author", "Content", "Keywords", "Channel ID", "Unique Hash"]
                self.sheet.append_row(headers)
                print("DEBUG: Added headers to Google Sheets")
            else:
                print("DEBUG: Headers already exist in Google Sheets")
                
        except Exception as e:
            print(f"ERROR: Failed to initialize Google Sheets: {e}")
            raise
    
    def message_exists(self, message_id: str) -> bool:
        """Check if a message already exists in the sheet"""
        try:
            # Search for message ID in column A
            cell = self.sheet.find(message_id, in_column=1)
            exists = cell is not None
            if exists:
                print(f"DEBUG: Message {message_id} already exists in sheet")
            return exists
        except gspread.exceptions.CellNotFound:
            return False
        except Exception as e:
            print(f"ERROR checking if message exists: {e}")
            return False
    
    def add_message(self, message: Dict):
        """Add a new message to the sheet if it doesn't exist"""
        try:
            message_id = message.get("id")
            if not message_id:
                print("ERROR: Message has no ID")
                return False
            
            # Check if message already exists
            if self.message_exists(message_id):
                print(f"DEBUG: Message {message_id} already exists, skipping")
                return False
            
            # Prepare row data
            row = [
                message_id,
                message.get("timestamp", ""),
                message.get("author", {}).get("username", "Unknown"),
                message.get("content", "")[:500],  # Limit content length
                ", ".join(message.get("matched_keywords", [])),
                message.get("channel_id", ""),
                f"{message_id}_{message.get('channel_id', '')}"  # Unique hash
            ]
            
            # Add to sheet
            self.sheet.append_row(row)
            print(f"DEBUG: Added new message to sheet: {message_id}")
            return True
            
        except Exception as e:
            print(f"ERROR adding message to sheet: {e}")
            return False

def extract_channel_id(channel_url: str) -> str:
    """Extracts channel ID from Discord URL"""
    if "/channels/" in channel_url:
        parts = channel_url.split("/")
        return parts[-1]
    return channel_url

async def main():
    # Configuration with debug info
    AUTH_TOKEN = os.getenv("DISCORD_TOKEN")
    CHANNEL_URLS_STR = os.getenv("CHANNEL_URLS", "")
    CHANNEL_URLS = [url.strip() for url in CHANNEL_URLS_STR.split(",") if url.strip()]
    MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "100"))
    GOOGLE_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    
    print("=" * 50)
    print("DEBUG: Starting Discord Scraper")
    print("=" * 50)
    print(f"DEBUG: AUTH_TOKEN exists: {AUTH_TOKEN is not None}")
    print(f"DEBUG: CHANNEL_URLS: {CHANNEL_URLS}")
    print(f"DEBUG: MAX_MESSAGES: {MAX_MESSAGES}")
    print(f"DEBUG: GOOGLE_CREDENTIALS exists: {GOOGLE_CREDENTIALS is not None}")
    print(f"DEBUG: SHEET_ID: {SHEET_ID}")
    
    # Validate required environment variables
    if not AUTH_TOKEN:
        print("ERROR: DISCORD_TOKEN environment variable not set")
        return
        
    if not CHANNEL_URLS:
        print("ERROR: CHANNEL_URLS environment variable not set or empty")
        return
        
    if not GOOGLE_CREDENTIALS:
        print("ERROR: GOOGLE_SHEETS_CREDENTIALS environment variable not set")
        return
        
    if not SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID environment variable not set")
        return
    
    # Tutoring keywords to search for
    TUTORING_KEYWORDS = [
        "due tonight",
        "urgent",
        "deadline", 
        "exam tomorrow",
        "stuck on",
        "need help asap",
        "failing"
    ]
    
    print(f"DEBUG: Searching for keywords: {TUTORING_KEYWORDS}")
    
    # Initialize components
    scraper = DiscordScraper(AUTH_TOKEN)
    keyword_filter = KeywordFilter(TUTORING_KEYWORDS)
    
    try:
        sheets_manager = GoogleSheetsManager(GOOGLE_CREDENTIALS, SHEET_ID)
    except Exception as e:
        print(f"ERROR: Failed to initialize Google Sheets manager: {e}")
        return
    
    new_messages_count = 0
    total_messages_processed = 0
    
    # Process each channel
    for channel_url in CHANNEL_URLS:
        channel_id = extract_channel_id(channel_url)
        print(f"\nDEBUG: Scraping channel {channel_id}...")
        
        try:
            messages = await scraper.fetch_messages(channel_id, MAX_MESSAGES)
            print(f"DEBUG: Fetched {len(messages)} total messages from channel {channel_id}")
            
            filtered_messages = keyword_filter.filter_messages(messages)
            print(f"DEBUG: Found {len(filtered_messages)} messages with keywords in channel {channel_id}")
            total_messages_processed += len(filtered_messages)
            
            # Add new messages to Google Sheets
            for message in filtered_messages:
                if sheets_manager.add_message(message):
                    new_messages_count += 1
            
        except Exception as e:
            print(f"ERROR processing channel {channel_id}: {e}")
    
    print("\n" + "=" * 50)
    print("DEBUG: Scraping complete")
    print(f"DEBUG: Processed {total_messages_processed} total messages with keywords")
    print(f"DEBUG: Added {new_messages_count} new messages to Google Sheets")
    print("=" * 50)

if __name__ == "__main__":
    asyncio.run(main())