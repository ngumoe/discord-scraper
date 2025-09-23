import aiohttp
import asyncio
import json
import time
import os
from typing import List, Dict, Optional
import gspread
from google.oauth2.service_account import Credentials

# Read configuration from environment variables
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
CHANNEL_URLS_STR = os.environ.get("CHANNEL_URLS", "")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "100"))
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")

print("=" * 50)
print("DEBUG: Starting Discord Scraper")
print("=" * 50)
print(f"DEBUG: DISCORD_TOKEN exists: {DISCORD_TOKEN is not None}")
print(f"DEBUG: CHANNEL_URLS_STR: {CHANNEL_URLS_STR[:100]}...")
print(f"DEBUG: MAX_MESSAGES: {MAX_MESSAGES}")
print(f"DEBUG: GOOGLE_SHEET_ID exists: {GOOGLE_SHEET_ID is not None}")

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
    def __init__(self, sheet_id: str):
        try:
            print("DEBUG: Initializing Google Sheets connection...")
            
            # Read credentials from file
            with open('google_credentials.json', 'r') as f:
                credentials_json = f.read().strip()
            
            print(f"DEBUG: First 100 chars of credentials: {credentials_json[:100]}...")
            
            # Parse the credentials from JSON string with explicit scopes
            creds_dict = json.loads(credentials_json)
            
            # Explicitly define the required scopes
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
            
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open_by_key(sheet_id).sheet1
            print("DEBUG: Successfully connected to Google Sheets")
            
            # Ensure headers exist with NEW COLUMNS for server/channel info
            if not self.sheet.get_all_records():
                headers = [
                    "Message ID", "Timestamp", "Author", "Content", "Keywords", 
                    "Channel ID", "Server ID", "Channel Name", "Server Name",
                    "Response Status", "Response Sent", "Response Content"
                ]
                self.sheet.append_row(headers)
                print("DEBUG: Added headers to Google Sheets")
            else:
                print("DEBUG: Headers already exist in Google Sheets - NEW MESSAGES WILL HAVE EXTRA COLUMNS")
                
        except json.JSONDecodeError as e:
            print(f"ERROR: Failed to parse Google credentials JSON: {e}")
            print("DEBUG: Credentials content (first 200 chars):", credentials_json[:200])
            raise
        except Exception as e:
            print(f"ERROR: Failed to initialize Google Sheets: {e}")
            raise
    
    def message_exists(self, message_id: str) -> bool:
        """Check if a message already exists in the sheet"""
        try:
            # find() returns None if not found, doesn't raise an exception
            cell = self.sheet.find(message_id, in_column=1)
            exists = cell is not None
            if exists:
                print(f"DEBUG: Message {message_id} already exists in sheet")
            else:
                print(f"DEBUG: Message {message_id} not found in sheet")
            return exists
        except Exception as e:
            print(f"ERROR checking if message exists: {e}")
            return False
    
    def add_message(self, message: Dict, channel_info: Dict = None):
        """Add a new message to the sheet if it doesn't exist"""
        try:
            message_id = message.get("id")
            if not message_id:
                print("ERROR: Message has no ID")
                return False
            
            if self.message_exists(message_id):
                print(f"DEBUG: Message {message_id} already exists, skipping")
                return False
            
            # NEW: Extract server ID from guild_id or use 'DM' for direct messages
            guild_id = message.get('guild_id', 'DM')
            
            # NEW: Prepare row data with server/channel info
            row = [
                message_id,
                message.get("timestamp", ""),
                message.get("author", {}).get("username", "Unknown"),
                message.get("content", "")[:500],
                ", ".join(message.get("matched_keywords", [])),
                message.get("channel_id", ""),
                guild_id,  # NEW: Server ID
                channel_info.get('name', 'Unknown') if channel_info else 'Unknown',  # NEW: Channel name
                channel_info.get('guild_name', 'DM') if channel_info else 'DM',  # NEW: Server name
                "Pending",  # NEW: Response Status
                "",  # NEW: Response Sent timestamp
                ""   # NEW: Response Content
            ]
            
            self.sheet.append_row(row)
            print(f"DEBUG: Added new message to sheet: {message_id}")
            return True
            
        except Exception as e:
            print(f"ERROR adding message to sheet: {e}")
            return False

# NEW FUNCTION: Get channel and server information
async def get_channel_info(channel_id: str, auth_token: str) -> Dict:
    """Get channel and server information"""
    url = f"https://discord.com/api/v9/channels/{channel_id}"
    headers = {"Authorization": auth_token}
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                channel_data = await response.json()
                info = {
                    'name': channel_data.get('name', 'Unknown'),
                    'guild_id': channel_data.get('guild_id', 'DM')
                }
                
                # If it's a guild channel, get server name
                if info['guild_id'] != 'DM':
                    guild_url = f"https://discord.com/api/v9/guilds/{info['guild_id']}"
                    async with session.get(guild_url, headers=headers) as guild_response:
                        if guild_response.status == 200:
                            guild_data = await guild_response.json()
                            info['guild_name'] = guild_data.get('name', 'Unknown')
                        else:
                            info['guild_name'] = 'Unknown'
                else:
                    info['guild_name'] = 'DM'
                
                return info
            else:
                return {'name': 'Unknown', 'guild_name': 'Unknown', 'guild_id': 'DM'}

def extract_channel_id(channel_url: str) -> str:
    """Extracts channel ID from Discord URL"""
    if "/channels/" in channel_url:
        parts = channel_url.split("/")
        return parts[-1]
    return channel_url

async def main():
    # Parse channel URLs
    CHANNEL_URLS_LIST = [url.strip() for url in CHANNEL_URLS_STR.split(",") if url.strip()]
    
    print(f"DEBUG: Parsed {len(CHANNEL_URLS_LIST)} channel URLs")
    
    # Validate configuration
    if not all([DISCORD_TOKEN, CHANNEL_URLS_LIST, GOOGLE_SHEET_ID]):
        missing = []
        if not DISCORD_TOKEN: missing.append("DISCORD_TOKEN")
        if not CHANNEL_URLS_LIST: missing.append("CHANNEL_URLS")
        if not GOOGLE_SHEET_ID: missing.append("GOOGLE_SHEET_ID")
        print(f"ERROR: Missing configuration: {missing}")
        return
    
    # Tutoring keywords
    TUTORING_KEYWORDS = [
        "due tonight", "urgent", "deadline", "exam tomorrow", 
        "stuck on", "assignment", "homework", "GIS tutor", "need help", "automation", "automation expert", "hiring", "remote", "need help asap", "failing"
    ]
    
    print(f"DEBUG: Searching for keywords: {TUTORING_KEYWORDS}")
    
    # Initialize components
    scraper = DiscordScraper(DISCORD_TOKEN)
    keyword_filter = KeywordFilter(TUTORING_KEYWORDS)
    
    try:
        sheets_manager = GoogleSheetsManager(GOOGLE_SHEET_ID)
    except Exception as e:
        print(f"ERROR: Failed to initialize Google Sheets: {e}")
        return
    
    new_messages_count = 0
    total_messages_processed = 0
    
    # Process each channel
    for channel_url in CHANNEL_URLS_LIST:
        channel_id = extract_channel_id(channel_url)
        print(f"\nDEBUG: Scraping channel {channel_id}...")
        
        try:
            # NEW: Get channel info first
            channel_info = await get_channel_info(channel_id, DISCORD_TOKEN)
            print(f"DEBUG: Channel: {channel_info.get('name')}, Server: {channel_info.get('guild_name')}")
            
            messages = await scraper.fetch_messages(channel_id, MAX_MESSAGES)
            print(f"DEBUG: Fetched {len(messages)} total messages")
            
            filtered_messages = keyword_filter.filter_messages(messages)
            print(f"DEBUG: Found {len(filtered_messages)} messages with keywords")
            total_messages_processed += len(filtered_messages)
            
            # NEW: Pass channel_info to add_message
            for message in filtered_messages:
                if sheets_manager.add_message(message, channel_info):
                    new_messages_count += 1
            
        except Exception as e:
            print(f"ERROR processing channel {channel_id}: {e}")
    
    print("\n" + "=" * 50)
    print(f"DEBUG: Added {new_messages_count} new messages to Google Sheets")
    print("=" * 50)

if __name__ == "__main__":
    asyncio.run(main())