import aiohttp
import asyncio
import json
import time
import os
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
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
            # Respect rate limits
            if self.rate_limit_remaining <= 0:
                current_time = time.time()
                sleep_time = max(0, self.rate_limit_reset - current_time)
                if sleep_time > 0:
                    print(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                    await asyncio.sleep(sleep_time)
            
            # Prepare request parameters
            params = {"limit": 100}
            if before_id:
                params["before"] = before_id
                
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": self.auth_token}
                try:
                    async with session.get(url, params=params, headers=headers) as response:
                        # Update rate limit information
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
                            # Rate limited, wait and retry
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
                # Add which keywords were matched
                matched_keywords = [k for k in self.keywords if k in content]
                message["matched_keywords"] = matched_keywords
                filtered_messages.append(message)
                
        return filtered_messages

def extract_channel_id(channel_url: str) -> str:
    """Extracts channel ID from Discord URL"""
    if "/channels/" in channel_url:
        parts = channel_url.split("/")
        return parts[-1]
    return channel_url

async def main():
    # Configuration - Get values from environment variables
    AUTH_TOKEN = os.getenv("DISCORD_TOKEN")
    CHANNEL_URLS = os.getenv("CHANNEL_URLS", "").split(",")
    MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "100"))
    
    # Debug: Check if environment variables are loading
    print("DEBUG: DISCORD_TOKEN exists:", AUTH_TOKEN is not None)
    print("DEBUG: CHANNEL_URLS:", CHANNEL_URLS)
    
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
    
    if not AUTH_TOKEN:
        print("Error: DISCORD_TOKEN environment variable not set")
        return
        
    if not CHANNEL_URLS or not CHANNEL_URLS[0]:
        print("Error: CHANNEL_URLS environment variable not set")
        return
    
    # Initialize scraper and filter
    scraper = DiscordScraper(AUTH_TOKEN)
    keyword_filter = KeywordFilter(TUTORING_KEYWORDS)
    
    all_filtered_messages = []
    
    # Process each channel
    for channel_url in CHANNEL_URLS:
        if not channel_url.strip():
            continue
            
        channel_id = extract_channel_id(channel_url.strip())
        print(f"Scraping channel {channel_id}...")
        
        try:
            messages = await scraper.fetch_messages(channel_id, MAX_MESSAGES)
            filtered_messages = keyword_filter.filter_messages(messages)
            all_filtered_messages.extend(filtered_messages)
            
            print(f"Found {len(filtered_messages)} relevant messages in {channel_id}")
        except Exception as e:
            print(f"Error processing channel {channel_id}: {e}")
    
    # Save results
    output = {
        "metadata": {
            "timestamp": time.time(),
            "channel_count": len(CHANNEL_URLS),
            "total_messages_found": len(all_filtered_messages)
        },
        "messages": all_filtered_messages
    }
    
    with open("discord_messages.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"Scraping complete. Found {len(all_filtered_messages)} messages with keywords.")
    print("Results saved to discord_messages.json")

if __name__ == "__main__":
    asyncio.run(main())