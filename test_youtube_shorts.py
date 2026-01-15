"""
Test script for YouTube Shorts API integration.
"""

import asyncio
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.youtube_api import YouTubeAPIService, SHORTS_CHANNELS


async def test_youtube_shorts():
    """Test fetching YouTube Shorts from movie recap channels."""
    print("\n" + "="*60)
    print("Testing YouTube Shorts API Integration")
    print("="*60)
    
    service = YouTubeAPIService()
    
    if not service.api_key:
        print("\n❌ YOUTUBE_API_KEY not set in environment!")
        print("   Add YOUTUBE_API_KEY to your .env file")
        return False
    
    print(f"\n📺 Channels configured: {len(SHORTS_CHANNELS)}")
    for channel_id, name in SHORTS_CHANNELS.items():
        print(f"   - {name}: {channel_id}")
    
    print("\n🔍 Fetching Shorts from first channel...")
    
    # Test single channel first
    first_channel_id = list(SHORTS_CHANNELS.keys())[0]
    first_channel_name = SHORTS_CHANNELS[first_channel_id]
    
    try:
        shorts = await service.fetch_channel_shorts(first_channel_id, max_results=3)
        
        print(f"\n✅ Fetched {len(shorts)} Shorts from {first_channel_name}")
        
        if shorts:
            print("\nSample results:")
            for i, short in enumerate(shorts[:3], 1):
                print(f"\n{i}. {short.get('title', 'Unknown')[:60]}...")
                print(f"   - Duration: {short.get('durationSeconds', 0)}s")
                print(f"   - Views: {short.get('viewCount', 0):,}")
                print(f"   - Likes: {short.get('likeCount', 0):,}")
                print(f"   - YouTube Key: {short.get('youtubeKey', 'N/A')}")
        
        # Save results
        with open("test_shorts_output.json", "w") as f:
            json.dump(shorts, f, indent=2)
        print(f"\n📁 Results saved to test_shorts_output.json")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "="*60)
    print("Test completed successfully!")
    print("="*60)
    return True


if __name__ == "__main__":
    asyncio.run(test_youtube_shorts())
