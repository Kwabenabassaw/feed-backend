"""
Simple test for fetch_tmdb_released_today method - outputs to file.
"""

import asyncio
import os
import sys
import json
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.jobs.ingestion import IngestionJob


async def test_released_today():
    """Test the fetch_tmdb_released_today method."""
    today = date.today().isoformat()
    
    output = []
    output.append("="*60)
    output.append(f"Testing fetch_tmdb_released_today() for {today}")
    output.append("="*60)
    
    job = IngestionJob()
    
    try:
        results = await job.fetch_tmdb_released_today()
        
        output.append(f"\n✅ Successfully fetched {len(results)} trailers released today")
        
        if results:
            output.append("\nSample results (first 5):")
            for i, item in enumerate(results[:5], 1):
                output.append(f"\n{i}. {item.get('title', 'Unknown')}")
                output.append(f"   - Media Type: {item.get('mediaType', 'unknown')}")
                output.append(f"   - YouTube Key: {item.get('youtubeKey', 'N/A')}")
                output.append(f"   - Release Date: {item.get('releaseDate', 'N/A')}")
                output.append(f"   - Video Type: {item.get('videoType', 'N/A')}")
                output.append(f"   - TMDB ID: {item.get('tmdbId', 'N/A')}")
                output.append(f"   - Source: {item.get('source', 'N/A')}")
        else:
            output.append("\nℹ️  No content found released today.")
            output.append("   This is normal - new releases don't happen every day!")
        
        # Write full results to JSON
        with open("test_results.json", "w") as f:
            json.dump(results, f, indent=2)
        output.append(f"\n\nFull results saved to test_results.json ({len(results)} items)")
        
    except Exception as e:
        output.append(f"\n❌ Error: {e}")
        import traceback
        output.append(traceback.format_exc())
        
        # Write to file
        with open("test_output.txt", "w") as f:
            f.write("\n".join(output))
        return False
    
    output.append("\n" + "="*60)
    output.append("Test completed successfully!")
    output.append("="*60)
    
    # Write to file
    with open("test_output.txt", "w") as f:
        f.write("\n".join(output))
    
    print("\n".join(output))
    return True


if __name__ == "__main__":
    asyncio.run(test_released_today())
