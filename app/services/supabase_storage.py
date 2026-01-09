"""
Supabase Storage Service

Handles uploading index files to Supabase Storage buckets.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional
import httpx

from ..config import get_settings
from ..core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class SupabaseStorage:
    """
    Manages index file uploads to Supabase Storage.
    
    Buckets:
    - indexes/: Lightweight JSON indices (global_trending, genre_*)
    - content/: Master content dictionary
    """
    
    INDEX_BUCKET = "indexes"
    CONTENT_BUCKET = "content"
    
    def __init__(self):
        self.url = settings.supabase_url
        self.key = settings.supabase_key
        self.local_indexes_path = Path("indexes")
    
    def _is_configured(self) -> bool:
        """Check if Supabase credentials are configured."""
        return bool(self.url and self.key)
    
    def _get_headers(self) -> dict:
        """Get authorization headers."""
        return {
            "Authorization": f"Bearer {self.key}",
            "apikey": self.key,
        }
    
    async def upload_file(
        self, 
        bucket: str, 
        filename: str, 
        content: bytes,
        content_type: str = "application/json"
    ) -> bool:
        """
        Upload a file to Supabase Storage.
        
        Args:
            bucket: Storage bucket name
            filename: Target filename
            content: File content as bytes
            content_type: MIME type
            
        Returns:
            True if successful, False otherwise
        """
        if not self._is_configured():
            logger.warning("supabase_not_configured", action="upload")
            print(f"[Supabase] ❌ Not configured - URL: {self.url}, Key: {'***set***' if self.key else 'NOT SET'}")
            return False
        
        # Supabase Storage API endpoint
        url = f"{self.url}/storage/v1/object/{bucket}/{filename}"
        
        print(f"[Supabase] 📤 Uploading {filename} to {bucket}...")
        print(f"[Supabase] URL: {url}")
        
        try:
            async with httpx.AsyncClient() as client:
                # Use PUT for upsert (create or replace)
                response = await client.put(
                    url,
                    content=content,
                    headers={
                        **self._get_headers(),
                        "Content-Type": content_type,
                        "x-upsert": "true",  # Overwrite if exists
                    },
                    timeout=30.0
                )
                
                print(f"[Supabase] Response: {response.status_code}")
                
                if response.status_code in (200, 201):
                    logger.info(
                        "supabase_upload_success",
                        bucket=bucket,
                        filename=filename,
                        size=len(content)
                    )
                    print(f"[Supabase] ✅ Uploaded {filename} ({len(content)} bytes)")
                    return True
                else:
                    error_text = response.text
                    logger.error(
                        "supabase_upload_failed",
                        bucket=bucket,
                        filename=filename,
                        status=response.status_code,
                        error=error_text
                    )
                    print(f"[Supabase] ❌ Failed: {response.status_code} - {error_text}")
                    return False
                    
        except Exception as e:
            logger.error("supabase_upload_error", error=str(e))
            print(f"[Supabase] ❌ Exception: {e}")
            return False
    
    async def upload_index(self, index_name: str) -> bool:
        """
        Upload a single index file.
        
        Args:
            index_name: Name without extension (e.g., "global_trending")
        """
        filepath = self.local_indexes_path / f"{index_name}.json"
        
        if not filepath.exists():
            logger.warning("index_file_not_found", path=str(filepath))
            return False
        
        content = filepath.read_bytes()
        return await self.upload_file(
            bucket=self.INDEX_BUCKET,
            filename=f"{index_name}.json",
            content=content
        )
    
    async def upload_all_indices(self) -> dict:
        """
        Upload all index files to Supabase.
        
        Returns:
            Dict with success/failure counts
        """
        print("\n" + "="*60)
        print("[Supabase Upload] 🚀 STARTING UPLOAD TO SUPABASE STORAGE")
        print("="*60)
        
        if not self._is_configured():
            logger.warning("supabase_not_configured", action="upload_all")
            print(f"[Supabase Upload] ❌ NOT CONFIGURED!")
            print(f"[Supabase Upload]    URL: {self.url or 'NOT SET'}")
            print(f"[Supabase Upload]    Key: {'***set***' if self.key else 'NOT SET'}")
            return {"success": 0, "failed": 0, "skipped": "not_configured"}
        
        print(f"[Supabase Upload] ✅ Credentials configured")
        print(f"[Supabase Upload]    URL: {self.url}")
        print(f"[Supabase Upload]    Bucket: {self.INDEX_BUCKET}")
        
        if not self.local_indexes_path.exists():
            logger.warning("indexes_directory_not_found")
            print(f"[Supabase Upload] ❌ Indexes directory not found: {self.local_indexes_path}")
            return {"success": 0, "failed": 0, "skipped": "no_directory"}
        
        # List files to upload
        files = list(self.local_indexes_path.glob("*.json"))
        print(f"[Supabase Upload] 📁 Found {len(files)} files to upload:")
        for f in files:
            print(f"[Supabase Upload]    - {f.name} ({f.stat().st_size} bytes)")
        
        success = 0
        failed = 0
        
        for filepath in files:
            index_name = filepath.stem
            
            try:
                content = filepath.read_bytes()
                print(f"\n[Supabase Upload] 📤 Uploading {filepath.name}...")
                
                result = await self.upload_file(
                    bucket=self.INDEX_BUCKET,
                    filename=filepath.name,
                    content=content
                )
                
                if result:
                    success += 1
                    print(f"[Supabase Upload] ✅ {filepath.name} uploaded successfully")
                else:
                    failed += 1
                    print(f"[Supabase Upload] ❌ {filepath.name} failed")
                    
            except Exception as e:
                logger.error(
                    "index_upload_error",
                    index=index_name,
                    error=str(e)
                )
                print(f"[Supabase Upload] ❌ {filepath.name} exception: {e}")
                failed += 1
        
        print("\n" + "="*60)
        print(f"[Supabase Upload] 📊 UPLOAD COMPLETE")
        print(f"[Supabase Upload]    ✅ Success: {success}")
        print(f"[Supabase Upload]    ❌ Failed:  {failed}")
        print(f"[Supabase Upload]    📅 Time:    {datetime.utcnow().isoformat()}")
        print("="*60 + "\n")
        
        logger.info(
            "upload_all_complete",
            success=success,
            failed=failed,
            timestamp=datetime.utcnow().isoformat()
        )
        
        return {"success": success, "failed": failed}
    
    async def download_index(self, index_name: str) -> Optional[list]:
        """
        Download an index from Supabase.
        
        Args:
            index_name: Name without extension
            
        Returns:
            List of index items or None if failed
        """
        if not self._is_configured():
            return None
        
        url = f"{self.url}/storage/v1/object/public/{self.INDEX_BUCKET}/{index_name}.json"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=10.0)
                
                if response.status_code == 200:
                    return response.json()
                    
        except Exception as e:
            logger.warning("supabase_download_failed", index=index_name, error=str(e))
        
        return None


# Singleton instance
_supabase_storage: Optional[SupabaseStorage] = None


def get_supabase_storage() -> SupabaseStorage:
    """Get singleton SupabaseStorage instance."""
    global _supabase_storage
    if _supabase_storage is None:
        _supabase_storage = SupabaseStorage()
    return _supabase_storage
