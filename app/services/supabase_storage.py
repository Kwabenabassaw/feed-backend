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
            return False
        
        url = f"{self.url}/storage/v1/object/{bucket}/{filename}"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    content=content,
                    headers={
                        **self._get_headers(),
                        "Content-Type": content_type,
                        "x-upsert": "true",  # Overwrite if exists
                    },
                    timeout=30.0
                )
                
                if response.status_code in (200, 201):
                    logger.info(
                        "supabase_upload_success",
                        bucket=bucket,
                        filename=filename,
                        size=len(content)
                    )
                    return True
                else:
                    logger.error(
                        "supabase_upload_failed",
                        bucket=bucket,
                        filename=filename,
                        status=response.status_code,
                        error=response.text
                    )
                    return False
                    
        except Exception as e:
            logger.error("supabase_upload_error", error=str(e))
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
        if not self._is_configured():
            logger.warning("supabase_not_configured", action="upload_all")
            return {"success": 0, "failed": 0, "skipped": "not_configured"}
        
        if not self.local_indexes_path.exists():
            logger.warning("indexes_directory_not_found")
            return {"success": 0, "failed": 0, "skipped": "no_directory"}
        
        success = 0
        failed = 0
        
        for filepath in self.local_indexes_path.glob("*.json"):
            index_name = filepath.stem
            
            try:
                content = filepath.read_bytes()
                result = await self.upload_file(
                    bucket=self.INDEX_BUCKET,
                    filename=filepath.name,
                    content=content
                )
                
                if result:
                    success += 1
                else:
                    failed += 1
                    
            except Exception as e:
                logger.error(
                    "index_upload_error",
                    index=index_name,
                    error=str(e)
                )
                failed += 1
        
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
