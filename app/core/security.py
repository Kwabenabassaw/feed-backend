"""
Firebase Authentication Middleware

Verifies JWT tokens from Firebase Auth and extracts user information.
Supports credentials from:
1. Local file (service-account.json)
2. Environment variable (GOOGLE_APPLICATION_CREDENTIALS_JSON)
3. Default credentials (Google Cloud environments)
"""

import os
import json
import tempfile
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import firebase_admin
from firebase_admin import auth, credentials

from ..config import get_settings

# Security scheme
security = HTTPBearer(auto_error=False)

# Firebase initialization flag
_firebase_initialized = False


def initialize_firebase():
    """
    Initialize Firebase Admin SDK with credentials from multiple sources.
    
    Priority:
    1. Local file path (FIREBASE_CREDENTIALS_PATH)
    2. JSON from environment variable (GOOGLE_APPLICATION_CREDENTIALS_JSON)
    3. Default credentials (for Google Cloud environments)
    """
    global _firebase_initialized
    
    if _firebase_initialized:
        return
    
    settings = get_settings()
    cred_path = settings.firebase_credentials_path
    
    # Option 1: Local credentials file
    if cred_path and os.path.exists(cred_path):
        print(f"[Firebase] Using credentials file: {cred_path}")
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
        _firebase_initialized = True
        return
    
    # Option 2: Credentials JSON from environment variable
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        try:
            print("[Firebase] Using credentials from GOOGLE_APPLICATION_CREDENTIALS_JSON")
            creds_dict = json.loads(creds_json)
            cred = credentials.Certificate(creds_dict)
            firebase_admin.initialize_app(cred)
            _firebase_initialized = True
            return
        except Exception as e:
            print(f"[Firebase] Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON: {e}")
    
    # Option 3: Default credentials (Google Cloud environments)
    try:
        print("[Firebase] Attempting default credentials (cloud environment)")
        firebase_admin.initialize_app()
        _firebase_initialized = True
    except Exception as e:
        print(f"[Firebase] Warning: Could not initialize Firebase: {e}")
        print("[Firebase] Auth will fail for protected endpoints")
        _firebase_initialized = True  # Mark as initialized to avoid retry loops


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> dict:
    """
    Verify Firebase ID token and return user info.
    
    Returns:
        dict with keys: uid, email (optional), name (optional)
    
    Raises:
        HTTPException 401 if token is missing or invalid
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    try:
        initialize_firebase()
        
        # Verify the ID token
        decoded_token = auth.verify_id_token(credentials.credentials)
        
        return {
            "uid": decoded_token["uid"],
            "email": decoded_token.get("email"),
            "name": decoded_token.get("name"),
            "picture": decoded_token.get("picture"),
        }
    
    except auth.ExpiredIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except auth.InvalidIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Authentication failed: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Optional[dict]:
    """
    Optional authentication - returns None if no token provided.
    
    Useful for endpoints that work differently for authenticated vs anonymous users.
    """
    if credentials is None:
        return None
    
    try:
        return await get_current_user(credentials)
    except HTTPException:
        return None
