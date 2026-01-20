"""
Social Router Tests

Tests for the social graph sync endpoints.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi.testclient import TestClient
from httpx import Response

# Mock Firebase auth before importing app
@pytest.fixture(autouse=True)
def mock_firebase():
    """Mock Firebase auth for all tests."""
    with patch("app.core.security.auth") as mock_auth:
        mock_auth.verify_id_token.return_value = {
            "uid": "test_user_123",
            "email": "test@example.com",
            "name": "Test User",
        }
        yield mock_auth


@pytest.fixture
def client(mock_firebase):
    """Create test client."""
    from app.main import app
    return TestClient(app)


@pytest.fixture
def auth_headers():
    """Create auth headers with mock token."""
    return {"Authorization": "Bearer mock_firebase_token"}


class TestFollowEndpoint:
    """Tests for POST /social/follow endpoint."""
    
    @patch("app.routers.social.httpx.AsyncClient")
    def test_follow_success(self, mock_client_class, client, auth_headers):
        """Test successful follow sync."""
        # Mock Supabase response
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.text = ""
        
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client
        
        response = client.post(
            "/social/follow",
            json={
                "target_uid": "target_user_456",
                "action": "follow",
            },
            headers=auth_headers,
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["action"] == "follow"
        assert data["follower_uid"] == "test_user_123"
        assert data["target_uid"] == "target_user_456"
    
    @patch("app.routers.social.httpx.AsyncClient")
    def test_duplicate_follow_idempotent(self, mock_client_class, client, auth_headers):
        """Test that duplicate follow doesn't fail (idempotent)."""
        # Supabase returns 200 for ignored duplicates
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""
        
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client
        
        # First follow
        response1 = client.post(
            "/social/follow",
            json={"target_uid": "target_user_456", "action": "follow"},
            headers=auth_headers,
        )
        assert response1.status_code == 200
        
        # Duplicate follow - should still succeed
        response2 = client.post(
            "/social/follow",
            json={"target_uid": "target_user_456", "action": "follow"},
            headers=auth_headers,
        )
        assert response2.status_code == 200
        assert response2.json()["success"] is True
    
    @patch("app.routers.social.httpx.AsyncClient")
    def test_unfollow_success(self, mock_client_class, client, auth_headers):
        """Test successful unfollow sync."""
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_response.text = ""
        
        mock_client = AsyncMock()
        mock_client.delete.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client
        
        response = client.post(
            "/social/follow",
            json={
                "target_uid": "target_user_456",
                "action": "unfollow",
            },
            headers=auth_headers,
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["action"] == "unfollow"
    
    def test_self_follow_rejected(self, client, auth_headers):
        """Test that self-follow is rejected."""
        response = client.post(
            "/social/follow",
            json={
                "target_uid": "test_user_123",  # Same as authenticated user
                "action": "follow",
            },
            headers=auth_headers,
        )
        
        assert response.status_code == 400
        assert "Cannot follow yourself" in response.json()["detail"]
    
    def test_missing_token_rejected(self, client):
        """Test that missing auth token is rejected."""
        response = client.post(
            "/social/follow",
            json={
                "target_uid": "target_user_456",
                "action": "follow",
            },
            # No auth headers
        )
        
        assert response.status_code == 401
    
    def test_invalid_action_rejected(self, client, auth_headers):
        """Test that invalid action is rejected."""
        response = client.post(
            "/social/follow",
            json={
                "target_uid": "target_user_456",
                "action": "invalid_action",
            },
            headers=auth_headers,
        )
        
        assert response.status_code == 422  # Pydantic validation error
    
    def test_missing_target_uid_rejected(self, client, auth_headers):
        """Test that missing target_uid is rejected."""
        response = client.post(
            "/social/follow",
            json={
                "action": "follow",
            },
            headers=auth_headers,
        )
        
        assert response.status_code == 422  # Pydantic validation error


class TestQueryEndpoints:
    """Tests for social query endpoints."""
    
    @patch("app.routers.social.httpx.AsyncClient")
    def test_get_followers(self, mock_client_class, client, auth_headers):
        """Test get followers endpoint."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"follower_id": "user_1", "created_at": "2025-01-01T00:00:00Z"},
            {"follower_id": "user_2", "created_at": "2025-01-02T00:00:00Z"},
        ]
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client
        
        response = client.get(
            "/social/followers/test_user_123",
            headers=auth_headers,
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["user_id"] == "test_user_123"
        assert data["count"] == 2
    
    @patch("app.routers.social.httpx.AsyncClient")
    def test_get_stats(self, mock_client_class, client, auth_headers):
        """Test get user stats endpoint."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "followers_count": 100,
                "following_count": 50,
                "updated_at": "2025-01-01T00:00:00Z",
            }
        ]
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client
        
        response = client.get(
            "/social/stats/test_user_123",
            headers=auth_headers,
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["followers_count"] == 100
        assert data["following_count"] == 50


# =============================================================================
# MANUAL SQL VERIFICATION (for reference)
# =============================================================================
#
# After running migrations, verify tables:
#
# -- Check tables exist
# SELECT * FROM follows LIMIT 5;
# SELECT * FROM user_stats LIMIT 5;
#
# -- Test follow (triggers should update counts)
# INSERT INTO follows (follower_id, following_id) VALUES ('user_a', 'user_b');
# SELECT * FROM user_stats WHERE user_id IN ('user_a', 'user_b');
#
# -- Test unfollow (triggers should decrement counts)
# DELETE FROM follows WHERE follower_id = 'user_a' AND following_id = 'user_b';
# SELECT * FROM user_stats WHERE user_id IN ('user_a', 'user_b');
#
# -- Test self-follow constraint
# INSERT INTO follows (follower_id, following_id) VALUES ('user_a', 'user_a');
# -- Should fail with constraint violation
#
# =============================================================================
