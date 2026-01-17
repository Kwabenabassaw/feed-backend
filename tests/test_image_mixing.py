import pytest
from unittest.mock import MagicMock
from app.services.generator import FeedGenerator

@pytest.fixture
def feed_generator():
    """Create a FeedGenerator instance with mocked dependencies."""
    return FeedGenerator(
        index_pool=MagicMock(),
        dedup_service=MagicMock(),
        redis_client=MagicMock()
    )

def test_mix_images_into_feed(feed_generator):
    """Test the 3:1 video to image mixing ratio."""

    # 10 videos, 5 images
    video_ids = [f"vid_{i}" for i in range(10)]
    image_ids = [f"img_{i}" for i in range(5)]

    mixed = feed_generator._mix_images_into_feed(video_ids, image_ids)

    # Expected pattern:
    # 0, 1, 2, IMG, 3, 4, 5, IMG, 6, 7, 8, IMG, 9

    assert len(mixed) == 10 + 3 # 13 items total

    # Check images at expected positions (0-indexed)
    # Positions: 3, 7, 11
    assert mixed[3] == "img_0"
    assert mixed[7] == "img_1"
    assert mixed[11] == "img_2"

    # Check some videos
    assert mixed[0] == "vid_0"
    assert mixed[4] == "vid_3"

def test_mix_images_not_enough_images(feed_generator):
    """Test mixing when there are fewer images than needed."""

    video_ids = [f"vid_{i}" for i in range(10)]
    image_ids = ["img_0"] # Only 1 image

    mixed = feed_generator._mix_images_into_feed(video_ids, image_ids)

    # Should insert image at 3, but then run out
    assert len(mixed) == 11
    assert mixed[3] == "img_0"

    # Position 7 should be a video (vid_6) since we ran out of images
    # Sequence: 0, 1, 2, IMG0, 3, 4, 5, (no img), 6...
    # Wait, the loop index i continues.
    # i=0..2 (append vid)
    # i=2 -> (2+1)%3==0 -> insert img_0. img_idx=1.
    # i=3..5 (append vid)
    # i=5 -> (5+1)%3==0 -> insert img? check img_idx < len(image_ids). 1 < 1 is False. No insert.

    assert mixed[7] == "vid_6"

def test_mix_images_no_images(feed_generator):
    """Test mixing with no images."""
    video_ids = ["vid_1"]
    mixed = feed_generator._mix_images_into_feed(video_ids, [])
    assert mixed == video_ids
