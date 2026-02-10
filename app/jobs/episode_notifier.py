
from datetime import datetime
import firebase_admin
from firebase_admin import messaging

from ..core.logging import get_logger
from ..core.security import initialize_firebase

logger = get_logger(__name__)


async def run_episode_notifier_job():
    """
    Trigger the 'episode_check' FCM topic.
    
    This sends a silent data message to all devices subscribed to 'episode_check'.
    The devices will then wake up (if possible) and check TMDB for new episodes
    of the shows they are watching.
    """
    try:
        # Ensure Firebase is initialized
        initialize_firebase()
        
        topic = 'episode_check'
        
        # Construct the message
        # See: https://firebase.google.com/docs/cloud-messaging/send-message
        message = messaging.Message(
            data={
                'type': 'EPISODE_CHECK',
                'timestamp': datetime.utcnow().isoformat(),
            },
            topic=topic,
            # Android config for high priority (to wake up doze mode)
            android=messaging.AndroidConfig(
                priority='high',
                ttl=0, # Deliver immediately or drop
            ),
            # APNs config for background fetch
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        content_available=True, # Required for background fetch
                    ),
                ),
            ),
        )

        # Send the message
        response = messaging.send(message)
        
        logger.info(
            "episode_check_triggered", 
            topic=topic, 
            message_id=response
        )
        print(f"[CRON JOB] 🚀 EPISODE CHECK TRIGGERED: {response}")
        
    except Exception as e:
        logger.error("episode_check_trigger_failed", error=str(e))
        print(f"[CRON JOB] ❌ EPISODE CHECK TRIGGER FAILED: {e}")
        raise e
