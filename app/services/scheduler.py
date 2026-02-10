"""
Background Job Scheduler

Manages scheduled background tasks using APScheduler.
- Ingestion: Fetches new content every 30 minutes
- Indexer: Regenerates indices every 30 minutes
"""

import asyncio
from datetime import datetime
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from ..config import get_settings
from ..core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# Global scheduler instance
_scheduler: Optional[AsyncIOScheduler] = None


def get_scheduler() -> AsyncIOScheduler:
    """Get or create the scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler()
    return _scheduler


class SchedulerService:
    """
    Manages background job scheduling.
    
    Jobs:
    1. Content Ingestion (every 30 minutes)
       - Fetches from YouTube RSS, TMDB trending
       - Updates master_content.json
       
    2. Index Generation (every 30 minutes, offset by 15 min)
       - Calculates scores
       - Regenerates genre buckets
       - Uploads to Supabase
    """
    
    def __init__(self):
        self.scheduler = get_scheduler()
        self._ingestion_job_id = "content_ingestion"
        self._indexer_job_id = "index_generation"
        self._episode_notifier_job_id = "episode_notifier"
    
    async def _run_ingestion(self):
        """Execute the ingestion job."""
        from ..jobs.ingestion import run_ingestion_job
        
        print("\n" + "="*60)
        print("[CRON JOB] 🔄 INGESTION JOB STARTED")
        print(f"[CRON JOB] Time: {datetime.utcnow().isoformat()}")
        print("="*60)
        
        logger.info("scheduler_job_started", job="ingestion")
        try:
            await run_ingestion_job()
            logger.info("scheduler_job_completed", job="ingestion")
            print("[CRON JOB] ✅ INGESTION JOB COMPLETED\n")
        except Exception as e:
            logger.error("scheduler_job_failed", job="ingestion", error=str(e))
            print(f"[CRON JOB] ❌ INGESTION JOB FAILED: {e}\n")
    
    async def _run_indexer(self):
        """Execute the indexer job."""
        from ..jobs.indexer import run_indexer_job
        
        print("\n" + "="*60)
        print("[CRON JOB] 🔄 INDEXER JOB STARTED")
        print(f"[CRON JOB] Time: {datetime.utcnow().isoformat()}")
        print("="*60)
        
        logger.info("scheduler_job_started", job="indexer")
        try:
            await run_indexer_job()
            logger.info("scheduler_job_completed", job="indexer")
            print("[CRON JOB] ✅ INDEXER JOB COMPLETED\n")
        except Exception as e:
            logger.error("scheduler_job_failed", job="indexer", error=str(e))
            print(f"[CRON JOB] ❌ INDEXER JOB FAILED: {e}\n")
    
    async def _run_upload(self):
        """Upload indices to Supabase after indexer runs."""
        from .supabase_storage import get_supabase_storage
        
        print("\n" + "="*60)
        print("[CRON JOB] 🔄 SUPABASE UPLOAD JOB STARTED")
        print(f"[CRON JOB] Time: {datetime.utcnow().isoformat()}")
        print("="*60)
        
        logger.info("scheduler_job_started", job="supabase_upload")
        try:
            storage = get_supabase_storage()
            result = await storage.upload_all_indices()
            logger.info("scheduler_job_completed", job="supabase_upload")
            print(f"[CRON JOB] ✅ SUPABASE UPLOAD COMPLETED: {result}\n")
        except Exception as e:
            logger.error("scheduler_job_failed", job="supabase_upload", error=str(e))
            print(f"[CRON JOB] ❌ SUPABASE UPLOAD FAILED: {e}\n")

    async def _run_episode_notifier(self):
        """Execute the episode notifier job."""
        from ..jobs.episode_notifier import run_episode_notifier_job
        
        print("\n" + "="*60)
        print("[CRON JOB] 🔄 EPISODE NOTIFIER JOB STARTED")
        print(f"[CRON JOB] Time: {datetime.utcnow().isoformat()}")
        print("="*60)
        
        logger.info("scheduler_job_started", job="episode_notifier")
        try:
            await run_episode_notifier_job()
            logger.info("scheduler_job_completed", job="episode_notifier")
            print("[CRON JOB] ✅ EPISODE NOTIFIER JOB COMPLETED\n")
        except Exception as e:
            logger.error("scheduler_job_failed", job="episode_notifier", error=str(e))
            print(f"[CRON JOB] ❌ EPISODE NOTIFIER JOB FAILED: {e}\n")
    
    def setup_jobs(self):
        """Configure and add all scheduled jobs."""
        
        # Ingestion: Every 30 minutes at :00 and :30
        self.scheduler.add_job(
            self._run_ingestion,
            trigger=CronTrigger(minute="0,30"),
            id=self._ingestion_job_id,
            name="Content Ingestion",
            replace_existing=True,
            max_instances=1,
        )
        
        # Indexer: Every 30 minutes at :15 and :45 (offset from ingestion)
        self.scheduler.add_job(
            self._run_indexer,
            trigger=CronTrigger(minute="15,45"),
            id=self._indexer_job_id,
            name="Index Generation",
            replace_existing=True,
            max_instances=1,
        )

        # Episode Notifier: Every 12 hours at 09:00 and 21:00 UTC
        self.scheduler.add_job(
            self._run_episode_notifier,
            trigger=CronTrigger(hour="9,21", minute="0"), # Run twice a day
            id=self._episode_notifier_job_id,
            name="Episode Notifier",
            replace_existing=True,
            max_instances=1,
        )
        
        logger.info(
            "scheduler_jobs_configured",
            ingestion_schedule="every 30 min at :00/:30",
            indexer_schedule="every 30 min at :15/:45",
            episode_notifier_schedule="every 12 hours at 09:00/21:00"
        )
    
    def start(self):
        """Start the scheduler."""
        if not self.scheduler.running:
            self.setup_jobs()
            self.scheduler.start()
            logger.info("scheduler_started")
    
    def stop(self):
        """Stop the scheduler gracefully."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            logger.info("scheduler_stopped")
    
    def get_job_status(self) -> dict:
        """Get status of all scheduled jobs."""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "pending": job.pending,
            })
        
        return {
            "running": self.scheduler.running,
            "jobs": jobs,
            "current_time": datetime.utcnow().isoformat(),
        }
    
    async def trigger_ingestion_now(self):
        """Manually trigger ingestion job."""
        logger.info("manual_trigger", job="ingestion")
        await self._run_ingestion()
    
    async def trigger_indexer_now(self):
        """Manually trigger indexer job."""
        logger.info("manual_trigger", job="indexer")
        await self._run_indexer()
        await self._run_upload()

    async def trigger_episode_notifier_now(self):
        """Manually trigger episode notifier job."""
        logger.info("manual_trigger", job="episode_notifier")
        await self._run_episode_notifier()


# Singleton instance
_scheduler_service: Optional[SchedulerService] = None


def get_scheduler_service() -> SchedulerService:
    """Get singleton SchedulerService instance."""
    global _scheduler_service
    if _scheduler_service is None:
        _scheduler_service = SchedulerService()
    return _scheduler_service
