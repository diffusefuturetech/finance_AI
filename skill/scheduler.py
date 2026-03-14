"""Scheduled tasks for daily market alerts via Feishu webhook."""

import logging
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import FEISHU_WEBHOOK_URL

logger = logging.getLogger(__name__)


class FeishuWebhook:
    """Send messages to Feishu group chat via webhook."""

    def __init__(self, webhook_url: str | None = None):
        self.webhook_url = webhook_url or FEISHU_WEBHOOK_URL
        if not self.webhook_url:
            raise ValueError("FEISHU_WEBHOOK_URL not configured")

    def send_text(self, content: str) -> bool:
        """Send plain text message."""
        payload = {
            "msg_type": "text",
            "content": {"text": content},
        }
        return self._post(payload)

    def send_rich_text(self, title: str, content: list[list[dict]]) -> bool:
        """Send rich text (post) message.

        Args:
            title: message title
            content: nested list of content elements, e.g.:
                [[{"tag": "text", "text": "hello"}]]
        """
        payload = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": content,
                    }
                }
            },
        }
        return self._post(payload)

    def _post(self, payload: dict) -> bool:
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == 0 or data.get("StatusCode") == 0:
                    return True
                logger.error(f"Feishu API error: {data}")
            else:
                logger.error(f"Feishu HTTP error: {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"Feishu webhook failed: {e}")
            return False


class DailyScheduler:
    """Manages scheduled analysis tasks."""

    def __init__(self):
        self.webhook = FeishuWebhook() if FEISHU_WEBHOOK_URL else None

    def daily_market_summary(self) -> None:
        """Generate and push daily market summary. Run at 15:30 CST."""
        from skill.handler import SkillHandler

        logger.info("Generating daily market summary...")
        handler = SkillHandler()
        summary = handler.handle_market()

        if self.webhook:
            self.webhook.send_text(summary)
            logger.info("Daily summary pushed to Feishu")
        else:
            logger.warning("Feishu webhook not configured, printing to stdout")
            print(summary)

    def daily_data_update(self) -> None:
        """Update Qlib data with today's trading data. Run at 16:00 CST."""
        from data.converter import QlibDataConverter

        logger.info("Starting daily data update...")
        converter = QlibDataConverter()
        symbols = converter._get_symbols("csi300")
        from datetime import date
        converter.incremental_update(symbols, date.today().strftime("%Y%m%d"))
        logger.info("Daily data update completed")


def run_scheduler():
    """Start APScheduler for daily tasks."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    scheduler_instance = DailyScheduler()
    sched = BlockingScheduler(timezone="Asia/Shanghai")

    # 15:30 - Daily market summary
    sched.add_job(
        scheduler_instance.daily_market_summary,
        "cron",
        hour=15,
        minute=30,
        day_of_week="mon-fri",
        id="market_summary",
    )

    # 16:00 - Daily data update
    sched.add_job(
        scheduler_instance.daily_data_update,
        "cron",
        hour=16,
        minute=0,
        day_of_week="mon-fri",
        id="data_update",
    )

    logger.info("Scheduler started. Jobs: 15:30 market summary, 16:00 data update")
    sched.start()


if __name__ == "__main__":
    run_scheduler()
