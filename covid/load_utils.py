import logging
import time

SECONDS_TO_SLEEP = 20

logger = logging.getLogger(__name__)


def sleep_and_log(seconds=SECONDS_TO_SLEEP):
    logger.info(f"Sleeping for {seconds} seconds between posts...")
    time.sleep(seconds)
