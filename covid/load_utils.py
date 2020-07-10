import time

SECONDS_TO_SLEEP = 20


def sleep_and_log(seconds=SECONDS_TO_SLEEP):
    print(f"Sleeping for {seconds} between posts...")
    time.sleep(seconds)
