from datetime import datetime

from attr import Factory


def _run_date_time(strftime=None):
    """Run date time"""
    if strftime is not None:
        return datetime.now().strftime(strftime)
    return datetime.now()


run_date_time = Factory(_run_date_time)
