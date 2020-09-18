from datetime import datetime


def run_date_time(strftime=None):
    """Run date time"""
    if strftime is not None:
        return datetime.now().strftime(strftime)
    return datetime.now()
