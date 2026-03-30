from datetime import datetime


def format_log_dt(dt: datetime | None) -> str:
    """
    Format a datetime for application logs without microseconds.

    Args:
        dt: Datetime value to format.

    Returns:
        A string formatted with second-level precision. Timezone offsets are
        preserved for aware datetimes. Returns "None" when dt is None.
    """
    if dt is None:
        return "None"
    return dt.replace(microsecond=0).isoformat(sep=" ")
