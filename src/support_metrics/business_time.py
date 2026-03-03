from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

@dataclass(frozen=True)
class BusinessCalendar:
    tz: ZoneInfo
    start: time
    end: time

def parse_hhmm(s: str) -> time:
    hh, mm = s.split(":")
    return time(int(hh), int(mm), 0)

def calendar_minutes_between(start_dt: datetime | None, end_dt: datetime | None) -> int | None:
    if not start_dt or not end_dt or start_dt >= end_dt:
        return None
    return int((end_dt - start_dt).total_seconds() // 60)

def business_minutes_between(cal: BusinessCalendar, start_dt: datetime | None, end_dt: datetime | None) -> int | None:
    if not start_dt or not end_dt or start_dt >= end_dt:
        return None

    s = start_dt.astimezone(cal.tz)
    e = end_dt.astimezone(cal.tz)

    total = 0
    cur = s

    while cur.date() <= e.date():
        if cur.weekday() < 5:
            day_start = datetime.combine(cur.date(), cal.start, tzinfo=cal.tz)
            day_end = datetime.combine(cur.date(), cal.end, tzinfo=cal.tz)

            seg_start = max(cur, day_start)
            seg_end = min(e, day_end)
            if seg_end > seg_start:
                total += int((seg_end - seg_start).total_seconds() // 60)

        cur = datetime.combine(cur.date() + timedelta(days=1), time(0, 0), tzinfo=cal.tz)

    return total
