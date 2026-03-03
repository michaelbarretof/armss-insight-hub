from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from dateutil.parser import isoparse

from .business_time import BusinessCalendar, business_minutes_between, calendar_minutes_between
from .config import normalize_priority_name

@dataclass(frozen=True)
class IssueFacts:
    issue_key: str
    issue_id: int | None
    project_key: str
    issue_type: str | None

    priority_name: str | None
    priority_norm: str | None

    created_at: datetime
    updated_at: datetime
    current_status: str | None

    assignee_account_id: str | None
    reporter_account_id: str | None

    first_comment_at: datetime | None
    first_response_minutes: int | None

    resolution_at: datetime | None
    resolution_minutes: int | None

    todo_minutes: int | None
    in_progress_minutes: int | None

    is_final: bool
    final_status: str | None
    escalated: bool

    sla_first_response_target_min: int | None
    sla_resolution_target_min: int | None
    sla_first_response_met: bool | None
    sla_resolution_met: bool | None

def _dt(s: str | None) -> datetime | None:
    return isoparse(s) if s else None

def extract_status_changes(issue_json: dict) -> list[tuple[datetime, str | None, str | None]]:
    changes = []
    changelog = issue_json.get("changelog", {})
    for h in changelog.get("histories", []) or []:
        ts = _dt(h.get("created"))
        for it in h.get("items", []) or []:
            if it.get("field") == "status":
                changes.append((ts, it.get("fromString"), it.get("toString")))
    changes.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc))
    return changes

def first_time_entered(changes, to_status: str) -> datetime | None:
    tgt = (to_status or "").strip().lower()
    for ts, _from, _to in changes:
        if ts and ((_to or "").strip().lower() == tgt):
            return ts
    return None

def compute_status_durations(created_at: datetime, end_at: datetime, initial_status: str, changes, minutes_fn):
    durations: dict[str, int] = {}
    cur_status = initial_status
    cur_time = created_at

    for ts, _from, to_s in changes:
        if not ts or ts <= cur_time:
            continue
        durations[cur_status] = (durations.get(cur_status, 0) or 0) + (minutes_fn(cur_time, ts) or 0)
        cur_status = to_s or cur_status
        cur_time = ts

    durations[cur_status] = (durations.get(cur_status, 0) or 0) + (minutes_fn(cur_time, end_at) or 0)
    return durations

def get_first_support_comment_datetime(jira_client, issue_key: str, support_account_ids: set[str]) -> datetime | None:
    """
    Retorna el timestamp del primer comentario cuyo author.accountId esté en support_account_ids.
    Jira comments incluye author.accountId y created, así que podemos filtrar por autor. [web:86]
    """
    start_at = 0
    first = None

    while True:
        data = jira_client.get_comments(issue_key, start_at=start_at, max_results=50)
        comments = data.get("comments", []) or []

        for c in comments:
            author = c.get("author") or {}
            author_id = author.get("accountId")
            if not author_id or author_id not in support_account_ids:
                continue

            ts = _dt(c.get("created"))
            if ts and (first is None or ts < first):
                first = ts

        start_at = int(data.get("startAt", 0)) + int(data.get("maxResults", 0))
        total = int(data.get("total", 0))
        if start_at >= total:
            break

    return first

def compute_issue_facts(issue_json: dict, jira_client, cal: BusinessCalendar, settings) -> IssueFacts:
    key = issue_json.get("key")
    issue_id = int(issue_json.get("id")) if issue_json.get("id") else None
    fields = issue_json.get("fields", {})

    created_at = _dt(fields.get("created"))
    updated_at = _dt(fields.get("updated"))

    project_key = (fields.get("project") or {}).get("key") or settings.jira_project_key
    issue_type = (fields.get("issuetype") or {}).get("name")

    priority_raw = (fields.get("priority") or {}).get("name")
    priority_name, priority_norm = normalize_priority_name(priority_raw)

    current_status = (fields.get("status") or {}).get("name")
    assignee_id = (fields.get("assignee") or {}).get("accountId")
    reporter_id = (fields.get("reporter") or {}).get("accountId")

    changes = extract_status_changes(issue_json)

    escalado_at = first_time_entered(changes, settings.status_escalado)
    done_at = first_time_entered(changes, settings.status_done)
    resolution_at = min([d for d in [escalado_at, done_at] if d], default=None) or _dt(fields.get("resolutiondate"))

    final_status = None
    if resolution_at:
        if escalado_at and resolution_at == escalado_at:
            final_status = settings.status_escalado
        else:
            final_status = settings.status_done

    is_final = final_status is not None
    escalated = (final_status or "").strip().lower() == settings.status_escalado.strip().lower()

    # Primera respuesta = primer comentario hecho por Soporte (lista blanca accountId)
    first_comment_at = get_first_support_comment_datetime(jira_client, key, settings.support_account_ids)

    sla_cfg = settings.sla.get(priority_norm or "", None)
    if sla_cfg:
        sla_first_target = int(sla_cfg["first"])
        sla_resolve_target = int(sla_cfg["resolve"])
        use_calendar = bool(sla_cfg["calendar"])
    else:
        sla_first_target = None
        sla_resolve_target = None
        use_calendar = True

    if use_calendar:
        minutes_fn = calendar_minutes_between
    else:
        minutes_fn = lambda a, b: business_minutes_between(cal, a, b)

    first_response_minutes = minutes_fn(created_at, first_comment_at)
    resolution_minutes = minutes_fn(created_at, resolution_at)

    end_for_durations = resolution_at or datetime.now(timezone.utc)
    durations = compute_status_durations(
        created_at=created_at,
        end_at=end_for_durations,
        initial_status=settings.status_todo,
        changes=changes,
        minutes_fn=minutes_fn,
    )
    todo_minutes = durations.get(settings.status_todo, 0)
    in_progress_minutes = durations.get(settings.status_in_progress, 0)

    sla_first_met = None
    if sla_first_target is not None and first_response_minutes is not None:
        sla_first_met = first_response_minutes <= sla_first_target

    sla_resolve_met = None
    if sla_resolve_target is not None and resolution_minutes is not None:
        sla_resolve_met = resolution_minutes <= sla_resolve_target

    return IssueFacts(
        issue_key=key,
        issue_id=issue_id,
        project_key=project_key,
        issue_type=issue_type,
        priority_name=priority_name,
        priority_norm=priority_norm,
        created_at=created_at,
        updated_at=updated_at,
        current_status=current_status,
        assignee_account_id=assignee_id,
        reporter_account_id=reporter_id,
        first_comment_at=first_comment_at,
        first_response_minutes=first_response_minutes,
        resolution_at=resolution_at,
        resolution_minutes=resolution_minutes,
        todo_minutes=todo_minutes,
        in_progress_minutes=in_progress_minutes,
        is_final=is_final,
        final_status=final_status,
        escalated=escalated,
        sla_first_response_target_min=sla_first_target,
        sla_resolution_target_min=sla_resolve_target,
        sla_first_response_met=sla_first_met,
        sla_resolution_met=sla_resolve_met,
    )
