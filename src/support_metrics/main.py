from __future__ import annotations
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .config import load_settings
from .jira_api import JiraClient
from .db import (
    connect_pool_local,
    connect_pool_supabase,
    get_state,
    set_state,
    exe_non_query,
)
from .business_time import BusinessCalendar, parse_hhmm
from .metrics import compute_issue_facts

STATE_KEY = "jira_metrics_last_successful_run"
SCHEMA = "armss"

UPSERT_SQL = f"""
insert into {SCHEMA}.jira_issue_facts (
  issue_key, issue_id, project_key,
  issue_type, priority_name, priority_norm,
  created_at, updated_at, current_status,
  assignee_account_id, reporter_account_id,
  first_comment_at, first_response_minutes,
  resolution_at, resolution_minutes,
  todo_minutes, in_progress_minutes,
  is_final, final_status, escalated,
  sla_first_response_target_min, sla_resolution_target_min,
  sla_first_response_met, sla_resolution_met,
  computed_at
) values (
  :issue_key, :issue_id, :project_key,
  :issue_type, :priority_name, :priority_norm,
  :created_at, :updated_at, :current_status,
  :assignee_account_id, :reporter_account_id,
  :first_comment_at, :first_response_minutes,
  :resolution_at, :resolution_minutes,
  :todo_minutes, :in_progress_minutes,
  :is_final, :final_status, :escalated,
  :sla_first_response_target_min, :sla_resolution_target_min,
  :sla_first_response_met, :sla_resolution_met,
  now()
)
on conflict (issue_key) do update set
  issue_id = excluded.issue_id,
  project_key = excluded.project_key,
  issue_type = excluded.issue_type,
  priority_name = excluded.priority_name,
  priority_norm = excluded.priority_norm,
  created_at = excluded.created_at,
  updated_at = excluded.updated_at,
  current_status = excluded.current_status,
  assignee_account_id = excluded.assignee_account_id,
  reporter_account_id = excluded.reporter_account_id,
  first_comment_at = excluded.first_comment_at,
  first_response_minutes = excluded.first_response_minutes,
  resolution_at = excluded.resolution_at,
  resolution_minutes = excluded.resolution_minutes,
  todo_minutes = excluded.todo_minutes,
  in_progress_minutes = excluded.in_progress_minutes,
  is_final = excluded.is_final,
  final_status = excluded.final_status,
  escalated = excluded.escalated,
  sla_first_response_target_min = excluded.sla_first_response_target_min,
  sla_resolution_target_min = excluded.sla_resolution_target_min,
  sla_first_response_met = excluded.sla_first_response_met,
  sla_resolution_met = excluded.sla_resolution_met,
  computed_at = now()
"""

def _build_epic_exclusion_clause(settings) -> str:
    if not settings.excluded_epic_keys:
        return ""

    keys = sorted(settings.excluded_epic_keys)
    keys_csv = ", ".join(keys)

    if settings.epic_field_mode == "parent":
        return f" AND (parent is EMPTY OR parent not in ({keys_csv}))"

    return f' AND ("Epic Link" is EMPTY OR "Epic Link" not in ({keys_csv}))'

def main():
    settings = load_settings()

    tz = ZoneInfo(settings.tz)
    cal = BusinessCalendar(
        tz=tz,
        start=parse_hhmm(settings.business_start),
        end=parse_hhmm(settings.business_end),
    )

    pool_local = connect_pool_local(settings)
    pool_supa = connect_pool_supabase(settings)

    jira = JiraClient(settings.jira_server, settings.jira_username, settings.jira_token)

    now_utc = datetime.now(timezone.utc)

    # Usamos el estado del ETL desde LOCAL como “fuente de verdad”
    last = get_state(pool_local, STATE_KEY, schema=SCHEMA)
    if last:
        last_dt = datetime.fromisoformat(last)
    else:
        last_dt = now_utc - timedelta(hours=settings.initial_lookback_hours)

    window_start = last_dt - timedelta(minutes=settings.overlap_minutes)
    epic_exclusion = _build_epic_exclusion_clause(settings)

    jql = (
        f'project = {settings.jira_project_key} '
        f'AND updated >= "{window_start.strftime("%Y-%m-%d %H:%M")}"'
        f'{epic_exclusion} '
        f'order by updated asc'
    )

    fields = [
        "created", "updated", "priority", "status", "assignee", "reporter",
        "resolutiondate", "issuetype", "project"
    ]

    next_token = None
    total = 0

    while True:
        page = jira.search_issues(jql=jql, fields=fields, next_page_token=next_token, max_results=100)
        issues = page.get("issues", []) or []
        if not issues:
            break

        for it in issues:
            issue_key = it.get("key")
            if not issue_key:
                continue

            full = jira.get_issue(issue_key, fields=fields, expand="changelog")
            facts = compute_issue_facts(full, jira, cal, settings)
            params = facts.__dict__

            # 1) UPSERT en Postgres local
            exe_non_query(UPSERT_SQL, pool_local, params)
            # 2) UPSERT en Supabase
            exe_non_query(UPSERT_SQL, pool_supa, params)

            total += 1

        next_token = page.get("nextPageToken")
        if not next_token:
            break

    # Solo avanzamos el puntero si ambas escrituras terminaron bien
    set_state(pool_local, STATE_KEY, now_utc.isoformat(), schema=SCHEMA)
    set_state(pool_supa, STATE_KEY, now_utc.isoformat(), schema=SCHEMA)

    print(f"OK. Upserted {total} issues to LOCAL + SUPABASE.")

if __name__ == "__main__":
    main()
