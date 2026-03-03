import os
import unicodedata
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _opt(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def _must(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def _norm_text(s: str) -> str:
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return " ".join(s.split())

def normalize_priority_name(name: str | None) -> tuple[str | None, str | None]:
    if not name:
        return None, None
    raw = name.strip()
    n = _norm_text(raw)

    if n == "mas alto":
        return raw, "mas_alto"
    if n == "alta":
        return raw, "alta"
    if n == "media":
        return raw, "media"
    if n == "baja":
        return raw, "baja"
    if n == "mas baja":
        return raw, "mas_baja"

    return raw, n.replace(" ", "_")

def _csv_set(s: str | None) -> set[str]:
    if not s:
        return set()
    return {x.strip() for x in s.split(",") if x.strip()}

def _to_bool(s: str | None, default: bool = False) -> bool:
    if s is None:
        return default
    return s.strip().lower() in {"1", "true", "yes", "y", "on"}

@dataclass(frozen=True)
class Settings:
    # Jira
    jira_server: str
    jira_username: str
    jira_token: str
    jira_project_key: str

    # Postgres local
    pg_dsn: str | None
    pg_host: str | None
    pg_port: int
    pg_db: str | None
    pg_user: str | None
    pg_pass: str | None
    pg_driver: str

    # Supabase Postgres
    supa_pg_dsn: str | None
    supa_host: str | None
    supa_port: int
    supa_db: str | None
    supa_user: str | None
    supa_pass: str | None
    supa_driver: str
    supa_ssl: bool

    # Incremental
    lookback_minutes: int
    overlap_minutes: int
    initial_lookback_hours: int

    # Business time
    tz: str
    business_start: str
    business_end: str

    # Workflow
    status_todo: str
    status_in_progress: str
    status_escalado: str
    status_done: str

    # Primera respuesta: lista blanca de soporte
    support_account_ids: set[str]

    # Exclusión de épicas
    excluded_epic_keys: set[str]
    epic_field_mode: str  # "epic_link" or "parent"

    # SLA por prioridad_norm -> {first, resolve, calendar}
    sla: dict

def load_settings() -> Settings:
    sla = {
        "mas_alto": {"first": int(os.getenv("SLA_MAS_ALTO_FIRST_MIN", "1")), "resolve": int(os.getenv("SLA_MAS_ALTO_RESOLVE_MIN", "120")), "calendar": True},
        "alta": {"first": int(os.getenv("SLA_ALTA_FIRST_MIN", "15")), "resolve": int(os.getenv("SLA_ALTA_RESOLVE_MIN", "480")), "calendar": True},
        "media": {"first": int(os.getenv("SLA_MEDIA_FIRST_MIN", "60")), "resolve": int(os.getenv("SLA_MEDIA_RESOLVE_MIN", "1200")), "calendar": False},
        "baja": {"first": int(os.getenv("SLA_BAJA_FIRST_MIN", "60")), "resolve": int(os.getenv("SLA_BAJA_RESOLVE_MIN", "1800")), "calendar": False},
        "mas_baja": {"first": int(os.getenv("SLA_MAS_BAJA_FIRST_MIN", "60")), "resolve": int(os.getenv("SLA_MAS_BAJA_RESOLVE_MIN", "1800")), "calendar": False},
    }

    support_ids = _csv_set(_must("SUPPORT_ACCOUNT_IDS"))
    excluded_epics = _csv_set(_opt("EXCLUDED_EPIC_KEYS", ""))

    epic_mode = (_opt("EPIC_FIELD_MODE", "epic_link") or "epic_link").strip().lower()
    if epic_mode not in {"epic_link", "parent"}:
        raise RuntimeError("EPIC_FIELD_MODE must be 'epic_link' or 'parent'")

    return Settings(
        jira_server=_must("JIRA_SERVER").rstrip("/"),
        jira_username=_must("JIRA_USERNAME"),
        jira_token=_must("JIRA_TOKEN"),
        jira_project_key=_must("JIRA_PROJECT_KEY"),

        pg_dsn=_opt("PG_DSN"),
        pg_host=_opt("PG_HOST"),
        pg_port=int(_opt("PG_PORT", "5432")),
        pg_db=_opt("PG_DB"),
        pg_user=_opt("PG_USER"),
        pg_pass=_opt("PG_PASS"),
        pg_driver=_opt("PG_DRIVER", "postgresql+pg8000"),

        supa_pg_dsn=_opt("SUPA_PG_DSN"),
        supa_host=_opt("SUPA_HOST"),
        supa_port=int(_opt("SUPA_PORT", "5432")),
        supa_db=_opt("SUPA_DB", "postgres"),
        supa_user=_opt("SUPA_USER", "postgres"),
        supa_pass=_opt("SUPA_PASS"),
        supa_driver=_opt("SUPA_DRIVER", "postgresql+pg8000"),
        supa_ssl=_to_bool(_opt("SUPA_SSL", "true"), default=True),

        lookback_minutes=int(_opt("LOOKBACK_MINUTES", "15")),
        overlap_minutes=int(_opt("OVERLAP_MINUTES", "5")),
        initial_lookback_hours=int(_opt("INITIAL_LOOKBACK_HOURS", "24")),

        tz=_opt("TZ", "America/Bogota"),
        business_start=_opt("BUSINESS_START", "08:00"),
        business_end=_opt("BUSINESS_END", "18:00"),

        status_todo=_opt("STATUS_TODO", "TO DO (ACCS)"),
        status_in_progress=_opt("STATUS_IN_PROGRESS", "IN PROGRESS (ACCS)"),
        status_escalado=_opt("STATUS_ESCALADO", "ESCALADO (ACCS)"),
        status_done=_opt("STATUS_DONE", "DONE (ACCS)"),

        support_account_ids=support_ids,
        excluded_epic_keys=excluded_epics,
        epic_field_mode=epic_mode,
        sla=sla,
    )
