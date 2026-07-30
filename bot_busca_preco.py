"""
BOT-BUSCADOR-PRECO
Versão consolidada em um único arquivo Python para Render.

Arquitetura:
- FastAPI recebe o webhook do Telegram.
- PostgreSQL armazena usuários, franquias, histórico e uma fila durável.
- Um loop assíncrono no mesmo processo processa as pesquisas.
- OpenAI Responses API executa pesquisa web e análise de imagens.

Não usa Celery nem Redis.
"""

from __future__ import annotations

import asyncio
import base64
import html
import io
import json
import logging
import os
import re
import secrets
import signal
import sys
import time
import textwrap
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from openai import AsyncOpenAI
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
    delete,
    func,
    select,
    text,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


# =============================================================================
# Ambiente e configuração
# =============================================================================


def load_local_env(path: str = ".env") -> None:
    """Carrega .env apenas em desenvolvimento e sem sobrescrever o ambiente."""
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


load_local_env()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "sim", "on"}


def env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} deve ser um número inteiro") from exc
    if minimum is not None and value < minimum:
        raise RuntimeError(f"{name} deve ser maior ou igual a {minimum}")
    return value


def env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.getenv(name, str(default)).strip().replace(",", ".")
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} deve ser numérico") from exc
    if minimum is not None and value < minimum:
        raise RuntimeError(f"{name} deve ser maior ou igual a {minimum}")
    return value


def normalize_database_url(url: str) -> str:
    url = url.strip()
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url[len("postgres://") :]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


def parse_admin_ids(raw: str) -> frozenset[int]:
    result: set[int] = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if not item.isdigit():
            raise RuntimeError("ADMIN_USER_IDS deve conter apenas IDs numéricos separados por vírgula")
        result.add(int(item))
    return frozenset(result)


@dataclass(frozen=True, slots=True)
class Settings:
    environment: str
    log_level: str
    timezone_name: str
    telegram_bot_token: str
    telegram_webhook_secret: str
    telegram_bot_username: str
    admin_user_ids: frozenset[int]
    support_contact: str
    sales_message: str
    license_payment_url: str
    project_api_key: str
    openai_api_key: str
    openai_model: str
    openai_max_output_tokens: int
    openai_timeout_seconds: int
    openai_search_context: str
    database_url: str
    public_base_url: str
    basic_monthly_quota: int
    premium_monthly_price_brl: float
    max_active_requests_per_user: int
    rate_limit_requests: int
    rate_limit_window_seconds: int
    default_capital_brl: float
    default_margin_percent: float
    default_roi_percent: float
    default_max_product_allocation_percent: float
    job_poll_seconds: float
    job_max_attempts: int
    stale_job_minutes: int
    history_free_limit: int
    history_premium_limit: int
    max_query_chars: int
    max_image_bytes: int
    webhook_enabled: bool

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)

    @classmethod
    def from_env(cls) -> "Settings":
        render_host = os.getenv("RENDER_EXTERNAL_HOSTNAME", "").strip()
        public_base_url = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
        if not public_base_url and render_host:
            public_base_url = f"https://{render_host}"

        secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
        if secret and not re.fullmatch(r"[A-Za-z0-9_-]{16,256}", secret):
            raise RuntimeError(
                "TELEGRAM_WEBHOOK_SECRET deve ter 16 a 256 caracteres e usar apenas letras, números, _ ou -"
            )

        search_context = os.getenv("OPENAI_SEARCH_CONTEXT", "medium").strip().lower()
        if search_context not in {"low", "medium", "high"}:
            raise RuntimeError("OPENAI_SEARCH_CONTEXT deve ser low, medium ou high")

        settings = cls(
            environment=os.getenv("ENVIRONMENT", "development").strip().lower(),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
            timezone_name=os.getenv("TIMEZONE", "America/Sao_Paulo").strip(),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_webhook_secret=secret,
            telegram_bot_username=os.getenv("TELEGRAM_BOT_USERNAME", "").strip().lstrip("@"),
            admin_user_ids=parse_admin_ids(os.getenv("ADMIN_USER_IDS", "")),
            support_contact=os.getenv("SUPPORT_CONTACT", "Atendimento pelo administrador do bot.").strip(),
            sales_message=os.getenv(
                "SALES_MESSAGE", "Ative o Premium para liberar consultas ilimitadas e ferramentas de revenda."
            ).strip(),
            license_payment_url=os.getenv("LICENSE_PAYMENT_URL", "").strip(),
            project_api_key=os.getenv("PROJECT_API_KEY", "").strip(),
            openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
            openai_model=os.getenv("OPENAI_MODEL", "gpt-5.6").strip(),
            openai_max_output_tokens=env_int("OPENAI_MAX_OUTPUT_TOKENS", 7000, 500),
            openai_timeout_seconds=env_int("OPENAI_TIMEOUT_SECONDS", 420, 30),
            openai_search_context=search_context,
            database_url=normalize_database_url(
                os.getenv("DATABASE_URL", "sqlite:///./bot_busca_preco.db")
            ),
            public_base_url=public_base_url,
            basic_monthly_quota=env_int("BASIC_MONTHLY_QUOTA", 20, 0),
            premium_monthly_price_brl=env_float("PREMIUM_MONTHLY_PRICE_BRL", 9.90, 0),
            max_active_requests_per_user=env_int("MAX_ACTIVE_REQUESTS_PER_USER", 1, 1),
            rate_limit_requests=env_int("RATE_LIMIT_REQUESTS", 8, 1),
            rate_limit_window_seconds=env_int("RATE_LIMIT_WINDOW_SECONDS", 60, 1),
            default_capital_brl=env_float("DEFAULT_CAPITAL_BRL", 5000, 0),
            default_margin_percent=env_float("DEFAULT_MARGIN_PERCENT", 20, 0),
            default_roi_percent=env_float("DEFAULT_ROI_PERCENT", 25, 0),
            default_max_product_allocation_percent=env_float(
                "DEFAULT_MAX_PRODUCT_ALLOCATION_PERCENT", 25, 0
            ),
            job_poll_seconds=env_float("JOB_POLL_SECONDS", 2.0, 0.5),
            job_max_attempts=env_int("JOB_MAX_ATTEMPTS", 2, 1),
            stale_job_minutes=env_int("STALE_JOB_MINUTES", 15, 2),
            history_free_limit=env_int("HISTORY_FREE_LIMIT", 5, 1),
            history_premium_limit=env_int("HISTORY_PREMIUM_LIMIT", 20, 1),
            max_query_chars=env_int("MAX_QUERY_CHARS", 2500, 100),
            max_image_bytes=env_int("MAX_IMAGE_BYTES", 10_000_000, 100_000),
            webhook_enabled=env_bool("WEBHOOK_ENABLED", True),
        )

        required = {
            "TELEGRAM_BOT_TOKEN": settings.telegram_bot_token,
            "TELEGRAM_WEBHOOK_SECRET": settings.telegram_webhook_secret,
            "OPENAI_API_KEY": settings.openai_api_key,
        }
        missing = [name for name, value in required.items() if not value]
        if missing and settings.environment == "production":
            raise RuntimeError(f"Variáveis obrigatórias ausentes: {', '.join(missing)}")

        # Valida o fuso já na inicialização.
        ZoneInfo(settings.timezone_name)
        return settings


SETTINGS = Settings.from_env()

logging.basicConfig(
    level=getattr(logging, SETTINGS.log_level, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger("bot_busca_preco")


# =============================================================================
# Banco de dados
# =============================================================================


class Base(DeclarativeBase):
    pass


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class User(Base):
    __tablename__ = "bot_users"

    telegram_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str] = mapped_column(String(128), default="")
    first_name: Mapped[str] = mapped_column(String(128), default="")
    cep: Mapped[str] = mapped_column(String(9), default="")
    city: Mapped[str] = mapped_column(String(128), default="")
    state: Mapped[str] = mapped_column(String(8), default="")
    plan: Mapped[str] = mapped_column(String(32), default="free")
    premium_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    month_key: Mapped[str] = mapped_column(String(7), default="")
    monthly_used: Mapped[int] = mapped_column(Integer, default=0)
    bonus_credits: Mapped[int] = mapped_column(Integer, default=0)
    pending_action: Mapped[str] = mapped_column(String(64), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class SearchJob(Base):
    __tablename__ = "search_jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    protocol: Mapped[str] = mapped_column(String(12), unique=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("bot_users.telegram_id"), index=True)
    chat_id: Mapped[int] = mapped_column(BigInteger)
    query: Mapped[str] = mapped_column(Text)
    mode: Mapped[str] = mapped_column(String(32), default="product")
    image_file_id: Mapped[str] = mapped_column(String(256), default="")
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    credit_reserved: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SearchHistory(Base):
    __tablename__ = "search_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("bot_users.telegram_id"), index=True)
    protocol: Mapped[str] = mapped_column(String(12), index=True)
    query: Mapped[str] = mapped_column(Text)
    mode: Mapped[str] = mapped_column(String(32))
    report: Mapped[str] = mapped_column(Text)
    sources_json: Mapped[str] = mapped_column(Text, default="[]")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class ProcessedUpdate(Base):
    __tablename__ = "processed_updates"

    update_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


ENGINE_KWARGS: dict[str, Any] = {"pool_pre_ping": True, "future": True}
if SETTINGS.database_url.startswith("sqlite"):
    ENGINE_KWARGS["connect_args"] = {"check_same_thread": False}

ENGINE = create_engine(SETTINGS.database_url, **ENGINE_KWARGS)
SessionLocal = sessionmaker(bind=ENGINE, class_=Session, expire_on_commit=False)


def init_database() -> None:
    Base.metadata.create_all(ENGINE)
    with ENGINE.connect() as connection:
        connection.execute(text("SELECT 1"))
    LOGGER.info("DATABASE_READY")


def cleanup_old_updates() -> None:
    cutoff = utcnow() - timedelta(days=30)
    with SessionLocal.begin() as session:
        session.execute(delete(ProcessedUpdate).where(ProcessedUpdate.created_at < cutoff))


def register_update(update_id: int) -> bool:
    try:
        with SessionLocal.begin() as session:
            if session.get(ProcessedUpdate, update_id) is not None:
                return False
            session.add(ProcessedUpdate(update_id=update_id))
        return True
    except IntegrityError:
        return False


def current_month_key() -> str:
    return datetime.now(SETTINGS.timezone).strftime("%Y-%m")


def is_premium(user: User) -> bool:
    return user.plan == "premium" and (as_utc(user.premium_until) or utcnow()) > utcnow()


def normalize_user_plan(user: User) -> None:
    if user.plan == "premium" and not is_premium(user):
        user.plan = "free"
        user.premium_until = None


def reset_month_if_needed(user: User) -> None:
    key = current_month_key()
    if user.month_key != key:
        user.month_key = key
        user.monthly_used = 0


def ensure_user(user_data: dict[str, Any]) -> dict[str, Any]:
    telegram_id = int(user_data["id"])
    with SessionLocal.begin() as session:
        user = session.get(User, telegram_id)
        if user is None:
            user = User(telegram_id=telegram_id, month_key=current_month_key())
            session.add(user)
        user.username = str(user_data.get("username") or "")[:128]
        user.first_name = str(user_data.get("first_name") or "")[:128]
        normalize_user_plan(user)
        reset_month_if_needed(user)
        session.flush()
        return user_snapshot(user)


def user_snapshot(user: User) -> dict[str, Any]:
    premium = is_premium(user)
    allowance = SETTINGS.basic_monthly_quota + max(user.bonus_credits, 0)
    remaining: int | None = None if premium else max(allowance - user.monthly_used, 0)
    return {
        "telegram_id": user.telegram_id,
        "username": user.username,
        "first_name": user.first_name,
        "cep": user.cep,
        "city": user.city,
        "state": user.state,
        "plan": "premium" if premium else "free",
        "premium_until": as_utc(user.premium_until),
        "blocked": user.blocked,
        "monthly_used": user.monthly_used,
        "remaining": remaining,
        "pending_action": user.pending_action,
    }


def get_user_snapshot(user_id: int) -> dict[str, Any] | None:
    with SessionLocal.begin() as session:
        user = session.execute(
            select(User).where(User.telegram_id == user_id).with_for_update()
        ).scalar_one_or_none()
        if user is None:
            return None
        normalize_user_plan(user)
        reset_month_if_needed(user)
        return user_snapshot(user)


def set_pending_action(user_id: int, action: str) -> None:
    with SessionLocal.begin() as session:
        user = session.get(User, user_id)
        if user is not None:
            user.pending_action = action[:64]


def set_user_cep(user_id: int, cep: str, city: str, state: str) -> None:
    with SessionLocal.begin() as session:
        user = session.get(User, user_id)
        if user is None:
            return
        user.cep = cep[:9]
        user.city = city[:128]
        user.state = state[:8]
        user.pending_action = ""


def reserve_job(
    *, user_id: int, chat_id: int, query: str, mode: str, image_file_id: str = ""
) -> dict[str, Any]:
    with SessionLocal.begin() as session:
        user = session.execute(
            select(User).where(User.telegram_id == user_id).with_for_update()
        ).scalar_one_or_none()
        if user is None:
            return {"ok": False, "reason": "user_not_found"}

        normalize_user_plan(user)
        reset_month_if_needed(user)

        if user.blocked:
            return {"ok": False, "reason": "blocked"}

        active_count = session.scalar(
            select(func.count(SearchJob.id)).where(
                SearchJob.user_id == user_id,
                SearchJob.status.in_(["pending", "processing"]),
            )
        ) or 0
        if active_count >= SETTINGS.max_active_requests_per_user:
            return {"ok": False, "reason": "active_request"}

        premium = is_premium(user)
        credit_reserved = False
        if not premium:
            allowance = SETTINGS.basic_monthly_quota + max(user.bonus_credits, 0)
            if user.monthly_used >= allowance:
                return {"ok": False, "reason": "quota_exhausted"}
            user.monthly_used += 1
            credit_reserved = True

        job_id = str(uuid.uuid4())
        protocol = secrets.token_hex(4).upper()
        job = SearchJob(
            id=job_id,
            protocol=protocol,
            user_id=user_id,
            chat_id=chat_id,
            query=query,
            mode=mode,
            image_file_id=image_file_id,
            credit_reserved=credit_reserved,
        )
        user.pending_action = ""
        session.add(job)
        session.flush()

        remaining = None
        if not premium:
            remaining = max(
                SETTINGS.basic_monthly_quota + max(user.bonus_credits, 0) - user.monthly_used,
                0,
            )
        return {"ok": True, "job_id": job_id, "protocol": protocol, "remaining": remaining}


def claim_next_job() -> dict[str, Any] | None:
    with SessionLocal.begin() as session:
        statement = (
            select(SearchJob)
            .where(SearchJob.status == "pending")
            .order_by(SearchJob.created_at.asc())
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        job = session.execute(statement).scalar_one_or_none()
        if job is None:
            return None
        job.status = "processing"
        job.attempts += 1
        job.started_at = utcnow()
        job.error = ""
        session.flush()
        return {
            "id": job.id,
            "protocol": job.protocol,
            "user_id": job.user_id,
            "chat_id": job.chat_id,
            "query": job.query,
            "mode": job.mode,
            "image_file_id": job.image_file_id,
            "attempts": job.attempts,
            "credit_reserved": job.credit_reserved,
        }


def complete_job(job_id: str, report: str, sources: list[dict[str, str]]) -> None:
    with SessionLocal.begin() as session:
        job = session.get(SearchJob, job_id)
        if job is None:
            return
        job.status = "completed"
        job.completed_at = utcnow()
        job.error = ""
        session.add(
            SearchHistory(
                user_id=job.user_id,
                protocol=job.protocol,
                query=job.query,
                mode=job.mode,
                report=report,
                sources_json=json.dumps(sources, ensure_ascii=False),
            )
        )


def fail_or_retry_job(job_id: str, error_message: str) -> str:
    """Retorna retry ou failed."""
    with SessionLocal.begin() as session:
        job = session.get(SearchJob, job_id)
        if job is None:
            return "failed"
        job.error = error_message[:4000]
        if job.attempts < SETTINGS.job_max_attempts:
            job.status = "pending"
            job.started_at = None
            return "retry"

        job.status = "failed"
        job.completed_at = utcnow()
        if job.credit_reserved:
            user = session.execute(
                select(User).where(User.telegram_id == job.user_id).with_for_update()
            ).scalar_one_or_none()
            if user is not None:
                reset_month_if_needed(user)
                user.monthly_used = max(user.monthly_used - 1, 0)
            job.credit_reserved = False
        return "failed"


def recover_stale_jobs() -> dict[str, int]:
    cutoff = utcnow() - timedelta(minutes=SETTINGS.stale_job_minutes)
    recovered = 0
    failed = 0
    with SessionLocal.begin() as session:
        jobs = session.scalars(
            select(SearchJob).where(
                SearchJob.status == "processing",
                SearchJob.started_at < cutoff,
            )
        ).all()
        for job in jobs:
            if job.attempts < SETTINGS.job_max_attempts:
                job.status = "pending"
                job.started_at = None
                job.error = "Recuperado após reinicialização ou interrupção do serviço"
                recovered += 1
            else:
                job.status = "failed"
                job.completed_at = utcnow()
                failed += 1
                if job.credit_reserved:
                    user = session.execute(
                        select(User).where(User.telegram_id == job.user_id).with_for_update()
                    ).scalar_one_or_none()
                    if user is not None:
                        reset_month_if_needed(user)
                        user.monthly_used = max(user.monthly_used - 1, 0)
                    job.credit_reserved = False
    return {"recovered": recovered, "failed": failed}


def recent_history(user_id: int, limit: int) -> list[dict[str, Any]]:
    with SessionLocal() as session:
        rows = session.scalars(
            select(SearchHistory)
            .where(SearchHistory.user_id == user_id)
            .order_by(SearchHistory.created_at.desc())
            .limit(limit)
        ).all()
        return [
            {
                "protocol": row.protocol,
                "query": row.query,
                "mode": row.mode,
                "created_at": as_utc(row.created_at),
            }
            for row in rows
        ]


def activate_premium(user_id: int, days: int) -> bool:
    with SessionLocal.begin() as session:
        user = session.execute(
            select(User).where(User.telegram_id == user_id).with_for_update()
        ).scalar_one_or_none()
        if user is None:
            return False
        current = as_utc(user.premium_until)
        base = current if current and current > utcnow() else utcnow()
        user.plan = "premium"
        user.premium_until = base + timedelta(days=days)
        user.blocked = False
        return True


def set_blocked(user_id: int, blocked: bool) -> bool:
    with SessionLocal.begin() as session:
        user = session.get(User, user_id)
        if user is None:
            return False
        user.blocked = blocked
        return True


def add_bonus_credits(user_id: int, quantity: int) -> bool:
    with SessionLocal.begin() as session:
        user = session.execute(
            select(User).where(User.telegram_id == user_id).with_for_update()
        ).scalar_one_or_none()
        if user is None:
            return False
        user.bonus_credits = max(user.bonus_credits + quantity, 0)
        return True


def admin_stats() -> dict[str, int]:
    with SessionLocal() as session:
        users = session.scalar(select(func.count(User.telegram_id))) or 0
        premium = session.scalar(
            select(func.count(User.telegram_id)).where(
                User.plan == "premium", User.premium_until > utcnow()
            )
        ) or 0
        pending = session.scalar(
            select(func.count(SearchJob.id)).where(SearchJob.status == "pending")
        ) or 0
        processing = session.scalar(
            select(func.count(SearchJob.id)).where(SearchJob.status == "processing")
        ) or 0
        completed = session.scalar(
            select(func.count(SearchJob.id)).where(SearchJob.status == "completed")
        ) or 0
        failed = session.scalar(
            select(func.count(SearchJob.id)).where(SearchJob.status == "failed")
        ) or 0
        return {
            "users": int(users),
            "premium": int(premium),
            "pending": int(pending),
            "processing": int(processing),
            "completed": int(completed),
            "failed": int(failed),
        }


def database_health() -> dict[str, Any]:
    started = utcnow()
    with ENGINE.connect() as connection:
        connection.execute(text("SELECT 1"))
    stats = admin_stats()
    latency_ms = int((utcnow() - started).total_seconds() * 1000)
    return {"database": "ok", "latency_ms": latency_ms, "queue": stats}


# =============================================================================
# Telegram
# =============================================================================


class TelegramAPI:
    def __init__(self, token: str) -> None:
        self.token = token
        self.api_base = f"https://api.telegram.org/bot{token}"
        self.file_base = f"https://api.telegram.org/file/bot{token}"
        self.client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=15.0))

    async def close(self) -> None:
        if self.client is not None:
            await self.client.aclose()
            self.client = None

    def _client(self) -> httpx.AsyncClient:
        if self.client is None:
            raise RuntimeError("Cliente Telegram não inicializado")
        return self.client

    async def call(self, method: str, payload: dict[str, Any] | None = None) -> Any:
        response = await self._client().post(f"{self.api_base}/{method}", json=payload or {})
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram {method}: {data.get('description', 'erro desconhecido')}")
        return data.get("result")

    async def send_message(
        self,
        chat_id: int,
        text_value: str,
        *,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str | None = "HTML",
        disable_preview: bool = True,
    ) -> Any:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text_value,
            "disable_web_page_preview": disable_preview,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return await self.call("sendMessage", payload)

    async def answer_callback(self, callback_query_id: str, text_value: str = "") -> None:
        payload: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text_value:
            payload["text"] = text_value[:200]
        try:
            await self.call("answerCallbackQuery", payload)
        except Exception:
            LOGGER.exception("TELEGRAM_CALLBACK_ANSWER_FAILED")

    async def send_document(
        self, chat_id: int, filename: str, content: bytes, caption: str = ""
    ) -> Any:
        data = {"chat_id": str(chat_id)}
        if caption:
            data["caption"] = caption[:1024]
        files = {"document": (filename, io.BytesIO(content), "text/plain")}
        response = await self._client().post(
            f"{self.api_base}/sendDocument", data=data, files=files, timeout=60.0
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(payload.get("description", "Falha ao enviar documento"))
        return payload.get("result")

    async def get_file_bytes(self, file_id: str, max_bytes: int) -> tuple[bytes, str]:
        result = await self.call("getFile", {"file_id": file_id})
        file_path = str(result.get("file_path") or "")
        if not file_path:
            raise RuntimeError("Telegram não retornou o caminho da imagem")
        response = await self._client().get(f"{self.file_base}/{file_path}", timeout=60.0)
        response.raise_for_status()
        if len(response.content) > max_bytes:
            raise ValueError("A imagem excede o limite permitido")
        suffix = Path(file_path).suffix.lower()
        mime = {
            ".png": "image/png",
            ".webp": "image/webp",
            ".gif": "image/gif",
        }.get(suffix, "image/jpeg")
        return response.content, mime


TELEGRAM = TelegramAPI(SETTINGS.telegram_bot_token)
OPENAI_CLIENT: AsyncOpenAI | None = None
WORKER_TASK: asyncio.Task[None] | None = None
STOP_EVENT = asyncio.Event()
RATE_LIMIT_BUCKETS: dict[int, list[float]] = {}


def consume_rate_limit(user_id: int) -> bool:
    """Limite simples por usuário para proteger o webhook contra abuso."""
    now = time.monotonic()
    cutoff = now - SETTINGS.rate_limit_window_seconds
    bucket = [timestamp for timestamp in RATE_LIMIT_BUCKETS.get(user_id, []) if timestamp >= cutoff]
    if len(bucket) >= SETTINGS.rate_limit_requests:
        RATE_LIMIT_BUCKETS[user_id] = bucket
        return False
    bucket.append(now)
    RATE_LIMIT_BUCKETS[user_id] = bucket
    return True


def inline_keyboard(rows: list[list[dict[str, str]]]) -> dict[str, Any]:
    return {"inline_keyboard": rows}


def button(text_value: str, *, callback: str | None = None, url: str | None = None) -> dict[str, str]:
    result = {"text": text_value}
    if callback is not None:
        result["callback_data"] = callback[:64]
    elif url is not None:
        result["url"] = url
    else:
        raise ValueError("Botão precisa de callback ou URL")
    return result


def main_menu(user: dict[str, Any]) -> dict[str, Any]:
    premium = user.get("plan") == "premium"
    revenda_text = "🧰 Revenda avançada" if premium else "🔒 Revenda Premium"
    premium_text = "⭐ Meu Premium" if premium else f"⭐ Premium R$ {SETTINGS.premium_monthly_price_brl:.2f}"
    return inline_keyboard(
        [
            [button("🔥 Promoções agora", callback="promo"), button("🔎 Buscar produto", callback="search")],
            [button("🏷️ Cupons", callback="coupon"), button("📂 Categorias", callback="categories")],
            [button("🕘 Histórico", callback="history"), button("👤 Minha conta", callback="account")],
            [button(revenda_text, callback="resale"), button(premium_text, callback="plans")],
            [button("❓ Ajuda", callback="help"), button("📤 Compartilhar", callback="share")],
        ]
    )


def categories_keyboard() -> dict[str, Any]:
    return inline_keyboard(
        [
            [button("🌐 Todas", callback="cat:todas"), button("📱 Eletrônicos", callback="cat:eletronicos")],
            [button("🏠 Casa e cozinha", callback="cat:casa"), button("🛠️ Ferramentas", callback="cat:ferramentas")],
            [button("🚗 Automotivo", callback="cat:automotivo"), button("💻 Informática", callback="cat:informatica")],
            [button("🧴 Beleza", callback="cat:beleza"), button("✍️ Digitar produto", callback="search")],
            [button("🏠 Menu principal", callback="menu")],
        ]
    )


def plans_keyboard() -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    if SETTINGS.license_payment_url:
        rows.append([button("💳 Ativar Premium", url=SETTINGS.license_payment_url)])
    else:
        rows.append([button("💬 Falar com suporte", callback="support")])
    rows.append([button("🏠 Menu principal", callback="menu")])
    return inline_keyboard(rows)


def resale_keyboard() -> dict[str, Any]:
    return inline_keyboard(
        [
            [button("💰 Oportunidades", callback="resale:opportunities"), button("📦 Revenda Amazon", callback="resale:amazon")],
            [button("🏪 Fornecedores", callback="resale:suppliers"), button("📊 Marketplaces", callback="resale:marketplaces")],
            [button("🔍 Análise profunda", callback="resale:deep"), button("🔄 Atualizar análise", callback="resale:update")],
            [button("🏠 Menu principal", callback="menu")],
        ]
    )


def after_report_keyboard(sources: list[dict[str, str]]) -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    for index, source in enumerate(sources[:6], start=1):
        url = source.get("url", "")
        if not url.startswith(("http://", "https://")):
            continue
        title = clean_button_title(source.get("title") or f"Fonte {index}")
        rows.append([button(f"🛒 {title}", url=url)])
    rows.append([button("🔥 Nova pesquisa", callback="search"), button("🏠 Menu", callback="menu")])
    rows.append([button("⭐ Ver Premium", callback="plans"), button("📤 Compartilhar", callback="share")])
    return inline_keyboard(rows)


def clean_button_title(value: str) -> str:
    value = re.sub(r"\s+", " ", value).strip()
    if len(value) > 48:
        value = value[:45].rstrip() + "..."
    return value or "Abrir fonte"


def split_text(value: str, limit: int = 3900) -> list[str]:
    value = value.strip()
    if len(value) <= limit:
        return [value]
    chunks: list[str] = []
    current = ""
    for paragraph in value.split("\n"):
        candidate = f"{current}\n{paragraph}".strip() if current else paragraph
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
        while len(paragraph) > limit:
            cut = paragraph.rfind(" ", 0, limit)
            if cut < limit // 2:
                cut = limit
            chunks.append(paragraph[:cut].strip())
            paragraph = paragraph[cut:].strip()
        current = paragraph
    if current:
        chunks.append(current)
    return chunks


async def send_long_report(
    chat_id: int, protocol: str, report: str, sources: list[dict[str, str]]
) -> None:
    chunks = split_text(report)
    max_message_chunks = 4
    for index, chunk in enumerate(chunks[:max_message_chunks]):
        prefix = f"Relatório {protocol}\n\n" if index == 0 else ""
        reply_markup = after_report_keyboard(sources) if index == min(len(chunks), max_message_chunks) - 1 else None
        await TELEGRAM.send_message(
            chat_id,
            prefix + chunk,
            parse_mode=None,
            reply_markup=reply_markup,
        )

    if len(chunks) > max_message_chunks or len(report) > 14_000:
        await TELEGRAM.send_document(
            chat_id,
            f"relatorio_{protocol}.txt",
            report.encode("utf-8"),
            caption=f"Relatório completo • protocolo {protocol}",
        )


# =============================================================================
# Segurança de consulta
# =============================================================================


RESTRICTED_PATTERNS = [
    r"\b(arma|armas|pistola|rev[oó]lver|fuzil|espingarda|muni[cç][aã]o|silenciador)\b",
    r"\b(taser|spray de pimenta|soco ingl[eê]s|canivete autom[aá]tico|switchblade)\b",
    r"\b(coca[ií]na|crack|maconha|cannabis|thc|lsd|ecstasy|mdma|cogumelo alucin[oó]geno)\b",
    r"\b(vape|cigarro eletr[oô]nico|nicotina|tabaco|cigarro)\b",
    r"\b(aposta|bet|cassino|roleta|jogo de azar|prediction market)\b",
    r"\b(explosivo|dinamite|granada|p[oó]lvora|bomba caseira)\b",
    r"\b(pornografia|conte[uú]do adulto|sex shop|vibrador|dildo)\b",
]


def query_is_restricted(query: str) -> bool:
    normalized = query.casefold()
    return any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in RESTRICTED_PATTERNS)


# =============================================================================
# Pesquisa OpenAI
# =============================================================================


def research_instructions(mode: str, user: dict[str, Any]) -> str:
    location = "Brasil"
    if user.get("city") and user.get("state"):
        location = f"{user['city']}/{user['state']}, Brasil"

    common = f"""
Você é o analista do Caçador de Promoções, especializado no mercado brasileiro.
A data local deve ser considerada no momento da pesquisa. O destino é {location}, CEP {user.get('cep') or 'não informado'}.

Regras obrigatórias:
1. Pesquise a web antes de concluir. Priorize páginas atuais da loja, fabricante, marketplace e fontes reconhecidas.
2. Compare exatamente o mesmo produto, modelo, voltagem, capacidade, cor e condição. Se houver divergência, sinalize.
3. Não invente preço, cupom, estoque, frete, avaliação, taxa, margem ou URL.
4. Diferencie dado confirmado de estimativa. Frete que depende do carrinho deve ser marcado como pendente de confirmação.
5. Calcule custo final = preço + frete - desconto confirmado. Mostre as contas de forma simples.
6. Não use a expressão “menor preço da internet” sem evidência ampla. Prefira “melhor oferta válida encontrada na pesquisa”.
7. Verifique sinais de confiabilidade da loja e riscos como marketplace terceiro, produto recondicionado, importação e ausência de nota fiscal.
8. Inclua links diretos e acessíveis para as ofertas e fontes usadas.
9. Não analise nem indique produtos ilegais, armas, drogas, nicotina, jogos de azar, explosivos, conteúdo adulto ou itens perigosos/restritos.
10. Responda em português do Brasil, de forma objetiva, com valores em reais.
"""

    if mode in {"resale", "suppliers", "marketplaces", "deep", "update"}:
        extra = f"""
Produza uma análise de revenda com:
- produto e especificação exata;
- fornecedor e preço de compra verificável;
- frete e custo total de aquisição;
- preço praticado nos marketplaces relevantes;
- taxas do canal, tributos e outros custos, sempre identificando premissas;
- lucro líquido unitário, margem líquida, ROI e ponto de equilíbrio;
- giro esperado apenas como hipótese, nunca como garantia;
- concorrência, riscos e restrições de marca/categoria;
- quantidade inicial sugerida para teste, respeitando capital padrão de R$ {SETTINGS.default_capital_brl:,.2f};
- meta mínima de margem de {SETTINGS.default_margin_percent:.1f}% e ROI de {SETTINGS.default_roi_percent:.1f}%;
- alocação máxima indicativa de {SETTINGS.default_max_product_allocation_percent:.1f}% do capital por produto;
- decisão final: DESCARTAR, MONITORAR ou TESTAR, com justificativa.
Não anuncie lucro garantido. Se faltarem taxas, preço ou evidência de demanda, diga claramente que a oportunidade não está validada.
"""
    elif mode == "coupon":
        extra = """
Pesquise cupons vigentes e condições de uso. Separe cupom confirmado, possível cupom de primeira compra e promoção sem código. Não apresente código expirado como válido.
"""
    elif mode == "promotions":
        extra = """
Busque promoções reais e atuais. Para cada destaque, informe preço, referência de mercado, desconto estimado, custo final quando possível, confiabilidade e decisão APROVEITAR, COMPARAR ou EVITAR.
"""
    else:
        extra = """
Estruture o relatório com: produto exato, melhor oferta válida, outras ofertas comparáveis, preço, cupom, frete, custo final, referência de mercado, economia estimada, confiabilidade, riscos e decisão APROVEITAR, COMPARAR ou EVITAR.
"""
    return textwrap.dedent(common + extra).strip()


def build_openai_input(query: str, image_data_url: str | None) -> list[dict[str, Any]]:
    content: list[dict[str, Any]] = [{"type": "input_text", "text": query}]
    if image_data_url:
        content.append({"type": "input_image", "image_url": image_data_url, "detail": "auto"})
    return [{"role": "user", "content": content}]


def collect_urls(value: Any, output: list[dict[str, str]]) -> None:
    if isinstance(value, dict):
        url = value.get("url")
        if isinstance(url, str) and url.startswith(("http://", "https://")):
            title = value.get("title") or value.get("site_name") or value.get("domain") or "Fonte consultada"
            output.append({"title": str(title), "url": url})
        for child in value.values():
            collect_urls(child, output)
    elif isinstance(value, list):
        for child in value:
            collect_urls(child, output)


def extract_sources(response: Any) -> list[dict[str, str]]:
    raw = response.model_dump(mode="json") if hasattr(response, "model_dump") else response
    found: list[dict[str, str]] = []
    collect_urls(raw, found)
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in found:
        url = item["url"].strip()
        if url in seen:
            continue
        seen.add(url)
        deduped.append({"title": clean_button_title(item.get("title", "Fonte")), "url": url})
        if len(deduped) >= 15:
            break
    return deduped


def append_sources(report: str, sources: list[dict[str, str]]) -> str:
    if not sources:
        return report.strip() + "\n\nFontes diretas não puderam ser extraídas automaticamente. Confirme as ofertas antes da compra."
    lines = [report.strip(), "", "FONTES CONSULTADAS"]
    for index, source in enumerate(sources[:12], start=1):
        lines.append(f"{index}. {source['title']} — {source['url']}")
    return "\n".join(lines)


async def run_research(job: dict[str, Any], user: dict[str, Any]) -> tuple[str, list[dict[str, str]]]:
    if OPENAI_CLIENT is None:
        raise RuntimeError("Cliente OpenAI não inicializado")

    image_data_url: str | None = None
    if job.get("image_file_id"):
        image_bytes, mime = await TELEGRAM.get_file_bytes(
            job["image_file_id"], SETTINGS.max_image_bytes
        )
        encoded = base64.b64encode(image_bytes).decode("ascii")
        image_data_url = f"data:{mime};base64,{encoded}"

    search_context = "high" if job["mode"] in {"deep", "resale", "suppliers", "marketplaces", "update"} else SETTINGS.openai_search_context
    tool: dict[str, Any] = {
        "type": "web_search",
        "search_context_size": search_context,
    }
    if user.get("city"):
        tool["user_location"] = {
            "type": "approximate",
            "country": "BR",
            "city": user["city"],
            "region": user.get("state") or "",
        }

    response = await OPENAI_CLIENT.responses.create(
        model=SETTINGS.openai_model,
        instructions=research_instructions(job["mode"], user),
        tools=[tool],
        tool_choice="auto",
        input=build_openai_input(job["query"], image_data_url),
        max_output_tokens=SETTINGS.openai_max_output_tokens,
    )
    report = (response.output_text or "").strip()
    if not report:
        raise RuntimeError("A OpenAI não retornou texto para a pesquisa")
    sources = extract_sources(response)
    return append_sources(report, sources), sources


# =============================================================================
# Fluxos do bot
# =============================================================================


async def lookup_cep(cep: str) -> tuple[str, str, str] | None:
    digits = re.sub(r"\D", "", cep)
    if len(digits) != 8:
        return None
    formatted = f"{digits[:5]}-{digits[5:]}"
    client = TELEGRAM._client()
    try:
        response = await client.get(f"https://viacep.com.br/ws/{digits}/json/", timeout=15.0)
        response.raise_for_status()
        data = response.json()
        if data.get("erro"):
            return None
        city = str(data.get("localidade") or "")
        state = str(data.get("uf") or "")
        return formatted, city, state
    except Exception:
        LOGGER.exception("CEP_LOOKUP_FAILED cep=%s", formatted)
        return formatted, "", ""


async def send_welcome(chat_id: int, user: dict[str, Any]) -> None:
    if not user.get("cep"):
        await asyncio.to_thread(set_pending_action, user["telegram_id"], "cep")
        await TELEGRAM.send_message(
            chat_id,
            "🔥 <b>Bem-vindo ao Caçador de Promoções</b>\n\n"
            "Eu comparo preços, verifico o desconto real e considero o frete para o seu destino.\n\n"
            "📍 Para começar, envie seu CEP.\nExemplo: <code>72400-000</code>",
        )
        return
    await TELEGRAM.send_message(
        chat_id,
        "🔥 <b>Caçador de Promoções</b>\n\nEscolha uma opção para pesquisar.",
        reply_markup=main_menu(user),
    )


async def send_account(chat_id: int, user_id: int) -> None:
    user = await asyncio.to_thread(get_user_snapshot, user_id)
    if user is None:
        return
    if user["plan"] == "premium":
        validity = user["premium_until"].astimezone(SETTINGS.timezone).strftime("%d/%m/%Y")
        usage = "ilimitadas para uso individual"
        plan = f"Premium até {validity}"
    else:
        usage = f"{user['monthly_used']} utilizadas • {user['remaining']} disponíveis"
        plan = "Gratuito"
    location = user.get("cep") or "não informado"
    if user.get("city"):
        location += f" • {user['city']}/{user['state']}"
    await TELEGRAM.send_message(
        chat_id,
        "👤 <b>MINHA CONTA</b>\n\n"
        f"Plano: <b>{html.escape(plan)}</b>\n"
        f"CEP: {html.escape(location)}\n"
        f"Consultas: {html.escape(usage)}\n"
        f"ID: <code>{user_id}</code>",
        reply_markup=inline_keyboard(
            [
                [button("📍 Alterar CEP", callback="change_cep"), button("⭐ Ver Premium", callback="plans")],
                [button("🕘 Histórico", callback="history"), button("💬 Suporte", callback="support")],
                [button("🏠 Menu", callback="menu")],
            ]
        ),
    )


async def send_history(chat_id: int, user_id: int) -> None:
    user = await asyncio.to_thread(get_user_snapshot, user_id)
    if user is None:
        return
    limit = SETTINGS.history_premium_limit if user["plan"] == "premium" else SETTINGS.history_free_limit
    history = await asyncio.to_thread(recent_history, user_id, limit)
    if not history:
        await TELEGRAM.send_message(
            chat_id,
            "🕘 <b>HISTÓRICO</b>\n\nNenhuma pesquisa concluída ainda.",
            reply_markup=inline_keyboard([[button("🔎 Nova pesquisa", callback="search"), button("🏠 Menu", callback="menu")]]),
        )
        return
    lines = ["🕘 <b>HISTÓRICO DE PESQUISAS</b>", ""]
    for item in history:
        dt = item["created_at"].astimezone(SETTINGS.timezone).strftime("%d/%m %H:%M")
        query = html.escape(re.sub(r"\s+", " ", item["query"])[:90])
        lines.append(f"• <code>{item['protocol']}</code> • {dt}\n  {query}")
    await TELEGRAM.send_message(
        chat_id,
        "\n".join(lines),
        reply_markup=inline_keyboard([[button("🔎 Nova pesquisa", callback="search"), button("🏠 Menu", callback="menu")]]),
    )


async def send_plans(chat_id: int) -> None:
    price = f"{SETTINGS.premium_monthly_price_brl:.2f}".replace(".", ",")
    await TELEGRAM.send_message(
        chat_id,
        "⭐ <b>PLANOS DO CAÇADOR DE PROMOÇÕES</b>\n\n"
        f"🆓 <b>GRATUITO</b>\n• {SETTINGS.basic_monthly_quota} consultas por mês\n"
        "• comparação de preços\n• busca por nome, link ou foto\n• frete pelo CEP\n\n"
        "💎 <b>PREMIUM</b>\n• consultas ilimitadas para uso individual\n"
        "• análise profunda\n• fornecedores e atacadistas\n• margem, ROI e ponto de equilíbrio\n"
        "• histórico ampliado\n\n"
        f"💳 <b>R$ {price} por 30 dias</b>\n\n{html.escape(SETTINGS.sales_message)}",
        reply_markup=plans_keyboard(),
    )


async def send_help(chat_id: int) -> None:
    await TELEGRAM.send_message(
        chat_id,
        "❓ <b>COMO USAR</b>\n\n"
        "Envie o nome exato do produto, um link ou uma foto. Informe modelo, voltagem, tamanho e preço observado quando possível.\n\n"
        "Exemplo: <code>Depurador Suggar DE62IX 60 cm inox 220V</code>\n\n"
        "Preços, estoque, cupons e frete podem mudar. Confirme tudo na loja antes de comprar.",
        reply_markup=inline_keyboard([[button("🔎 Buscar produto", callback="search"), button("🏠 Menu", callback="menu")]]),
    )


async def enqueue_search(
    chat_id: int,
    user_id: int,
    query: str,
    mode: str,
    image_file_id: str = "",
) -> None:
    query = re.sub(r"\s+", " ", query).strip()
    if not query and not image_file_id:
        await TELEGRAM.send_message(chat_id, "Informe o produto ou envie uma foto para iniciar a pesquisa.")
        return
    if len(query) > SETTINGS.max_query_chars:
        query = query[: SETTINGS.max_query_chars]
    if query_is_restricted(query):
        await TELEGRAM.send_message(
            chat_id,
            "Não posso pesquisar ou indicar esse tipo de produto. O serviço não atende itens perigosos, ilegais ou sujeitos a restrições especiais.",
            reply_markup=inline_keyboard([[button("🏠 Menu", callback="menu")]]),
        )
        return

    user = await asyncio.to_thread(get_user_snapshot, user_id)
    if user is None:
        return
    if not user.get("cep"):
        await asyncio.to_thread(set_pending_action, user_id, "cep")
        await TELEGRAM.send_message(chat_id, "📍 Antes da pesquisa, envie seu CEP para considerar o frete.")
        return

    premium_modes = {"resale", "suppliers", "marketplaces", "deep", "update"}
    if mode in premium_modes and user["plan"] != "premium":
        await TELEGRAM.send_message(
            chat_id,
            "🔒 Essa análise faz parte do Premium, que libera revenda, fornecedores, marketplaces e análise profunda.",
            reply_markup=plans_keyboard(),
        )
        return

    result = await asyncio.to_thread(
        reserve_job,
        user_id=user_id,
        chat_id=chat_id,
        query=query or "Analise o produto mostrado na imagem e pesquise ofertas equivalentes.",
        mode=mode,
        image_file_id=image_file_id,
    )
    if not result["ok"]:
        reason = result["reason"]
        if reason == "quota_exhausted":
            await TELEGRAM.send_message(
                chat_id,
                f"⚠️ Suas {SETTINGS.basic_monthly_quota} consultas gratuitas deste mês foram utilizadas.\n\n"
                "Você pode aguardar a renovação mensal ou continuar com o Premium.",
                reply_markup=plans_keyboard(),
            )
        elif reason == "active_request":
            await TELEGRAM.send_message(
                chat_id,
                "Já existe uma pesquisa sua em processamento. Aguarde o resultado antes de iniciar outra.",
            )
        elif reason == "blocked":
            await TELEGRAM.send_message(chat_id, f"Seu acesso está suspenso. {html.escape(SETTINGS.support_contact)}")
        else:
            await TELEGRAM.send_message(chat_id, "Não foi possível registrar a pesquisa. Tente novamente.")
        return

    remaining = result.get("remaining")
    quota_line = "Consultas: ilimitadas" if remaining is None else f"Consultas gratuitas restantes: <b>{remaining}</b>"
    await TELEGRAM.send_message(
        chat_id,
        "✅ <b>Pesquisa recebida</b>\n\n"
        f"Protocolo: <code>{result['protocol']}</code>\n{quota_line}\n\n"
        "🔍 Estou comparando preços, frete, cupons e confiabilidade das ofertas.",
    )


async def process_cep_message(chat_id: int, user_id: int, text_value: str) -> bool:
    if not re.fullmatch(r"\s*\d{5}-?\d{3}\s*", text_value):
        return False
    result = await lookup_cep(text_value)
    if result is None:
        await TELEGRAM.send_message(chat_id, "CEP inválido. Envie oito números, por exemplo: <code>72400-000</code>.")
        return True
    cep, city, state = result
    await asyncio.to_thread(set_user_cep, user_id, cep, city, state)
    user = await asyncio.to_thread(get_user_snapshot, user_id)
    location = f"{cep}"
    if city:
        location += f" • {city}/{state}"
    await TELEGRAM.send_message(
        chat_id,
        f"✅ <b>CEP confirmado: {html.escape(location)}</b>\n\n"
        f"Você possui {SETTINGS.basic_monthly_quota} consultas gratuitas por mês.",
        reply_markup=main_menu(user or {}),
    )
    return True


def infer_mode(text_value: str, pending_action: str) -> str:
    pending_map = {
        "search": "product",
        "coupon": "coupon",
        "resale": "resale",
        "suppliers": "suppliers",
        "marketplaces": "marketplaces",
        "deep": "deep",
        "update": "update",
    }
    if pending_action in pending_map:
        return pending_map[pending_action]
    upper = text_value.upper()
    if "MODO PROFUNDO" in upper or "ANÁLISE PROFUNDA" in upper:
        return "deep"
    if "FORNECEDOR" in upper or "ATACADISTA" in upper:
        return "suppliers"
    if "MARKETPLACE" in upper or "REVENDA" in upper or "AMAZON" in upper:
        return "resale"
    if "CUPOM" in upper:
        return "coupon"
    if "PROMO" in upper or "VARREDURA GERAL" in upper:
        return "promotions"
    return "product"


async def handle_admin_command(chat_id: int, user_id: int, text_value: str) -> bool:
    command = text_value.split()[0].lower()
    if not command.startswith("/admin_"):
        return False
    if user_id not in SETTINGS.admin_user_ids:
        await TELEGRAM.send_message(chat_id, "Comando restrito aos administradores.")
        return True

    parts = text_value.split()
    try:
        if command in {"/admin_licenca", "/admin_ativar"}:
            if len(parts) < 3:
                raise ValueError("Uso: /admin_licenca ID dias")
            target, days = int(parts[1]), int(parts[2])
            if days < 1 or days > 3660:
                raise ValueError("Dias deve estar entre 1 e 3660")
            ok = await asyncio.to_thread(activate_premium, target, days)
            message = f"Premium ativado para {target} por {days} dias." if ok else "Usuário não encontrado. Ele precisa iniciar o bot primeiro."
            await TELEGRAM.send_message(chat_id, message)
            if ok:
                try:
                    await TELEGRAM.send_message(target, f"⭐ <b>Seu Premium foi ativado por {days} dias.</b>")
                except Exception:
                    LOGGER.exception("PREMIUM_NOTIFICATION_FAILED user=%s", target)
            return True
        if command == "/admin_bloquear":
            if len(parts) != 2:
                raise ValueError("Uso: /admin_bloquear ID")
            target = int(parts[1])
            ok = await asyncio.to_thread(set_blocked, target, True)
            await TELEGRAM.send_message(chat_id, "Usuário bloqueado." if ok else "Usuário não encontrado.")
            return True
        if command == "/admin_desbloquear":
            if len(parts) != 2:
                raise ValueError("Uso: /admin_desbloquear ID")
            target = int(parts[1])
            ok = await asyncio.to_thread(set_blocked, target, False)
            await TELEGRAM.send_message(chat_id, "Usuário desbloqueado." if ok else "Usuário não encontrado.")
            return True
        if command == "/admin_creditos":
            if len(parts) != 3:
                raise ValueError("Uso: /admin_creditos ID quantidade")
            target, quantity = int(parts[1]), int(parts[2])
            ok = await asyncio.to_thread(add_bonus_credits, target, quantity)
            await TELEGRAM.send_message(chat_id, "Créditos ajustados." if ok else "Usuário não encontrado.")
            return True
        if command == "/admin_stats":
            stats = await asyncio.to_thread(admin_stats)
            await TELEGRAM.send_message(
                chat_id,
                "📊 <b>ESTATÍSTICAS</b>\n\n"
                f"Usuários: {stats['users']}\nPremium: {stats['premium']}\n"
                f"Fila pendente: {stats['pending']}\nProcessando: {stats['processing']}\n"
                f"Concluídas: {stats['completed']}\nFalhas: {stats['failed']}",
            )
            return True
        raise ValueError("Comando administrativo desconhecido")
    except (ValueError, IndexError) as exc:
        await TELEGRAM.send_message(chat_id, html.escape(str(exc)))
        return True


async def handle_message(message: dict[str, Any], user: dict[str, Any]) -> None:
    chat_id = int(message["chat"]["id"])
    user_id = int(user["telegram_id"])
    text_value = str(message.get("text") or message.get("caption") or "").strip()
    photos = message.get("photo") or []
    image_file_id = str(photos[-1].get("file_id") or "") if photos else ""

    if user.get("blocked"):
        await TELEGRAM.send_message(chat_id, f"Seu acesso está suspenso. {html.escape(SETTINGS.support_contact)}")
        return

    if text_value and await handle_admin_command(chat_id, user_id, text_value):
        return

    command = text_value.split()[0].lower() if text_value.startswith("/") else ""
    if command == "/start":
        await send_welcome(chat_id, user)
        return
    if command == "/menu":
        refreshed = await asyncio.to_thread(get_user_snapshot, user_id) or user
        await TELEGRAM.send_message(chat_id, "Escolha uma opção:", reply_markup=main_menu(refreshed))
        return
    if command == "/id":
        await TELEGRAM.send_message(chat_id, f"Seu ID do Telegram é <code>{user_id}</code>.")
        return
    if command in {"/status", "/conta"}:
        await send_account(chat_id, user_id)
        return
    if command == "/historico":
        await send_history(chat_id, user_id)
        return
    if command == "/planos":
        await send_plans(chat_id)
        return
    if command == "/ajuda":
        await send_help(chat_id)
        return

    pending_action = str(user.get("pending_action") or "")
    if pending_action == "cep" or not user.get("cep"):
        if text_value and await process_cep_message(chat_id, user_id, text_value):
            return
        await asyncio.to_thread(set_pending_action, user_id, "cep")
        await TELEGRAM.send_message(chat_id, "📍 Envie seu CEP antes de pesquisar. Exemplo: <code>72400-000</code>.")
        return

    if text_value and await process_cep_message(chat_id, user_id, text_value):
        return

    if not text_value and not image_file_id:
        await TELEGRAM.send_message(chat_id, "Envie o nome, link ou foto de um produto.")
        return

    mode = infer_mode(text_value, pending_action)
    query = text_value or "Analise o produto desta foto, identifique o modelo e pesquise ofertas equivalentes."
    await enqueue_search(chat_id, user_id, query, mode, image_file_id)


async def handle_callback(callback: dict[str, Any], user: dict[str, Any]) -> None:
    callback_id = str(callback["id"])
    data = str(callback.get("data") or "")
    message = callback.get("message") or {}
    chat_id = int((message.get("chat") or {}).get("id") or user["telegram_id"])
    user_id = int(user["telegram_id"])
    await TELEGRAM.answer_callback(callback_id)

    refreshed = await asyncio.to_thread(get_user_snapshot, user_id) or user
    if refreshed.get("blocked"):
        await TELEGRAM.send_message(chat_id, f"Seu acesso está suspenso. {html.escape(SETTINGS.support_contact)}")
        return

    if data == "menu":
        await TELEGRAM.send_message(chat_id, "Escolha uma opção:", reply_markup=main_menu(refreshed))
    elif data == "search":
        await asyncio.to_thread(set_pending_action, user_id, "search")
        await TELEGRAM.send_message(
            chat_id,
            "🔎 <b>Qual produto você procura?</b>\n\nEnvie o nome exato, um link ou uma foto.\n"
            "Exemplo: <code>Air fryer Mondial 5 litros 220V</code>",
        )
    elif data == "coupon":
        await asyncio.to_thread(set_pending_action, user_id, "coupon")
        await TELEGRAM.send_message(chat_id, "🏷️ Envie o produto ou o link da loja para pesquisar cupons atuais.")
    elif data == "categories":
        await TELEGRAM.send_message(chat_id, "🔥 <b>Escolha uma categoria</b>", reply_markup=categories_keyboard())
    elif data == "promo":
        await enqueue_search(
            chat_id,
            user_id,
            "Encontre promoções reais e atuais com boa economia nas principais categorias de varejo, priorizando ofertas confiáveis e com entrega para meu CEP.",
            "promotions",
        )
    elif data.startswith("cat:"):
        category_map = {
            "todas": "todas as categorias de varejo",
            "eletronicos": "eletrônicos",
            "casa": "casa e cozinha",
            "ferramentas": "ferramentas",
            "automotivo": "peças e acessórios automotivos permitidos e seguros",
            "informatica": "informática",
            "beleza": "beleza e cuidados pessoais não restritos",
        }
        category = category_map.get(data.split(":", 1)[1], "varejo")
        await enqueue_search(
            chat_id,
            user_id,
            f"Faça uma varredura de promoções reais e atuais na categoria {category}, com preços, desconto real, frete e links diretos.",
            "promotions",
        )
    elif data == "account":
        await send_account(chat_id, user_id)
    elif data == "history":
        await send_history(chat_id, user_id)
    elif data == "plans":
        await send_plans(chat_id)
    elif data == "help":
        await send_help(chat_id)
    elif data == "support":
        await TELEGRAM.send_message(chat_id, f"💬 {html.escape(SETTINGS.support_contact)}")
    elif data == "change_cep":
        await asyncio.to_thread(set_pending_action, user_id, "cep")
        await TELEGRAM.send_message(chat_id, "📍 Envie o novo CEP.")
    elif data == "share":
        if not SETTINGS.telegram_bot_username:
            await TELEGRAM.send_message(chat_id, "Defina TELEGRAM_BOT_USERNAME no Render para habilitar o compartilhamento.")
            return
        bot_url = f"https://t.me/{SETTINGS.telegram_bot_username}"
        share_text = "Encontrei um bot que compara preços, verifica promoções e considera o frete pelo CEP."
        share_url = f"https://t.me/share/url?url={quote(bot_url, safe='')}&text={quote(share_text, safe='')}"
        await TELEGRAM.send_message(
            chat_id,
            "📤 <b>Compartilhe o Caçador de Promoções</b>",
            reply_markup=inline_keyboard([[button("📤 Enviar para um contato", url=share_url)], [button("🏠 Menu", callback="menu")]]),
        )
    elif data == "resale":
        if refreshed["plan"] != "premium":
            await TELEGRAM.send_message(
                chat_id,
                "🔒 <b>REVENDA AVANÇADA</b>\n\n"
                "Inclui fornecedores, Amazon, marketplaces, margem, ROI, ponto de equilíbrio e cenários de risco.",
                reply_markup=plans_keyboard(),
            )
        else:
            await TELEGRAM.send_message(chat_id, "🧰 <b>FERRAMENTAS PARA REVENDEDORES</b>", reply_markup=resale_keyboard())
    elif data.startswith("resale:"):
        if refreshed["plan"] != "premium":
            await send_plans(chat_id)
            return
        action = data.split(":", 1)[1]
        prompts = {
            "opportunities": ("resale", "Informe produto, categoria, capital disponível e restrições para buscar oportunidades."),
            "amazon": ("resale", "Informe o produto ou categoria que deseja analisar para revenda na Amazon."),
            "suppliers": ("suppliers", "Informe produto ou categoria para pesquisar fornecedores e atacadistas."),
            "marketplaces": ("marketplaces", "Informe o produto para comparar Amazon, Mercado Livre e Shopee."),
            "deep": ("deep", "Envie produto, link ou foto para uma análise profunda."),
            "update": ("update", "Envie o produto ou protocolo e diga o que deve ser atualizado."),
        }
        pending, prompt = prompts.get(action, ("resale", "Informe o produto para análise."))
        await asyncio.to_thread(set_pending_action, user_id, pending)
        await TELEGRAM.send_message(chat_id, prompt)
    else:
        await TELEGRAM.send_message(chat_id, "Opção não reconhecida. Use o menu.", reply_markup=main_menu(refreshed))


async def process_update(update: dict[str, Any]) -> None:
    update_id = int(update.get("update_id", 0))
    if update_id and not await asyncio.to_thread(register_update, update_id):
        LOGGER.info("DUPLICATE_UPDATE update_id=%s", update_id)
        return

    callback = update.get("callback_query")
    message = update.get("message")
    source = callback or message or {}
    telegram_user = source.get("from") or {}
    if not telegram_user.get("id"):
        return

    user = await asyncio.to_thread(ensure_user, telegram_user)
    user_id = int(user["telegram_id"])
    source_message = message or (callback.get("message", {}) if callback else {})
    source_chat = source_message.get("chat", {})
    chat_id = int(source_chat.get("id") or user_id)

    if user_id not in SETTINGS.admin_user_ids and not consume_rate_limit(user_id):
        await TELEGRAM.send_message(
            chat_id,
            "Muitas solicitações em sequência. Aguarde alguns instantes e tente novamente.",
        )
        return

    try:
        if callback:
            await handle_callback(callback, user)
        elif message:
            await handle_message(message, user)
    except Exception:
        LOGGER.exception("UPDATE_PROCESSING_FAILED update_id=%s user=%s", update_id, user_id)
        try:
            await TELEGRAM.send_message(
                chat_id,
                "Ocorreu uma falha ao processar a mensagem. Tente novamente pelo menu.",
            )
        except Exception:
            LOGGER.exception("ERROR_MESSAGE_DELIVERY_FAILED")


# =============================================================================
# Worker interno
# =============================================================================


async def process_job(job: dict[str, Any]) -> None:
    user = await asyncio.to_thread(get_user_snapshot, int(job["user_id"]))
    if user is None:
        state = await asyncio.to_thread(fail_or_retry_job, job["id"], "Usuário não encontrado")
        LOGGER.error("JOB_USER_NOT_FOUND protocol=%s state=%s", job["protocol"], state)
        return

    try:
        LOGGER.info(
            "JOB_STARTED protocol=%s user=%s mode=%s attempt=%s",
            job["protocol"],
            job["user_id"],
            job["mode"],
            job["attempts"],
        )
        report, sources = await run_research(job, user)
        await asyncio.to_thread(complete_job, job["id"], report, sources)
        await send_long_report(int(job["chat_id"]), job["protocol"], report, sources)
        LOGGER.info("JOB_COMPLETED protocol=%s sources=%s", job["protocol"], len(sources))
    except Exception as exc:
        LOGGER.exception("JOB_FAILED protocol=%s", job["protocol"])
        state = await asyncio.to_thread(fail_or_retry_job, job["id"], f"{type(exc).__name__}: {exc}")
        if state == "retry":
            try:
                await TELEGRAM.send_message(
                    int(job["chat_id"]),
                    f"A pesquisa <code>{job['protocol']}</code> encontrou uma instabilidade e será tentada novamente automaticamente.",
                )
            except Exception:
                LOGGER.exception("RETRY_NOTICE_FAILED")
        else:
            try:
                await TELEGRAM.send_message(
                    int(job["chat_id"]),
                    f"Não foi possível concluir a pesquisa <code>{job['protocol']}</code>. O crédito foi devolvido. Tente novamente mais tarde.",
                    reply_markup=inline_keyboard([[button("🔎 Nova pesquisa", callback="search"), button("🏠 Menu", callback="menu")]]),
                )
            except Exception:
                LOGGER.exception("FAILURE_NOTICE_FAILED")


async def worker_loop() -> None:
    LOGGER.info("INTERNAL_WORKER_READY")
    while not STOP_EVENT.is_set():
        try:
            job = await asyncio.to_thread(claim_next_job)
            if job is None:
                try:
                    await asyncio.wait_for(STOP_EVENT.wait(), timeout=SETTINGS.job_poll_seconds)
                except asyncio.TimeoutError:
                    pass
                continue
            await process_job(job)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("WORKER_LOOP_ERROR")
            await asyncio.sleep(min(SETTINGS.job_poll_seconds * 2, 10))
    LOGGER.info("INTERNAL_WORKER_STOPPED")


# =============================================================================
# Webhook, API e ciclo de vida
# =============================================================================


async def configure_telegram() -> None:
    if not SETTINGS.telegram_bot_token:
        LOGGER.warning("TELEGRAM_DISABLED token ausente")
        return
    commands = [
        {"command": "start", "description": "Iniciar o Caçador de Promoções"},
        {"command": "menu", "description": "Abrir o menu principal"},
    ]
    await TELEGRAM.call("setMyCommands", {"commands": commands})

    if not SETTINGS.webhook_enabled:
        LOGGER.warning("WEBHOOK_DISABLED_BY_ENV")
        return
    if not SETTINGS.public_base_url:
        LOGGER.warning("WEBHOOK_NOT_CONFIGURED PUBLIC_BASE_URL/RENDER_EXTERNAL_HOSTNAME ausente")
        return
    webhook_url = f"{SETTINGS.public_base_url}/telegram/webhook"
    await TELEGRAM.call(
        "setWebhook",
        {
            "url": webhook_url,
            "secret_token": SETTINGS.telegram_webhook_secret,
            "allowed_updates": ["message", "callback_query"],
            "drop_pending_updates": False,
        },
    )
    LOGGER.info("TELEGRAM_WEBHOOK_READY url=%s", webhook_url)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global OPENAI_CLIENT, WORKER_TASK
    await asyncio.to_thread(init_database)
    await asyncio.to_thread(cleanup_old_updates)
    recovery = await asyncio.to_thread(recover_stale_jobs)
    LOGGER.info("JOB_RECOVERY recovered=%s failed=%s", recovery["recovered"], recovery["failed"])

    await TELEGRAM.start()
    OPENAI_CLIENT = AsyncOpenAI(
        api_key=SETTINGS.openai_api_key or None,
        timeout=float(SETTINGS.openai_timeout_seconds),
        max_retries=1,
    )
    try:
        await configure_telegram()
    except Exception:
        LOGGER.exception("TELEGRAM_CONFIGURATION_FAILED")
        if SETTINGS.environment == "production":
            raise

    STOP_EVENT.clear()
    WORKER_TASK = asyncio.create_task(worker_loop(), name="internal-search-worker")
    LOGGER.info("APPLICATION_READY environment=%s model=%s", SETTINGS.environment, SETTINGS.openai_model)
    try:
        yield
    finally:
        STOP_EVENT.set()
        if WORKER_TASK is not None:
            WORKER_TASK.cancel()
            try:
                await WORKER_TASK
            except asyncio.CancelledError:
                pass
        if OPENAI_CLIENT is not None:
            await OPENAI_CLIENT.close()
        await TELEGRAM.close()
        LOGGER.info("APPLICATION_STOPPED")


app = FastAPI(title="BOT-BUSCADOR-PRECO", version="3.0.0-single-file", lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "BOT-BUSCADOR-PRECO", "status": "online", "version": "3.0.0-single-file"}


@app.get("/health")
async def health() -> JSONResponse:
    try:
        result = await asyncio.to_thread(database_health)
        return JSONResponse({"status": "ok", **result})
    except Exception as exc:
        LOGGER.exception("HEALTHCHECK_FAILED")
        return JSONResponse({"status": "error", "detail": str(exc)}, status_code=503)


@app.get("/admin/status")
async def protected_status(x_project_api_key: str | None = Header(default=None)) -> dict[str, Any]:
    if not SETTINGS.project_api_key:
        raise HTTPException(status_code=404, detail="Endpoint não habilitado")
    if not secrets.compare_digest(x_project_api_key or "", SETTINGS.project_api_key):
        raise HTTPException(status_code=401, detail="API key inválida")
    return await asyncio.to_thread(database_health)


@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str | None = Header(default=None),
) -> dict[str, bool]:
    if not SETTINGS.telegram_webhook_secret:
        raise HTTPException(status_code=503, detail="Webhook não configurado")
    if not secrets.compare_digest(
        x_telegram_bot_api_secret_token or "", SETTINGS.telegram_webhook_secret
    ):
        raise HTTPException(status_code=403, detail="Segredo do webhook inválido")

    try:
        update = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="JSON inválido") from exc
    if not isinstance(update, dict):
        raise HTTPException(status_code=400, detail="Atualização inválida")

    # A maior parte do fluxo apenas registra a pesquisa e responde rapidamente.
    await process_update(update)
    return {"ok": True}


def install_signal_handlers() -> None:
    def _stop(*_: Any) -> None:
        STOP_EVENT.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _stop)
        except (ValueError, OSError):
            pass


if __name__ == "__main__":
    import uvicorn

    install_signal_handlers()
    port = env_int("PORT", 10000, 1)
    uvicorn.run("bot_busca_preco:app", host="0.0.0.0", port=port, log_level=SETTINGS.log_level.lower())
