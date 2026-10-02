"""
Credential encryption and log redaction.

* API key / secret are encrypted at rest with Fernet (AES-128-CBC + HMAC-SHA256).
* The Fernet master key comes from ``CH_MASTER_KEY``; if absent a key is generated
  once and stored in ``<data_dir>/.master_key`` with 0600 permissions.
* ``SecretRedactingFilter`` scrubs any registered secret (and anything that looks like
  a long MEXC key) from every log record, so keys are never written in plaintext.
"""
from __future__ import annotations

import logging
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Set

from cryptography.fernet import Fernet, InvalidToken


@dataclass(frozen=True)
class Credentials:
    api_key: str
    api_secret: str

    def masked(self) -> str:
        k = self.api_key
        return f"{k[:4]}…{k[-4:]}" if len(k) > 8 else "****"


class MasterKey:
    @staticmethod
    def load_or_create(data_dir: Path, env_value: Optional[str]) -> bytes:
        if env_value:
            return env_value.encode()
        data_dir.mkdir(parents=True, exist_ok=True)
        path = data_dir / ".master_key"
        if path.exists():
            return path.read_bytes().strip()
        key = Fernet.generate_key()
        with open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "wb") as fh:
            fh.write(key)
        try:
            path.chmod(stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            pass
        return key


class Cipher:
    def __init__(self, master_key: bytes):
        self._f = Fernet(master_key)

    def encrypt(self, plaintext: str) -> str:
        return self._f.encrypt(plaintext.encode()).decode()

    def decrypt(self, token: str) -> str:
        try:
            return self._f.decrypt(token.encode()).decode()
        except InvalidToken as exc:  # pragma: no cover - depends on on-disk state
            raise ValueError("Stored credentials cannot be decrypted with the current master key") from exc


class SecretRedactingFilter(logging.Filter):
    """Logging filter replacing registered secrets with ``***``."""

    _KEY_PATTERN = re.compile(r"(?i)(secret|apikey|api_key|signature)[\"']?\s*[:=]\s*[\"']?([A-Za-z0-9\-_]{12,})")

    def __init__(self) -> None:
        super().__init__()
        self._secrets: Set[str] = set()

    def register(self, *values: Optional[str]) -> None:
        for v in values:
            if v and len(v) >= 8:
                self._secrets.add(v)

    def clear(self) -> None:
        self._secrets.clear()

    def redact(self, text: str) -> str:
        for s in self._secrets:
            if s in text:
                text = text.replace(s, "***")
        return self._KEY_PATTERN.sub(lambda m: f"{m.group(1)}=***", text)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:  # pragma: no cover
            return True
        redacted = self.redact(msg)
        if redacted != msg:
            record.msg = redacted
            record.args = ()
        return True


REDACTOR = SecretRedactingFilter()


def redact_all(values: Iterable[Optional[str]]) -> None:
    REDACTOR.register(*values)
