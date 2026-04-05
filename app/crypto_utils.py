from __future__ import annotations

import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken

from app.config import PRIVATE_KEY_ENCRYPTION_SECRET

_CIPHER_VERSION = 'fernet-v1'


def _derive_key(secret: str) -> bytes:
    raw = hashlib.sha256((secret or '').encode('utf-8')).digest()
    return base64.urlsafe_b64encode(raw)


def _get_fernet() -> Fernet:
    return Fernet(_derive_key(PRIVATE_KEY_ENCRYPTION_SECRET))


def encrypt_private_key(value: str) -> tuple[str, str]:
    token = _get_fernet().encrypt((value or '').encode('utf-8')).decode('utf-8')
    return token, _CIPHER_VERSION


def decrypt_private_key(value: str, *, encrypted: bool, version: str | None = None) -> str | None:
    if value is None:
        return None
    if not encrypted:
        return value
    if version not in (None, _CIPHER_VERSION):
        raise RuntimeError(f'Versión de cifrado no soportada: {version}')
    try:
        return _get_fernet().decrypt(value.encode('utf-8')).decode('utf-8')
    except InvalidToken as exc:
        raise RuntimeError('No se pudo descifrar la private key almacenada') from exc
