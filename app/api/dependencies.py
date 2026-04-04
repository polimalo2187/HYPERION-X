from __future__ import annotations

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.api.security import verify_session_token

bearer_scheme = HTTPBearer(auto_error=False)


def require_session(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> dict:
    if credentials is None or credentials.scheme.lower() != 'bearer':
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Authorization Bearer requerido')
    return verify_session_token(credentials.credentials)


def require_admin_session(session: dict = Depends(require_session)) -> dict:
    if not session.get('is_admin'):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Acceso de administrador requerido')
    return session
