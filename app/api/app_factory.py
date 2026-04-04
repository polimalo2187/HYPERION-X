from __future__ import annotations

import logging
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes_admin import router as admin_router
from app.api.routes_auth import router as auth_router
from app.api.routes_health import router as health_router
from app.api.routes_user import router as user_router
from app.config import MINIAPP_ALLOWED_ORIGINS

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    app = FastAPI(
        title='Trading X Hiper Pro MiniApp API',
        version='1.0.0',
        docs_url='/docs',
        redoc_url='/redoc',
    )

    allow_origins = MINIAPP_ALLOWED_ORIGINS or ['*']
    allow_credentials = '*' not in allow_origins
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=allow_credentials,
        allow_methods=['*'],
        allow_headers=['*'],
    )

    @app.middleware('http')
    async def add_request_context(request: Request, call_next):
        request_id = request.headers.get('x-request-id') or str(uuid.uuid4())
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            logger.exception('API request failed request_id=%s path=%s', request_id, request.url.path)
            raise
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        response.headers['x-request-id'] = request_id
        response.headers['x-response-time-ms'] = str(elapsed_ms)
        logger.info('API %s %s -> %s in %sms request_id=%s', request.method, request.url.path, response.status_code, elapsed_ms, request_id)
        return response

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(_: Request, exc: RuntimeError):
        return JSONResponse(status_code=500, content={'detail': str(exc)})

    @app.get('/')
    def root() -> dict:
        return {
            'service': 'trading-x-hiper-pro-miniapp-api',
            'status': 'ok',
            'docs': '/docs',
            'health': '/health',
        }

    app.include_router(health_router)
    app.include_router(auth_router)
    app.include_router(user_router)
    app.include_router(admin_router)
    return app
