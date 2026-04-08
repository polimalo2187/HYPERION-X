from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.api.dependencies import require_session
from app.payment_service import cancel_payment_order, confirm_payment_order, create_payment_order
from app.services.billing_service import get_billing_overview, serialize_order_public
from app.services.user_service import get_user_profile

router = APIRouter(prefix='/api/v1/billing', tags=['billing'])


class PaymentOrderCreateRequest(BaseModel):
    days: int


class PaymentOrderActionRequest(BaseModel):
    order_id: str


@router.get('')
def billing(session: dict = Depends(require_session)) -> dict:
    user_id = int(session['user_id'])
    overview = get_billing_overview(user_id)
    overview['user'] = get_user_profile(user_id)
    return overview


@router.get('/order')
def billing_active_order(session: dict = Depends(require_session)) -> dict:
    return {'order': get_billing_overview(int(session['user_id'])).get('active_order')}


@router.post('/order')
def billing_create_order(payload: PaymentOrderCreateRequest, session: dict = Depends(require_session)) -> dict:
    try:
        order = create_payment_order(int(session['user_id']), int(payload.days))
        return {'ok': True, 'order': serialize_order_public(order)}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


@router.post('/order/confirm')
def billing_confirm_order(payload: PaymentOrderActionRequest, session: dict = Depends(require_session)) -> dict:
    result = confirm_payment_order(payload.order_id, int(session['user_id']))
    order = result.get('order')
    verification = result.get('verification') or {}
    response = {
        'ok': bool(result.get('ok')),
        'reason': result.get('reason'),
        'message': result.get('message'),
        'order': serialize_order_public(order),
        'verification': verification,
    }
    if response['ok']:
        response['user'] = get_user_profile(int(session['user_id']))
    return response


@router.post('/order/cancel')
def billing_cancel_order(payload: PaymentOrderActionRequest, session: dict = Depends(require_session)) -> dict:
    cancelled = cancel_payment_order(payload.order_id, int(session['user_id']))
    if not cancelled:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail='No se pudo cancelar la orden')
    return {'ok': True, 'message': 'Orden cancelada correctamente'}
