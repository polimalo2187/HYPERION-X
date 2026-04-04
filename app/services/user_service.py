from __future__ import annotations

from app.database import get_admin_trade_stats, get_admin_visual_stats


def get_admin_overview() -> dict:
    visual = get_admin_visual_stats() or {}
    trade_stats = get_admin_trade_stats(720) or {}
    return {
        'visual': visual,
        'trade_stats_30d': trade_stats,
    }
