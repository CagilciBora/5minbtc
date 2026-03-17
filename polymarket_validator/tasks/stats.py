"""Async loop: prints accuracy/calibration table every 60 seconds."""

import asyncio

from polymarket_validator import config
from polymarket_validator.storage.db import Database
from polymarket_validator.utils.logger import get_logger

log = get_logger("stats")


def _compute_stats(resolved: list[dict]) -> str:
    """Compute and format stats from resolved predictions."""
    total = len(resolved)
    directional = [r for r in resolved if r["label"] in ("UP", "DOWN")]
    skipped = [r for r in resolved if r["label"] == "SKIP"]

    lines = [
        "",
        "=" * 60,
        "  PREDICTION STATS",
        "=" * 60,
        f"  Total predictions:   {total}",
        f"  Directional (UP/DN): {len(directional)}",
        f"  Skipped:             {len(skipped)}",
    ]

    # Directional accuracy
    if directional:
        correct = sum(
            1
            for r in directional
            if (r["label"] == "UP" and r["outcome"] == 1.0)
            or (r["label"] == "DOWN" and r["outcome"] == 0.0)
        )
        accuracy = correct / len(directional)
        lines.append(f"  Directional accuracy: {accuracy:.1%} ({correct}/{len(directional)})")
    else:
        lines.append("  Directional accuracy: N/A")

    # Brier score (over all resolved, including SKIP)
    brier_sum = 0.0
    for r in resolved:
        brier_sum += (r["p_hat"] - r["outcome"]) ** 2
    brier = brier_sum / total if total else 0.0
    lines.append(f"  Brier score:          {brier:.4f}")

    # Edge-stratified accuracy
    for threshold in config.EDGE_THRESHOLDS:
        high_edge = [
            r
            for r in directional
            if abs(r["p_hat"] - r["p_market"]) > threshold
        ]
        if high_edge:
            correct_he = sum(
                1
                for r in high_edge
                if (r["label"] == "UP" and r["outcome"] == 1.0)
                or (r["label"] == "DOWN" and r["outcome"] == 0.0)
            )
            acc_he = correct_he / len(high_edge)
            lines.append(
                f"  Accuracy |edge|>{threshold:.2f}: {acc_he:.1%} ({correct_he}/{len(high_edge)})"
            )
        else:
            lines.append(f"  Accuracy |edge|>{threshold:.2f}: N/A (no samples)")

    lines.append("=" * 60)
    return "\n".join(lines)


async def run_stats(db: Database):
    """Periodically print stats once enough markets are resolved."""
    log.info("Stats reporter started (interval=%ds)", config.STATS_PRINT_INTERVAL)

    while True:
        try:
            resolved = await db.get_resolved()
            if len(resolved) >= config.STATS_MIN_RESOLVED:
                report = _compute_stats(resolved)
                log.info(report)
            else:
                log.debug(
                    "Stats: %d/%d resolved (waiting for more)",
                    len(resolved),
                    config.STATS_MIN_RESOLVED,
                )
        except asyncio.CancelledError:
            log.info("Stats reporter cancelled")
            return
        except Exception:
            log.exception("Stats error")

        await asyncio.sleep(config.STATS_PRINT_INTERVAL)
