"""Half-Kelly bet sizing and P&L helpers for the money simulation."""

from polymarket_validator import config


def half_kelly_fraction(label: str, p_hat: float, p_market: float) -> float:
    """Compute the Half-Kelly fraction of bankroll to bet.

    Polymarket binary market model:
      - Buy 'UP' shares at price p_market.  Pays $1 if UP, $0 if DOWN.
      - Net odds for UP: b = (1 - p_market) / p_market
      - Kelly: f* = (p_win * b - (1 - p_win)) / b
      - Half-Kelly: f = f* / 2

    Args:
        label:    'UP', 'DOWN', or 'SKIP'
        p_hat:    model P(UP)
        p_market: market mid P(UP)

    Returns:
        Fraction in [0, KELLY_MAX_FRACTION].  0 = skip bet.
    """
    if label == "UP":
        p_win = p_hat
        p_side = p_market        # price of the UP share
    elif label == "DOWN":
        p_win = 1.0 - p_hat     # our P(DOWN)
        p_side = 1.0 - p_market  # price of the DOWN share
    else:
        return 0.0

    if p_side <= 0.01 or p_side >= 0.99:
        return 0.0

    b = (1.0 - p_side) / p_side  # net odds (profit per $1 staked if win)

    f_star = (p_win * b - (1.0 - p_win)) / b  # full Kelly
    f = f_star / 2.0                           # half Kelly

    f = max(0.0, min(f, config.KELLY_MAX_FRACTION))

    if f < config.MIN_KELLY_FRACTION:
        return 0.0

    return f


def compute_pnl(label: str, outcome: float, bet_size: float, p_market: float) -> float:
    """Compute the dollar P&L for a resolved bet.

    Args:
        label:     'UP', 'DOWN', or 'SKIP'
        outcome:   1.0 = UP resolved, 0.0 = DOWN resolved
        bet_size:  dollars staked
        p_market:  market mid P(UP) at prediction time

    Returns:
        Positive = profit, negative = loss.  0 for SKIP or zero-bet.
    """
    if label not in ("UP", "DOWN") or bet_size <= 0:
        return 0.0

    if label == "UP":
        p_side = p_market
        won = outcome == 1.0
    else:  # DOWN
        p_side = 1.0 - p_market
        won = outcome == 0.0

    if p_side <= 0.01 or p_side >= 0.99:
        return 0.0

    b = (1.0 - p_side) / p_side  # net odds

    return bet_size * b if won else -bet_size
