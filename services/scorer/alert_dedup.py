"""
Redis-backed per-entity alert cooldown.

Prevents alert storms when a stream remains anomalous across many consecutive
windows.  The implementation is a single atomic Redis SET NX EX command —
there is no TOCTOU race even with multiple concurrent scoring replicas because
the set-if-not-exists and expiry are applied atomically by the server.

Failure behaviour: if the Redis command raises, should_alert() returns True
(fail open).  It is safer to over-alert than to silently suppress alerts when
the dedup store is unavailable.
"""

import logging

import redis

logger = logging.getLogger(__name__)

_KEY_PREFIX = "alert:cooldown"


class AlertDedup:
    """
    Per-(model, entity) alert rate-limiter backed by Redis TTL keys.

    Usage
    -----
    dedup = AlertDedup(redis_client, cooldown_s=300)
    if dedup.should_alert(model_name, entity_id):
        publisher.publish_alert(...)
    """

    def __init__(self, redis_client: redis.Redis, cooldown_s: int) -> None:
        self._redis = redis_client
        self._cooldown_s = cooldown_s

    def should_alert(self, model_name: str, entity_id: str) -> bool:
        """
        Returns True if an alert should be fired for this (model, entity) pair
        and sets the cooldown key as a side-effect.

        Returns False if the key already exists (entity is in cooldown).
        """
        key = f"{_KEY_PREFIX}:{model_name}:{entity_id}"
        try:
            result = self._redis.set(key, "1", nx=True, ex=self._cooldown_s)
            return result is not None
        except redis.RedisError:
            logger.warning(
                "alert dedup Redis error for %s / %s — failing open (allowing alert)",
                model_name,
                entity_id,
            )
            return True
