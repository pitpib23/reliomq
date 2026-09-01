"""Opt-in, zero-configuration runtime visibility for reliomq.

reliomq logs through the standard library ``logging`` module and, by
default, installs nothing: unless your application configures logging
itself, INFO/DEBUG messages stay invisible (see the package docstring in
``reliomq/__init__.py`` and the README's Logging section for the fully
manual alternative).

This module is the implementation behind ``SenderConfig``/``RelayConfig``'s
``log_level=``/``debug=`` fields: a small convenience so a developer who does
not want to think about Python logging can still see what reliomq is doing.
Calling :func:`enable_logging` attaches one ``StreamHandler`` to the
``"reliomq"`` logger (never to the root logger) and stops it from
propagating further, so turning this on can never duplicate lines through a
handler your application already configured elsewhere. It is idempotent and
thread-safe: call it as many times as you like (every ``Sender``/``Relay``
that has ``log_level``/``debug`` set calls it once at construction time)
and only one handler is ever attached.
"""

from __future__ import annotations

import logging
import sys
import threading
from typing import TextIO


PACKAGE_LOGGER_NAME = "reliomq"

_LEVEL_NAMES: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

_FORMAT = "%(asctime)s %(levelname)-8s %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_lock = threading.Lock()
_handler: logging.Handler | None = None


def normalize_log_level(value: int | str) -> int:
    """Turn ``"INFO"``/``"debug"``/``logging.DEBUG``/``10`` into an int level.

    Raises:
        ValueError: If ``value`` is not a recognized level name or integer.
    """

    if isinstance(value, bool):
        raise ValueError("log_level must be a level name or integer, not a bool")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        key = value.strip().upper()
        if key in _LEVEL_NAMES:
            return _LEVEL_NAMES[key]
    raise ValueError(
        f"log_level must be one of {sorted(_LEVEL_NAMES)} or an integer, got {value!r}"
    )


def enable_logging(level: int | str = logging.INFO, *, stream: TextIO | None = None) -> logging.Logger:
    """Attach a simple stderr handler to reliomq's logger tree, idempotently.

    This is the function ``log_level=``/``debug=True`` on ``SenderConfig``
    and ``RelayConfig`` call for you; call it directly if you want reliomq's
    quick-start logging without going through a config object (for example,
    before constructing anything).

    The handler is attached to the ``"reliomq"`` logger only, and that
    logger's ``propagate`` is turned off, so this can never produce duplicate
    lines through a root/application handler you already configured. If you
    would rather reliomq's records flow into your own logging setup and
    formatting, do not call this -- just set the level yourself::

        logging.getLogger("reliomq").setLevel(logging.DEBUG)

    Returns the ``"reliomq"`` logger, mostly for tests and introspection.
    """

    resolved_level = normalize_log_level(level)
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)

    global _handler
    with _lock:
        if _handler is None:
            handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
            handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))
            package_logger.addHandler(handler)
            package_logger.propagate = False
            _handler = handler
        _handler.setLevel(resolved_level)
        package_logger.setLevel(resolved_level)

    return package_logger
