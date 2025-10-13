import warnings

from dishka import FromDishka
from faststream.__about__ import (
    __version__ as FASTSTREAM_VERSION,  # noqa: N812
)

FASTSTREAM_05 = FASTSTREAM_VERSION.startswith("0.5")
FASTSTREAM_06 = FASTSTREAM_VERSION.startswith("0.6")

if FASTSTREAM_05:
    from .faststream_05 import FastStreamProvider, inject, setup_dishka
    warnings.warn(
        (
            "Support for version FastStream 0.5.* will be removed "
            "in dishka-faststream 0.6.0."
        ),
        category=RuntimeWarning,
        stacklevel=2,
    )

elif FASTSTREAM_06:
    from .faststream_06 import (  # type: ignore[assignment]
        FastStreamProvider,
        inject,
        setup_dishka,
    )
else:
    raise RuntimeError(  # noqa: TRY003
        f"FastStream {FASTSTREAM_VERSION} version not supported",
    )

__all__ = (
    "FastStreamProvider",
    "FromDishka",
    "inject",
    "setup_dishka",
)
