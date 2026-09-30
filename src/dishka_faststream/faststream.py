__all__ = (
    "FastStreamProvider",
    "inject",
    "setup_dishka",
    "wrap_callback",
)

import warnings
from collections.abc import AsyncIterator, Awaitable, Callable
from inspect import Parameter, iscoroutinefunction
from typing import (
    Annotated,
    Any,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
    get_type_hints,
    overload,
)

from dishka import AsyncContainer, Container, Provider, Scope, from_context
from dishka.exception_base import DishkaError
from dishka.integrations.base import InjectFunc, wrap_injection
from faststream import BaseMiddleware, Context, FastStream
from faststream._internal.basic_types import DecodedMessage
from faststream._internal.broker import BrokerUsecase as BrokerType
from faststream.asgi import AsgiFastStream
from faststream.message import StreamMessage

try:
    from faststream.context import ContextRepo
except ImportError:
    # FastStream 0.6 does not expose the ContextRepo class through a public module.
    from faststream._internal.context import ContextRepo

_ReturnT = TypeVar("_ReturnT")
_ParamsP = ParamSpec("_ParamsP")


class FastStreamProvider(Provider):
    context = from_context(ContextRepo, scope=Scope.REQUEST)
    message = from_context(StreamMessage, scope=Scope.REQUEST)


Application: TypeAlias = FastStream | AsgiFastStream

try:
    # import works only if fastapi is installed
    from faststream._internal.fastapi import (
        Context as FastAPIContext,
        StreamRouter,
    )

except ImportError:
    ContextAnnotation: TypeAlias = Annotated[ContextRepo, Context("context")]

else:
    ContextAnnotation = Annotated[  # type: ignore[misc,assignment]
        ContextRepo,
        FastAPIContext("context"),
        Context("context"),
    ]

    Application |= StreamRouter


class ApplicationLike(Protocol):
    broker: BrokerType[Any, Any]


def _wrap_sync_callback(
    callback: Callable[..., _ReturnT],
    container: Container,
    context: ContextRepo,
) -> Callable[..., _ReturnT]:
    return wrap_injection(
        func=callback,
        container_getter=lambda _args, _kwargs: container,
        is_async=False,
        scope=Scope.REQUEST,
        provide_context=lambda _args, _kwargs: {ContextRepo: context},
    )


def _wrap_async_callback(
    callback: Callable[..., _ReturnT],
    container: AsyncContainer,
    context: ContextRepo,
) -> Callable[..., _ReturnT]:
    return wrap_injection(
        func=callback,
        container_getter=lambda _args, _kwargs: container,
        is_async=True,
        scope=Scope.REQUEST,
        provide_context=lambda _args, _kwargs: {ContextRepo: context},
    )


@overload
def wrap_callback(
    *,
    callback: Callable[..., Awaitable[_ReturnT]],
    container: AsyncContainer,
    context: ContextRepo,
) -> Callable[..., Awaitable[_ReturnT]]: ...


@overload
def wrap_callback(
    *,
    callback: Callable[..., _ReturnT],
    container: Container,
    context: ContextRepo,
) -> Callable[..., _ReturnT]: ...


def wrap_callback(
    *,
    callback: Callable[..., _ReturnT],
    container: Container | AsyncContainer,
    context: ContextRepo,
) -> Callable[..., _ReturnT]:
    """Inject callback dependencies in a new REQUEST scope.

    Share the context with the broker and FastStream application.
    StreamMessage context is not available in broker callbacks.
    """
    is_async_callback = iscoroutinefunction(callback)
    if is_async_callback:
        return _wrap_async_callback(
            callback=callback,
            container=cast("AsyncContainer", container),
            context=context,
        )

    if isinstance(container, AsyncContainer):
        msg = "Can't use async container in sync context"
        raise DishkaError(msg)

    return _wrap_sync_callback(
        callback=callback,
        container=container,
        context=context,
    )


def setup_dishka(
    container: AsyncContainer,
    app: "Application | ApplicationLike | None" = None,
    broker: "BrokerType[Any, Any] | None" = None,
    *,
    finalize_container: bool = False,
    auto_inject: bool | InjectFunc[_ParamsP, _ReturnT] = False,
) -> None:
    """Setup dishka integration with FastStream.

    You must provide either app or broker.

    Args:
        container: AsyncContainer instance.
        app: FastStream Application or StreamRouter instance.
        broker: FastStream broker instance.
        finalize_container: bool. Can be used only with app.
        auto_inject: bool or custom inject func.
    """
    if (app and broker) or (not app and not broker):
        raise ValueError(  # noqa: TRY003
            "You must provide either app or broker to setup dishka integration.",
        )

    if finalize_container:
        if getattr(app, "after_shutdown", None):
            app.after_shutdown(container.close)  # type: ignore[union-attr]

        else:
            warnings.warn(
                "For use `finalize_container=True` "
                "you must provide `app: FastStream | AsgiFastStream` "
                "argument.",
                category=RuntimeWarning,
                stacklevel=2,
            )

    broker = broker or getattr(app, "broker", None)
    assert broker  # noqa: S101

    broker.insert_middleware(DishkaMiddleware(container))

    if auto_inject is not False:
        inject_func: InjectFunc[_ParamsP, _ReturnT]
        inject_func = inject if auto_inject is True else auto_inject
        broker.config.fd_config.call_decorators = (
            inject_func,
            *broker.config.fd_config.call_decorators,
        )


class DishkaMiddleware:
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    def __call__(self, *args: Any, **kwargs: Any) -> "_DishkaMiddleware":
        return _DishkaMiddleware(self.container, *args, **kwargs)


class _DishkaMiddleware(BaseMiddleware):
    def __init__(
        self,
        container: AsyncContainer,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.container = container
        super().__init__(*args, **kwargs)

    async def consume_scope(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> AsyncIterator[DecodedMessage]:
        async with self.container(
            {
                StreamMessage: msg,
                ContextRepo: self.context,
                type(msg): msg,
            },
        ) as request_container:
            with self.context.scope("dishka", request_container):
                return cast(
                    "AsyncIterator[DecodedMessage]",
                    await call_next(msg),
                )


def inject(func: Callable[_ParamsP, _ReturnT]) -> Callable[_ParamsP, _ReturnT]:
    param_name = _find_context_param(func)
    if param_name:
        additional_params = []
    else:
        additional_params = [DISHKA_CONTEXT_PARAM]
        param_name = DISHKA_CONTEXT_PARAM.name

    return wrap_injection(
        func=func,
        is_async=True,
        additional_params=additional_params,
        container_getter=lambda _, p: p[param_name].get_local("dishka"),
    )


def _find_context_param(func: Callable[_ParamsP, _ReturnT]) -> str | None:
    hints = get_type_hints(func)
    return next(
        (name for name, hint in hints.items() if hint is ContextRepo),
        None,
    )


DISHKA_CONTEXT_PARAM = Parameter(
    name="dishka_context__",
    annotation=ContextAnnotation,
    kind=Parameter.KEYWORD_ONLY,
)
