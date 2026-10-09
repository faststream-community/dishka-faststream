from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, ParamSpec, TypeVar
from unittest.mock import Mock

import pytest
from dishka import (
    AsyncContainer,
    Container,
    make_async_container,
)
from dishka.integrations.base import InjectFunc
from faststream import ContextRepo, FastStream
from faststream.nats import NatsBroker, TestNatsBroker

from dishka_faststream import (
    FromDishka,
    inject,
    setup_dishka,
    wrap_callback,
)

from .common import (
    APP_DEP_VALUE,
    REQUEST_DEP_VALUE,
    AppDep,
    AppProvider,
    CallbackDependency,
    RequestDep,
)

_ParamsP = ParamSpec("_ParamsP")
_ReturnT = TypeVar("_ReturnT")


@asynccontextmanager
async def dishka_app(
    view: Callable[..., Any],
    provider: AppProvider,
    *,
    auto_inject: bool | InjectFunc[_ParamsP, _ReturnT] = False,
) -> AsyncIterator[NatsBroker]:
    broker = NatsBroker()
    sub = broker.subscriber("test")
    sub(inject(view))

    app = FastStream(broker)

    container = make_async_container(provider)
    setup_dishka(container, app=app, auto_inject=auto_inject)

    async with TestNatsBroker(broker) as br:
        assert isinstance(br, NatsBroker)
        yield br

    await container.close()


async def get_with_app(
    a: FromDishka[AppDep],
    mock: FromDishka[Mock],
) -> str:
    mock(a)
    return "passed"


@pytest.mark.asyncio()
async def test_app_dependency(app_provider: AppProvider) -> None:
    async with dishka_app(get_with_app, app_provider) as client:
        msg = await client.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(APP_DEP_VALUE)
        app_provider.app_released.assert_not_called()
    app_provider.app_released.assert_called()


async def get_with_request(
    a: FromDishka[RequestDep],
    mock: FromDishka[Mock],
) -> str:
    mock(a)
    return "passed"


@pytest.mark.asyncio()
async def test_request_dependency(app_provider: AppProvider) -> None:
    async with dishka_app(get_with_request, app_provider) as client:
        msg = await client.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(REQUEST_DEP_VALUE)
        app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_autoinject_before_subscriber(app_provider: AppProvider) -> None:
    broker = NatsBroker()
    app = FastStream(broker)

    container = make_async_container(app_provider)
    setup_dishka(container, app=app, auto_inject=True)

    sub = broker.subscriber("test")
    sub(get_with_request)

    async with TestNatsBroker(broker) as br:
        assert isinstance(br, NatsBroker)
        msg = await br.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(REQUEST_DEP_VALUE)
        app_provider.request_released.assert_called_once()

    await container.close()


@pytest.mark.asyncio()
async def test_autoinject_after_subscriber(app_provider: AppProvider) -> None:
    broker = NatsBroker()
    app = FastStream(broker)

    sub = broker.subscriber("test")
    sub(get_with_request)

    container = make_async_container(app_provider)
    setup_dishka(container, app=app, auto_inject=True)

    async with TestNatsBroker(broker) as br:
        assert isinstance(br, NatsBroker)
        msg = await br.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(REQUEST_DEP_VALUE)
        app_provider.request_released.assert_called_once()

    await container.close()


@pytest.mark.asyncio()
async def test_faststream_with_broker(app_provider: AppProvider) -> None:
    broker = NatsBroker()

    sub = broker.subscriber("test")
    sub(get_with_request)

    container = make_async_container(app_provider)
    setup_dishka(container, broker=broker, auto_inject=True)

    async with TestNatsBroker(broker) as br:
        assert isinstance(br, NatsBroker)
        msg = await br.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(REQUEST_DEP_VALUE)
        app_provider.request_released.assert_called_once()

    await container.close()


async def handle_for_custom_inject(
    a: FromDishka[AppDep],
    mock: FromDishka[Mock],
) -> str:
    mock(a)
    return "passed"


@pytest.mark.asyncio()
async def test_custom_auto_inject(app_provider: AppProvider) -> None:
    async with dishka_app(
        handle_for_custom_inject,
        app_provider,
        auto_inject=inject,
    ) as client:
        msg = await client.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(APP_DEP_VALUE)
        app_provider.app_released.assert_not_called()
    app_provider.app_released.assert_called()


def sync_error_callback(
    error: Exception,
    dependency: FromDishka[CallbackDependency],
) -> tuple[Exception, CallbackDependency]:
    return error, dependency


def sync_request_callback(request: FromDishka[RequestDep]) -> RequestDep:
    return request


async def async_error_callback(
    error: Exception,
    dependency: FromDishka[CallbackDependency],
) -> tuple[Exception, CallbackDependency]:
    return error, dependency


@pytest.mark.asyncio()
async def test_async_callback_with_context(
    app_provider: AppProvider,
    async_callback_container: AsyncContainer,
) -> None:
    context = ContextRepo()
    broker_error = ValueError("broker error")
    wrapped = wrap_callback(
        callback=async_error_callback,
        container=async_callback_container,
        context=context,
    )

    error, dependency = await wrapped(broker_error)

    assert error is broker_error
    assert dependency.context is context
    assert dependency.request == REQUEST_DEP_VALUE
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_async_callback_without_context(
    app_provider: AppProvider,
    async_callback_container: AsyncContainer,
) -> None:
    wrapped = wrap_callback(
        callback=get_with_request,
        container=async_callback_container,
    )

    assert await wrapped() == "passed"
    app_provider.mock.assert_called_once_with(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


def test_sync_callback_with_context(
    app_provider: AppProvider,
    callback_container: Container,
) -> None:
    context = ContextRepo()
    broker_error = ValueError("broker error")
    wrapped = wrap_callback(
        callback=sync_error_callback,
        container=callback_container,
        context=context,
    )

    error, dependency = wrapped(broker_error)

    assert error is broker_error
    assert dependency.context is context
    assert dependency.request == REQUEST_DEP_VALUE
    app_provider.request_released.assert_called_once()


def test_sync_callback_without_context(
    app_provider: AppProvider,
    callback_container: Container,
) -> None:
    wrapped = wrap_callback(
        callback=sync_request_callback,
        container=callback_container,
    )

    assert wrapped() == REQUEST_DEP_VALUE
    app_provider.request_released.assert_called_once()
