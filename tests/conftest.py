import pytest
import pytest_asyncio
from dishka import AsyncContainer, Container, make_async_container, make_container

from dishka_faststream import FastStreamProvider

from .common import AppProvider, CallbackProvider


@pytest.fixture()
def app_provider() -> AppProvider:
    return AppProvider()


@pytest.fixture()
def async_container(app_provider: AppProvider) -> AsyncContainer:
    return make_async_container(app_provider)


@pytest_asyncio.fixture()
async def async_callback_container(
    app_provider: AppProvider,
) -> AsyncContainer:
    return make_async_container(
        app_provider,
        CallbackProvider(),
        FastStreamProvider(),
    )


@pytest.fixture()
def callback_container(app_provider: AppProvider) -> Container:
    return make_container(
        app_provider,
        CallbackProvider(),
        FastStreamProvider(),
    )
