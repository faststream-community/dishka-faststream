import pytest
from dishka import AsyncContainer, make_async_container

from .common import AppProvider


@pytest.fixture()
def app_provider() -> AppProvider:
    return AppProvider()


@pytest.fixture()
def async_container(app_provider: AppProvider) -> AsyncContainer:
    return make_async_container(app_provider)
