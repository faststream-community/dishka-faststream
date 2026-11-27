from collections.abc import Iterable
from dataclasses import dataclass
from typing import NewType
from unittest.mock import Mock

from dishka import Provider, Scope, from_context, provide
from dishka.entities.depends_marker import FromDishka
from faststream import ContextRepo

ContextDep = NewType("ContextDep", str)
UserDep = NewType("UserDep", str)
StepDep = NewType("StepDep", str)

AppDep = NewType("AppDep", str)
APP_DEP_VALUE = AppDep("APP")

RequestDep = NewType("RequestDep", str)
REQUEST_DEP_VALUE = RequestDep("REQUEST")

WebSocketDep = NewType("WebSocketDep", str)
WS_DEP_VALUE = WebSocketDep("WS")

AppMock = NewType("AppMock", Mock)


@dataclass
class CallbackDependency:
    context: ContextRepo
    request: RequestDep


class CallbackProvider(Provider):
    @provide(scope=Scope.REQUEST)
    def dependency(
        self,
        context: ContextRepo,
        request: RequestDep,
    ) -> CallbackDependency:
        return CallbackDependency(context, request)


class AppProvider(Provider):
    context = from_context(provides=ContextDep, scope=Scope.REQUEST)

    def __init__(self) -> None:
        super().__init__()
        self.app_released = Mock()
        self.request_released = Mock()
        self.websocket_released = Mock()
        self.db_session_released = Mock()
        self.mock = Mock()
        self._app_mock = AppMock(Mock())

    @provide(scope=Scope.APP)
    def app(self) -> Iterable[AppDep]:
        yield APP_DEP_VALUE
        self.app_released()

    @provide(scope=Scope.REQUEST)
    def request(self) -> Iterable[RequestDep]:
        yield REQUEST_DEP_VALUE
        self.request_released()

    @provide(scope=Scope.REQUEST)
    def websocket(self) -> Iterable[WebSocketDep]:
        yield WS_DEP_VALUE
        self.websocket_released()

    @provide(scope=Scope.REQUEST)
    def user(self, context: FromDishka[ContextDep]) -> Iterable[UserDep]:
        yield UserDep(f"user_id.from_context({context})")
        self.db_session_released()

    @provide(scope=Scope.REQUEST)
    def get_mock(self) -> Mock:
        return self.mock

    @provide(scope=Scope.APP)
    def app_mock(self) -> AppMock:
        return self._app_mock

    @provide(scope=Scope.STEP)
    def step(self, request: FromDishka[RequestDep]) -> StepDep:
        return StepDep(f"step for {request}")
