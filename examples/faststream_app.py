from faststream import ContextRepo, FastStream
from faststream.nats import NatsBroker, NatsMessage

from dishka import Provider, Scope, make_async_container, provide
from dishka_faststream import (
    FastStreamProvider,
    FromDishka,
    setup_dishka,
    wrap_callback,
)


class A:
    def __init__(self) -> None:
        pass


class B:
    def __init__(self, a: A) -> None:
        self.a = a


class ErrorHandler:
    def __init__(self, b: B, context: ContextRepo) -> None:
        self.b = b
        self.context = context

    async def handle(self, error: Exception) -> None:
        print(f"Broker error: {error!r}; dependency: {self.b!r}")


class MyProvider(Provider):
    @provide(scope=Scope.APP)
    def get_a(self) -> A:
        return A()

    @provide(scope=Scope.REQUEST)
    def get_b(self, a: A) -> B:
        return B(a)

    @provide(scope=Scope.REQUEST)
    def get_error_handler(self, b: B, context: ContextRepo) -> ErrorHandler:
        return ErrorHandler(b, context)


async def error_callback(
    error: Exception,
    error_handler: FromDishka[ErrorHandler],
) -> None:
    await error_handler.handle(error)


provider = MyProvider()
container = make_async_container(provider, FastStreamProvider())
context = ContextRepo()

broker = NatsBroker(
    context=context,
    error_cb=wrap_callback(
        callback=error_callback,
        container=container,
        context=context,
    ),
)
app = FastStream(broker, context=context)
setup_dishka(container, app, auto_inject=True)


@broker.subscriber("test")
async def handler(
    msg: str,
    a: FromDishka[A],
    b: FromDishka[B],
    raw_message: FromDishka[NatsMessage],
    faststream_context: FromDishka[ContextRepo],
) -> None:
    print(msg, a, b)


@app.after_startup
async def t() -> None:
    await broker.publish("test", "test")
