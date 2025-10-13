from dataclasses import dataclass

import nox


nox.options.default_venv_backend = "uv"
nox.options.reuse_existing_virtualenvs = True
nox.options.stop_on_first_error = True


DISHKA_VERSIONS = ["1.7.0", None]
PYTHON_VERSIONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]
PYTHON_VERSIONS_LESS_314 = ["3.10", "3.11", "3.12", "3.13"]
PYTHON_VERSIONS_LESS_313 = ["3.10", "3.11", "3.12"]


@dataclass(frozen=True, slots=True)
class IntegrationEnv:
    library: str
    version: str

    def get_req(self) -> str:
        return f"requirements/{self.library.replace('_', '-')}-{self.version}.txt"


FASTSTREAM_ENVS = [
    *[(IntegrationEnv("faststream", "060"), py_version) for py_version in PYTHON_VERSIONS],
    *[(IntegrationEnv("faststream", "latest"), py_version) for py_version in PYTHON_VERSIONS],
]


def install_command(dependency: str, version: str | None = None):
    """Return install command for a specific dependency."""
    return f"{dependency}=={version}" if version else dependency


def load_test_dependencies() -> list[str]:
    """Load development dependencies from pyproject.toml."""
    toml_data = nox.project.load_toml("pyproject.toml")
    return toml_data["dependency-groups"]["test"]


@nox.session()
@nox.parametrize("faststream_env,python", FASTSTREAM_ENVS)
@nox.parametrize("dishka_version", DISHKA_VERSIONS)
def tests(session: nox.Session, faststream_env: IntegrationEnv, dishka_version: str | None):
    """Run tests with different versions of dependencies."""

    session.install(install_command("dishka", dishka_version))
    session.install("-r", faststream_env.get_req())

    dev_deps = load_test_dependencies()
    session.install(*dev_deps)

    session.install("-e", ".")

    session.run(
        "pytest",
        "tests",
        "--cov=dishka_faststream",
        "--cov-report=term-missing",
        "--cov-append",
        "--cov-config=.coveragerc",
        env={
            "COVERAGE_FILE": f".coverage.{session.name}",
        },
        *session.posargs,
    )


@nox.session
def coverage(session: nox.Session) -> None:
    """Generate and view coverage report."""
    session.install("coverage")
    session.run("coverage", "combine")
    session.run("coverage", "report", "--fail-under=80")
