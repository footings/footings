import nox

PYTHON_TEST_VERSIONS = ["3.7", "3.8"]


@nox.session(python=PYTHON_TEST_VERSIONS, venv_backend="conda")
def update_environments(session):
    session.run(
        "conda",
        "env",
        "update",
        "--prefix",
        session.virtualenv.location,
        "--file",
        "environments/footings-dev.yml",
        # options
        silent=False,
    )


@nox.session(python=PYTHON_TEST_VERSIONS, venv_backend="conda")
def test(session):
    session.install("-e", ".", "--no-deps")
    session.run("pytest", "-vv")


@nox.session(venv_backend="none")
def test_ci(session):
    session.install("-e", ".", "--no-deps")
    session.run("pytest", "-vv")


@nox.session(venv_backend="none")
def coverage_ci(session):
    session.install("-e", ".", "--no-deps")
    session.run("pytest", "--cov=./", "--cov-report=xml")


@nox.session(python="3.7", venv_backend="none")
def docs(session):
    session.install(".")
    session.run("sphinx-build", "-E", "-v", "-b", "html", "docs", "docs/_build")
    session.run("rm", "-r", "docs/generated/")
    session.run("rm", "-r", "docs/jupyter_execute/")


@nox.session(python="3.7", venv_backend="none")
def changelog(session):
    session.run(
        "auto-changelog",
        "--output",
        "docs/changelog.md",
        "--unreleased",
        "true",
        "--commit-limit",
        "false",
    )
