import nox

PYTHON_TEST_VERSIONS = ["3.6", "3.7", "3.8"]


@nox.session(python=PYTHON_TEST_VERSIONS, venv_backend="conda")
def tests(session):
    session.run(
        "conda",
        "env",
        "update",
        "--prefix",
        session.virtualenv.location,
        "--file",
        "environment.yml",
        # options
        silent=False,
    )
    session.install(".[testing]")
    session.run("pytest")


@nox.session(python="3.7", venv_backend="conda")
def docs(session):
    session.install(".[docs]")
