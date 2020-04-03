"""Package setup"""

from setuptools import setup

with open("README.md") as f:
    README = f.read()

AUTHOR = "Dustin Tindall"
AUTHOR_EMAIL = "dustin.tindall@gmail.com"

INSTALL_REQUIRES = ["attrs"]
EXTRAS_REQUIRE = {}
EXTRAS_REQUIRE["docs"] = []
EXTRAS_REQUIRE["tests"] = ["pytest"]
EXTRAS_REQUIRE["dev"] = EXTRAS_REQUIRE["tests"] + EXTRAS_REQUIRE["docs"] + ["pre-commit"]
CLASSIFIERS = [
    "Intended Audience :: Actuaries",
    "License :: OSI Approved :: BSD License",
]

setup(
    name="footings",
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    maintainer=AUTHOR,
    maintainer_email=AUTHOR_EMAIL,
    description="A DAG Based Model Building Library",
    long_description=README,
    long_description_content_type="text/markdown",
    license="BSD (3-clause)",
    version="0.1.0",
    url="https://github.com/dustindall/footings-core",
    packages=("footings",),
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    classifiers=CLASSIFIERS,
    include_package_data=True,
    test_suite="tests",
)
