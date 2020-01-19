# Copyright (C) 2019 Dustin Tindall

import setuptools

DESCRIPTION = "Actuarial modeling with Dask and the wider PyData ecosystem"
LONG_DESCRIPTION = """\
"""

NAME = "footings"
AUTHOR = "Dustin Tindall"
AUTHOR_EMAIL = "dustin.tindall@gmail.com"
LICENSE = "BSD (3-clause)"
VERSION = "0.0.0"
URL = "https://github.com/dustindall/footings-core"
DOWNLOAD_URL = "https://github.com/dustindall/footings-core"

INSTALL_REQUIRES = ["numpy", "pandas", "dask"]

PACKAGES = setuptools.find_packages()

CLASSIFIERS = [
    "Intended Audience :: Actuaries",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "License :: OSI Approved :: BSD License",
]

setuptools.setup(
    name=NAME,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    maintainer=AUTHOR,
    maintainer_email=AUTHOR_EMAIL,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    license=LICENSE,
    version=VERSION,
    url=URL,
    download_url=DOWNLOAD_URL,
    install_requires=INSTALL_REQUIRES,
    packages=PACKAGES,
    classifiers=CLASSIFIERS,
)
