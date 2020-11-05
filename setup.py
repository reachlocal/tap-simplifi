#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-simplifi",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_simplifi"],
    install_requires=[
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-simplifi=tap_simplifi:main
    """,
    packages=["tap_simplifi"],
    package_data = {
        "schemas": ["tap_simplifi/schemas/*.json"]
    },
    include_package_data=True,
)
