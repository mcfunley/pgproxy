#! /usr/bin/env python
from setuptools import setup, find_packages

setup(
    name = 'PGProxy',
    version = '0.1',
    description = 'PGProxy, a testing proxy for Postgres.',
    author = 'Dan McKinley',
    author_email = 'mcfunley@gmail.com',
    url = 'http://mcfunley.com/',
    package_data = { 'pgproxy': ['service.tac'] },
    packages = find_packages(exclude=['tests'])
)
