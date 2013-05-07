#!/usr/bin/python

from setuptools import setup, find_packages

setup(
    name="fasttrack",
    license='Apache License 2.0',
    version="0.1.0",
    description="Library for efficiently adding analytics to your project.",
    long_description=open('README.rst', 'r').read(),
    author="Nathan Pinger",
    author_email="npinger@ycharts.com",
    url="",
    packages=find_packages(exclude=['tests']),
    test_suite='nose.collector',
    install_requires=[
        'redis==2.6.0',
        'python-dateutil==1.5',
    ],
    tests_require=[
        'nose>=1.0',
    ],
    classifiers=[
        "Intended Audience :: Developers",
        'Intended Audience :: System Administrators',
        "Programming Language :: Python",
        "Topic :: Software Development",
        "Topic :: Utilities",
    ],
)
