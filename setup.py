#!/usr/bin/env python
from setuptools import setup

setup(
    name='eeUtil',
    version='0.1',
    description='Python wrapper for easier data management on Google Earth Engine.',
    license='MIT',
    author='Francis Gassert',
    url='https://github.com/fgassert/eeUtil',
    packages=['eeUtil'],
    install_requires=[
        'earthengine-api',
        'google-cloud-storage'
    ]
)
