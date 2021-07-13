#!/usr/bin/env python
from setuptools import setup

with open('README.md') as f:
    desc = f.read()

setup(
    name='eeUtil',
    version='0.3.0',
    description='Python wrapper for easier data management on Google Earth Engine.',
    long_description=desc,
    long_description_content_type='text/markdown',
    license='MIT',
    author='Francis Gassert',
    url='https://github.com/fgassert/eeUtil',
    packages=['eeUtil'],
    install_requires=[
        'earthengine-api>=0.1.232,<0.2',
        'google-cloud-storage>=1.31.1,<2',
        'google-api-core>=1.22.2<2'
    ]
)
