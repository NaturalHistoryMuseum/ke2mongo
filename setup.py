
import os

try:
    from setuptools import setup
except:
    from distutils.core import setup

version = '0.1'

setup(
    name='ke2mongo',
    version=version,
    description='Import KE Data into Mongo DB',
    author='Ben Scott',
    author_email='ben@benscott.co.uk',
    license='Apache License 2.0',
    packages=[
        'ke2mongo',
    ]
)



