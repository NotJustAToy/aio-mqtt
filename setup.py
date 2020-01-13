#!/usr/bin/env python
import os
import imp
import setuptools

with open('README.rst', 'rb') as readme_file:
    README = readme_file.read().decode('utf-8')

VERSION = imp.load_source('version', os.path.join('.', 'aio_mqtt', 'version.py'))
VERSION = VERSION.__version__

setuptools.setup(
    name='aio-mqtt',
    version=VERSION,
    description="Asynchronous MQTT client for 3.1.1 protocol version.",
    long_description=README,
    author='Not Just A Toy Corp.',
    author_email='dev@notjustatoy.com',
    url='https://github.com/NotJustAToy/aio-mqtt',
    packages=setuptools.find_packages(exclude=('tests', 'tests.*')),
    keywords='mqtt asyncio',
    zip_safe=False,
    include_package_data=True,
    license='Apache License 2.0',
    python_requires='>=3.6.0',
    tests_require=[
        'pytest>=3.1.1,<4',
        'pytest-cov>=2.3.1',
        'pytest-flake8>=0.8.1',
        'pytest-mypy>=0.4.0',
        'safety',
        'piprot',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Communications',
        'Topic :: Internet',
    ]
)
