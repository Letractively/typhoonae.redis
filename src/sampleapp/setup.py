"""Setup script."""

import os
from setuptools import setup, find_packages


setup(
    name='sampleapp',
    version='1',
    author="Tobias Rodaebel",
    author_email="tobias dot rodaebel at googlemail dot com",
    description="TyphoonAE Redis Datastore Sample Application .",
    long_description=(),
    license="Apache License 2.0",
    keywords=["appengine", "gae", "typhoonae", "wsgi", "redis"],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Application',
        ],
    url='',
    packages=find_packages(os.sep.join(['src', 'sampleapp'])),
    include_package_data=True,
    package_dir={'': os.sep.join(['src', 'sampleapp'])},
    install_requires=[
        'setuptools',
        ],
    zip_safe=False,
)
