#!/usr/bin/env python
#import ez_setup

#ez_setup.use_setuptools()

from setuptools import setup, find_packages
import sys

python_version = sys.version_info

install_deps = []
'''
if python_version[0] == 2:
    if python_version[1] in [6,7]:
        install_deps.append('argparse >= 1.2.1')
install_deps.append('pip>=8.0.1')
install_deps.append('paramiko>=2.1.1')
'''
with open('requirements.txt') as file_requirements:
    install_deps = file_requirements.read().splitlines()

setup(name='Tstat Analyzer',
      version='0.1.1',
      description='Tstat analyzer using Elasticsearch and MySQL',
      author='Devarshi Ghoshal',
      author_email='dghoshal@lbl.gov',
      keywords='',
      packages=find_packages(exclude=['ez_setup']),
      include_package_data=True,
      zip_safe=False,
      classifiers=['Development Status :: 1 - Alpha',
                   'Intended Audience :: Science/Research',
                   'Natural Language :: English',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python :: 2.7',
                   'Topic :: Scientific/Engineering',
                   'License :: OSI Approved :: BSD License'
      ],
      install_requires=install_deps,
      entry_points={'console_scripts': ['tsa = tsa:main']},
      data_files=[('resources', ['resources/column.types']),
                    ('config',['config/config.ini']),
                    ('', ['setup.py'])]
)
