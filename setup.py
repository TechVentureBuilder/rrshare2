# -*- coding: utf-8 -*-
from setuptools import setup

setup_requires = \
['setuptools_scm',
 'setuptools_scm_git_archive'
        ]

packages = \
['rrshare',
 'rrshare.RQSetting',
 'rrshare.rqFactor',
 'rrshare.rqFetch',
 'rrshare.rqSU',
 'rrshare.rqUpdate',
 'rrshare.rqUtil',
 'rrshare.rqWeb',
 'rrshare.sql']

package_data = \
{'': ['*'], 'rrshare': ['templates/*'], 'rrshare.rqWeb': ['templates/*']}

install_requires = \
['click>=7.1.2,<8.0.0',
 'easyquotation>=0.7.4,<0.8.0',
 'streamlit>=0.80.0,<0.88.0',
 'tushare>=1.2.62,<2.0.0',
 'zenlog>=1.1,<2.0']

entry_points = \
{'console_scripts': ['rrshare = rrshare:entry_point']}

setup_kwargs = {
    'name': 'rrshare',

    'setup_requires': 'setuptools_scm',
    'use_scm_version': True,

    'description': 'stock data(from tusharepro) & analysis',
    'long_description': None,
    'author': 'rome',
    'author_email': romepeng@outlook.com,
    'maintainer': None,
    'maintainer_email': None,
    'url': None,
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'entry_points': entry_points,
    'python_requires': '>=3.8,<4.0',
}


setup(**setup_kwargs)
