# Copyright (c) 2017-2018 Neogeo-Technologies.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


from onegeo_manager import __version__
from setuptools import find_packages
from setuptools import setup


version = str(__version__)

setup(
    name='onegeo-manager',
    version=version,
    description='Onegeo Manager',
    author='Neogeo Technologies',
    author_email='contact@neogeo.fr',
    url='https://github.com/neogeo-technologies/onegeo-manager',
    license='Apache License, Version 2.0',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'],
    packages=find_packages(where='.'),
    install_requires=[
        'geojson>=2.3.0,<2.4.0',
        'numpy>=1.14.0,<1.15.0',
        'OWSLib>=0.16.0',
        'requests>=2.13.0',
        'xmltodict>=0.11,<0.12'])
