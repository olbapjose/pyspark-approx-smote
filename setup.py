#
# Copyright (c) 2019 by Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from codecs import open
from os import path
from setuptools import setup, find_packages

# Read the long description from README.MD
here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pyspark_approx_smote',
    version='0.1',
    description='Pyspark wrapper of Spark (Scala) Approx SMOTE oversampling algorithm',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/olbapjose/pyspark-approx-smote',
    author='Pablo J. Villacorta',
    author_email='pjvi@decsai.ugr.es',
    classifiers=[
        # Project Maturity
        'Development Status :: 2 - Pre-Alpha',

        # Intended Users
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        # License
        'License :: OSI Approved :: Apache Software License',

        # Supported Python Versions
        'Programming Language :: Python :: 3.6',
    ],
    keywords='pyspark smote approx-smote',
    packages=find_packages(),
    include_package_data=False
)
