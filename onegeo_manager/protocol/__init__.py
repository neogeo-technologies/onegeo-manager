# Copyright (c) 2017-2019 Neogeo-Technologies.
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


from importlib import import_module
import os


def all():
    p = 'onegeo_manager.protocol'
    return tuple(
        (lambda f: (
            f, import_module('{}.{}'.format(p, f), __name__).__description__)
         )(filename[:-3])
        for filename in os.listdir(os.path.dirname(os.path.realpath(__file__)))
        if filename.endswith('.py') and not filename.startswith('__'))


__all__ = ['all']
