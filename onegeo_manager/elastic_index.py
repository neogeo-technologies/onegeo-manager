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


__all__ = ['ElasticIndex']


class ElasticIndex(object):

    def __init__(self, name):

        self._name = name
        self._analyzer = None
        self._search_analyzer = None

    @property
    def name(self):
        return self._name

    @property
    def analyzer(self):
        return self._analyzer

    @property
    def search_analyzer(self):
        return self._search_analyzer

    def set_analyzer(self, obj):
        self._analyzer = obj

    def set_search_analyzer(self, obj):
        self._search_analyzer = obj
