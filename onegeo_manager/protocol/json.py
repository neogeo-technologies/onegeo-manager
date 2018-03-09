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


from onegeo_manager.index_profile import AbstractIndexProfile
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import deconstruct
import re
import requests


__description__ = 'JSON'


def get(url):

    r = requests.get(url)
    if r.status_code == 200:
        r.raise_for_status()

    pattern = '^(text|application)\/((\w+)\+?)+\;?((\s?\w+\=[\w\d\D]+);?)*$'
    s = re.search(pattern, r.headers['Content-Type'])
    if s and s.group(2) == 'json':
        return r.json()

    raise Exception


class Resource(AbstractResource):

    def __init__(self, source, name):
        super().__init__(source, name)


def fun_todo(serie):  # TODO
    return (serie[-1][0],)


class Source(AbstractSource):

    def __init__(self, url):
        super().__init__(url)

    def get_resources(self, names=[]):
        tree = deconstruct(get(self.uri))

        resources = []
        structures = fun_todo(tuple((o, len(t)) for o, t in tree))
        for structure in structures:
            resource = Resource(self, 'foo')
            for item in dict(tree).get(structure):
                resource.add_column(item[-1], column_type=None)
            resources.append(resource)
        return resources

    def get_collection(self, resource_name):
        pass


class IndexProfile(AbstractIndexProfile):

    def __init__(self, name, resource):
        super().__init__(name, resource)

    def get_collection(self, **opts):
        pass

    def generate_elastic_mapping(self):
        pass
