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


from collections import Counter
from functools import wraps
import json
import numpy as np
from onegeo_manager.index_profile import AbstractIndexProfile
from onegeo_manager.index_profile import fetch_mapping
from onegeo_manager.index_profile import not_searchable
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import accumulate
from onegeo_manager.utils import clean_my_obj
from onegeo_manager.utils import iterate
import operator
from pathlib import Path
import re
import requests


__description__ = 'JSON'


class Resource(AbstractResource):

    _path = None

    def __init__(self, source, path=None):
        self._path = path
        super().__init__(source)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self):
        raise AttributeError("Attibute is locked, you can't modify it.")

    @path.deleter
    def path(self):
        raise AttributeError("Attibute is locked, you can't delete it.")

    def get_collection(self):
        yield from self.source._data[self._path]


class Source(AbstractSource):

    def __init__(self, uri):
        super().__init__(uri)

    @property
    def _data(self):
        if self.uri.startswith('file://'):
            p = Path(self.uri[7:])
            if not p.exists():
                raise ConnectionError('The given path does not exist.')
            with open(p) as f:
                return json.load(f)

        if self.uri.startswith('http'):
            r = requests.get(self.uri)
            if r.status_code == 200:
                r.raise_for_status()
            pattern = '^(text|application)\/((\w+)\+?)+\;?((\s?\w+\=[\w\d\D]+);?)*$'
            s = re.search(pattern, r.headers['Content-Type'])
            if s and s.group(2) == 'json':
                return r.json()

    def get_resources(self):

        # Deconstructs json data
        counter = Counter(path for path, value in iterate(self._data))
        # Then returns statistics of this one
        inputted = (
            (v, k)
            for k, v in sorted(counter.items(), key=operator.itemgetter(1)))

        # This is for testing with simple-feature...
        occurs = []
        structures = []
        for occur, parent, children in tuple(accumulate(inputted)):
            occurs.append(occur)
            structures.append(
                children and (parent, children) or (parent,))

        test = tuple(occurs > np.std(occurs))
        resources = []
        for i in [test.index(v) for v in test if v]:
            structure = structures[i]
            resource = Resource(self, path=structure[0])
            for item in structure[1]:
                resource.add_column(item)
            resources.append(resource)
        return resources

    def get_collection(self, resource):
        # Deprecated -> Use Resource.get_collection()
        yield from resource.get_collection()


class IndexProfile(AbstractIndexProfile):

    def __init__(self, name, resource):
        super().__init__(name, resource)

    def _format(fun):

        @wraps(fun)
        def wrapper(self, *args, **kwargs):

            def alias(properties):
                new = {}
                for k, v in properties.items():
                    prop = self.get_property(k)
                    if prop.rejected:
                        continue
                    new[prop.alias or prop.name] = v
                return new

            for record in fun(self, *args, **kwargs):
                yield {
                    'lineage': {
                        # 'resource': {
                        #     'name': self.resource.name},
                        'source': {
                            'protocol': self.resource.source.protocol,
                            'uri': self.resource.source.uri}},
                    'properties': alias(record)}

        return wrapper

    @_format
    def get_collection(self, **opts):
        yield from self.resource.get_collection(**opts)

    def generate_elastic_mapping(self):

        props = {}
        for p in self.iter_properties():
            if not p.rejected:
                props[p.alias or p.name] = fetch_mapping(p)

        return clean_my_obj({
            self.name: {
                'properties': {
                    'lineage': {
                        'properties': {
                            # 'resource': {
                            #     'properties': {
                            #         'name': not_searchable('keyword')}},
                            'source': {
                                'properties': {
                                    'protocol': not_searchable('keyword'),
                                    'uri': not_searchable('keyword')}}}},
                    'properties': {'properties': props}}}})
