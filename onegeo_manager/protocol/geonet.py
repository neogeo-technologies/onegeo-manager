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


from functools import wraps
from onegeo_manager.index_profile import AbstractIndexProfile
from onegeo_manager.index_profile import fetch_mapping
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import browse2
from onegeo_manager.utils import clean_my_obj
from onegeo_manager.utils import execute_http_get
from onegeo_manager.utils import ResponseConverter
from onegeo_manager.utils import StaticClass
import operator
from urllib.parse import urlparse


class Method(metaclass=StaticClass):

    @staticmethod
    def get(self, url, **params):
        return execute_http_get(url, **params)

    @classmethod
    def search(cls, url, **params):
        return cls.get(cls, url, **params)


class Resource(AbstractResource):

    def __init__(self, source, name):
        super().__init__(source, name)

    def authorized_column_type(self, val):
        return val in operator.add(self.COLUMN_TYPE, ['object'])


class Source(AbstractSource):

    def __init__(self, url, name):
        super().__init__(url, name)

        params = {'fast': 'true', 'from': 0, 'to': 0}
        self.summary = self.__search(**params)['response']['summary']

    def get_resources(self):
        resources = []
        for entry in self.summary['types']['type']:
            resource = Resource(self, entry['@name'])
            if entry['@name'] in ('dataset', 'series', 'service'):
                resource.add_columns(
                    ({'name': 'title',
                      'column_type': 'keyword'},
                     {'name': 'abstract',
                      'column_type': 'text'},
                     {'name': 'lineage',
                      'column_type': 'text'},
                     {'name': 'keyword',
                      'column_type': 'keyword'},
                     {'name': 'category',
                      'column_type': 'keyword',
                      'rule': 'info/category'},
                     {'name': 'updateFrequency',
                      'column_type': 'keyword'},
                     {'name': 'create_date',
                      'column_type': 'date',
                      'rule': 'info/createDate'},
                     {'name': 'change_date',
                      'column_type': 'date',
                      'rule': 'info/changeDate'},
                     {'name': 'popularity',
                      'column_type': 'integer',
                      'rule': 'info/popularity'},
                     {'name': 'publisher',
                      'column_type': 'text',
                      'rule': 'responsibleParty/organisationName'},
                     {'name': 'rights',
                      'column_type': 'keyword',
                      'rule': ('LegalConstraints[@preformatted=false]'
                               '/useLimitation/CharacterString')}))  # ~^(\w+\s*)+$
            if entry['@name'] == 'nonGeographicDataset':
                resource.add_columns(
                    ({'name': 'title',
                      'column_type': 'keyword'},
                     {'name': 'abstract',
                      'column_type': 'text'},
                     {'name': 'keyword',
                      'column_type': 'keyword'},
                     {'name': 'category',
                      'column_type': 'keyword',
                      'rule': 'info/category'},
                     {'name': 'create_date',
                      'column_type': 'date',
                      'rule': 'info/createDate'},
                     {'name': 'change_date',
                      'column_type': 'date',
                      'rule': 'info/changeDate'},
                     {'name': 'popularity',
                      'column_type': 'integer',
                      'rule': 'info/popularity'},
                     {'name': 'publisher',
                      'column_type': 'text'},
                     {'name': 'rights',
                      'column_type': 'keyword'}))

            resources.append(resource)
        return resources

    def get_collection(self, resource_name, count=100):

        params = {'fast': 'false', 'from': 1,
                  'to': count, 'type': resource_name}

        while True:
            data = self.__search(**params)['response']['metadata']
            if isinstance(data, dict):
                data = [data]
            yield from data
            if len(data) < count:
                break
            params['from'] += count
            params['to'] += count

    @ResponseConverter()
    def __search(self, **params):
        return Method.search(self.uri, **params)


class IndexProfile(AbstractIndexProfile):

    def __init__(self, name, elastic_index, resource):
        super().__init__(name, elastic_index, resource)

    def _format(f):

        @wraps(f)
        def wrapper(self, *args, **kwargs):

            def set_aliases(properties):
                new = {}
                for k, v in properties.items():
                    prop = self.get_property(k)
                    if prop.rejected:
                        continue
                    new[prop.alias or prop.name] = v
                return new

            for doc in f(self, *args, **kwargs):

                properties = {}
                for p in self.iter_properties():
                    res = browse2(doc, p.rule and p.rule.split('/') or [p.name])
                    if not res:
                        continue
                    properties[p.name] = (len(res) == 1) and res[0] or res

                url = urlparse(self.resource.source.uri, allow_fragments=False)

                # TODO: Gestion du basicAuth en amont
                source_uri = '{schema}://{hostname}{port}{path}{params}'.format(
                    schema=url.scheme,
                    hostname=url.hostname,
                    port=(url.port and ':{0}'.format(url.port) or ''),
                    path=url.path,
                    params=(url.params and '?{0}'.format(url.params) or ''))

                metadata_uri = '{schema}://{hostname}{port}{path}'.format(
                    schema=url.scheme,
                    hostname=url.hostname,
                    port=(url.port and ':{0}'.format(url.port) or ''),
                    path='{0}.metadata.get?uuid={1}'.format(
                        url.path.split('.search')[0], doc['info']['uuid']))

                yield {
                    'properties': set_aliases(properties),
                    'origin': {
                        'resource': {
                            'name': self.resource.name},
                        'source': {
                            'name': self.resource.source.name,
                            'type': self.resource.source.protocol,
                            'uri': source_uri},
                        'uri': metadata_uri,
                        'uuid': doc['info']['uuid']},
                    'raw_data': doc}

        return wrapper

    @_format
    def get_collection(self):
        return self.resource.source.get_collection(self.resource.name)

    def generate_elastic_mapping(self):

        analyzer = self.elastic_index.analyzer
        search_analyzer = self.elastic_index.search_analyzer

        mapping = {self.name: {
            'properties': {
                'raw_data': {
                    'dynamic': False,
                    'enabled': False,
                    'include_in_all': False,
                    'type': 'object'},
                'origin': {
                    'properties': {
                        'resource': {
                            'properties': {
                                'name': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'}}},
                        'source': {
                            'properties': {
                                'type': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'name': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'uri': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'}}},
                        'uri': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'},
                        'uuid': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'}}}}}}

        if self.tags:
            mapping[self.name]['properties']['tags'] = {
                'analyzer': analyzer,
                'boost': 1.0,
                # 'doc_value'
                # 'eager_global_ordinals'
                # 'fields'
                # 'ignore_above'
                # 'include_in_all'
                'index': True,
                'index_options': 'docs',
                'norms': True,
                # 'null_value'
                'store': False,
                'search_analyzer': search_analyzer,
                'similarity': 'classic',
                'term_vector': 'yes',
                'type': 'keyword'}

        props = {}
        for p in self.iter_properties():
            if p.rejected:
                continue
            props[p.alias or p.name] = fetch_mapping(p)

        if props:
            mapping[self.name]['properties']['properties'] = {
                'properties': props}

        return clean_my_obj(mapping)
