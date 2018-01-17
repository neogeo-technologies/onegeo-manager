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
from onegeo_manager.index_profile import not_searchable
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import clean_my_obj
import operator
from owslib import csw


class Resource(AbstractResource):

    def __init__(self, source, name):
        super().__init__(source, name)

    def authorized_column_type(self, val):
        return val in operator.add(self.COLUMN_TYPE, ['object', 'geo_shape'])


class Source(AbstractSource):

    OUTPUSCHEMA = {
        'http://www.opengis.net/cat/csw/2.0.2': [
            'nonGeographicDataset', 'dataset', 'series', 'service'],
        'http://www.isotc211.org/2005/gmd': []}

    def __init__(self, url, name):
        super().__init__(url, name)
        self._csw = csw.CatalogueServiceWeb(url)
        self.capabilities = self._csw.response

    def get_resources(self, names=[]):
        auth_names = ['dataset', 'nonGeographicDataset', 'series', 'service']
        if names and not any(map(lambda v: v in names, auth_names)):
            raise ValueError('Some given names are not found in this context.')

        resources = []
        for val in names or auth_names:
            resource = Resource(self, val)
            resource.add_columns((
                {'name': 'abstract', 'column_type': 'text'},
                # {'name': 'accessrights', 'column_type': 'text'},  # dct
                # {'name': 'alternative', 'column_type': 'text'},  # dct
                # {'name': 'created', 'column_type': 'text'},  # dct
                # {'name': 'bbox', 'column_type': 'geo_shape'},  # ows
                # {'name': 'bbox_wgs84', 'column_type': 'geo_shape'}, # ows
                {'name': 'date', 'column_type': 'date'},
                {'name': 'identifier', 'column_type': 'text'},
                # {'name': 'ispartof', 'column_type': 'text'},  # dct
                # {'name': 'issued', 'column_type': 'text'},  # dct
                # {'name': 'license', 'column_type': 'text'},  # dct
                # {'name': 'modified', 'column_type': 'text'},  # dct
                # {'name': 'references', 'column_type': 'object'},  # dct
                {'name': 'relation', 'column_type': 'text'},
                {'name': 'rights', 'column_type': 'text'},
                # {'name': 'rightsholder', 'column_type': 'text'},  # dct
                # {'name': 'spatial', 'column_type': 'text'},  # dct
                {'name': 'source', 'column_type': 'text'},
                {'name': 'subjects', 'column_type': 'text'},
                # {'name': 'temporal', 'column_type': 'text'},  # dct
                {'name': 'title', 'column_type': 'text'},
                {'name': 'type', 'column_type': 'text'},
                {'name': 'uris', 'column_type': 'object'},
                {'name': 'xml', 'column_type': 'text'}))
            resources.append(resource)
        return resources

    def get_collection(self, resource_name, step=10):

        params = {
            'cql': "type LIKE '{0}'".format(resource_name),
            'typenames': 'csw:Record',
            'esn': 'full',
            'format': 'application/xml',
            'outputschema': tuple(
                tuple(k for v in l if v == resource_name)[0]
                for k, l in self.OUTPUSCHEMA.items() if resource_name in l)[0],
            'resulttype': 'results',
            'startposition': 0,
            'maxrecords': step}

        while True:
            self._csw.getrecords2(**params)
            records = list(self._csw.records.values())
            for rec in records:
                data = {}
                resource = self.get_resources(names=[rec.type])[0]
                for col in resource.iter_columns():
                    try:
                        attr = getattr(rec, col['name'])
                    except AttributeError:
                        data[col['name']] = None
                        continue

                    if col['name'] == 'bbox_wgs84' \
                            and col['type'] == 'geo_shape' and attr:
                        attr = {
                            'type': 'Feature',
                            'crs': {
                                'type': 'name',
                                'properties': {'name': str(attr.crs)}},
                            'geometry': {
                                'type': 'Polygon',
                                'coordinates': [[
                                    [attr.minx, attr.miny],
                                    [attr.maxx, attr.miny],
                                    [attr.maxx, attr.maxy],
                                    [attr.minx, attr.maxy]]]}}

                    data[col['name']] = \
                        isinstance(attr, bytes) and attr.decode() or attr
                yield data

            if len(records) < step:
                break
            params['startposition'] += step


class IndexProfile(AbstractIndexProfile):

    def __init__(self, name, elastic_index, resource):
        super().__init__(name, elastic_index, resource)

    def authorized_column_type(self, val):
        return val in operator.add(self.COLUMN_TYPE, ['object', 'geo_shape'])

    def _format(fun):

        @wraps(fun)
        def wrapper(self, *args, **kwargs):

            def set_aliases(properties):
                new = {}
                for k, v in properties.items():
                    prop = self.get_property(k)
                    if prop.rejected:
                        continue
                    new[prop.alias or prop.name] = v
                return new

            for record in fun(self, *args, **kwargs):
                xml = 'xml' in record and record.pop('xml') or None
                uri = 'uri' in record and record.pop('uris') or None
                yield {
                    'lineage': {
                        'resource': {
                            'name': self.resource.name},
                        'source': {
                            'name': self.resource.source.name,
                            'protocol': self.resource.source.protocol,
                            'uri': self.resource.source.protocol}},
                    'properties': set_aliases(record),
                    'uri': uri,
                    'xml': xml}

        return wrapper

    @_format
    def get_collection(self, **opts):
        return self.resource.source.get_collection(self.resource.name, **opts)

    def generate_elastic_mapping(self):

        props = {}
        for p in self.iter_properties(ignore=['xml', 'uris']):
            if not p.rejected:
                props[p.alias or p.name] = fetch_mapping(p)

        return clean_my_obj({
            self.name: {
                'properties': {
                    'lineage': {
                        'properties': {
                            'resource': {
                                'properties': {
                                    'name': not_searchable('keyword')}},
                            'source': {
                                'properties': {
                                    'name': not_searchable('keyword'),
                                    'type': not_searchable('keyword'),
                                    'uri': not_searchable('keyword')}}}},
                    'properties': {
                        'properties': props},
                    'tags': {
                        'analyzer': self.elastic_index.analyzer,
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
                        'search_analyzer': self.elastic_index.search_analyzer,
                        'similarity': 'classic',
                        'term_vector': 'yes',
                        'type': 'keyword'},
                    'uri': {
                        'type': 'nested',
                        'properties': {
                            'protocol': not_searchable('keyword'),
                            'name': not_searchable('text'),
                            'description': not_searchable('text'),
                            'url': not_searchable('keyword')}},
                    'xml': not_searchable('text')}}})
