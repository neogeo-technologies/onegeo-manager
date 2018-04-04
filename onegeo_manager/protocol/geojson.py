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
import geojson
from onegeo_manager.exception import UnexpectedError
from onegeo_manager.index_profile import AbstractIndexProfile
from onegeo_manager.index_profile import fetch_mapping
from onegeo_manager.index_profile import not_searchable
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import clean_my_obj
from pathlib import Path
import re
import requests


__description__ = 'GeoJSON'


class Resource(AbstractResource):

    GEOMETRY_TYPE = ['Point', 'MultiPoint', 'Polygon', 'MultiPolygon',
                     'LineString', 'MultiLineString', 'GeometryCollection']

    def __init__(self, source):
        super().__init__(source)

        self.geometry = 'GeometryCollection'

    def authorized_geometry_type(self, val):
        return val in self.GEOMETRY_TYPE

    def set_geometry_column(self, geom_type):
        if not self.authorized_geometry_type(geom_type):
            raise UnexpectedError(
                "'{0}' is not an authorized geometry type".format(geom_type))
        self.geometry = geom_type

    def get_collection(self):
        yield from self.source._data['features']


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
                return geojson.load(f)

        if self.uri.startswith('http'):
            r = requests.get(self.uri)
            if r.status_code == 200:
                r.raise_for_status()
            pattern = '^(text|application)\/((\w+)\+?)+\;?(((\s?\w+\=[\w\d\D]+)|(subtype\=geojson));?)*$'
            if re.match(pattern, r.headers['Content-Type']):
                return geojson.loads(r._content.decode('utf-8'))

    def get_resources(self, *args, **kwargs):
        data = self._data
        if data.errors():
            raise Exception(data.errors())
        if data.__class__.__qualname__ == 'FeatureCollection':
            features = data['features']
            feature = len(features) > 0 and features[0]
            properties = feature.get('properties', None)
            geometry = feature.get('geometry', None)
        else:
            raise Exception('No FeatureCollection found')

        resource = Resource(self)
        if properties:
            for item in properties.keys():
                resource.add_column(item)
        if geometry:
            resource.set_geometry_column(geometry.get('type'))
        return [resource]

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
                    'geometry': record.get('geometry'),
                    'lineage': {
                        # 'resource': {
                        #     'name': self.resource.name},
                        'source': {
                            'protocol': self.resource.source.protocol,
                            'uri': self.resource.source.uri}},
                    'properties': alias(record['properties'])}

        return wrapper

    @_format
    def get_collection(self, **opts):
        yield from self.resource.get_collection(**opts)

    def generate_elastic_mapping(self):

        geometry_mapping = {
            'type': 'geo_shape',
            'tree': 'quadtree',
            # 'precision': '',
            # 'tree_levels': '',
            # 'strategy': '',
            'distance_error_pct': 0,
            'orientation': 'counterclockwise',
            'points_only': False}

        props = {}
        for p in self.iter_properties():
            if not p.rejected:
                props[p.alias or p.name] = fetch_mapping(p)

        return clean_my_obj({
            self.name: {
                'properties': {
                    'geometry': geometry_mapping,
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
