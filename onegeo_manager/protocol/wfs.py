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


from functools import wraps
from onegeo_manager.exception import NotYetImplemented
from onegeo_manager.exception import OGCExceptionReport
from onegeo_manager.exception import UnexpectedError
from onegeo_manager.index_profile import AbstractIndexProfile
from onegeo_manager.index_profile import fetch_mapping
from onegeo_manager.index_profile import not_searchable
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import browse
from onegeo_manager.utils import clean_my_obj
from onegeo_manager.utils import digest_object
from onegeo_manager.utils import StaticClass
import operator
import re
import requests
import xmltodict


__description__ = 'OGC:WFS'


def response_converter(fun):

    @wraps(fun)
    def wrapper(*args, **kwargs):
        response = fun(*args, **kwargs)
        if not isinstance(response, str):
            return response
        data = xmltodict.parse(response, process_namespaces=False)

        if 'ExceptionReport' in data:
            report = data['ExceptionReport']
            if report['@version'] == '2.0.0':
                code = report['Exception']['@exceptionCode']
            else:
                code = report['Exception']['@exceptionCode']

            raise OGCExceptionReport(
                code, report['Exception']['ExceptionText'])

        return data
    return wrapper


class Method(metaclass=StaticClass):

    SERVICE = 'WFS'
    VERSION = ('1.1.0', '2.0.0')
    REQUEST = ('GetCapabilities', 'DescribeFeatureType', 'GetFeature')

    def authorized_requests(self, val):
        return val in self.REQUEST

    def authorized_versions(self, val):
        return val in self.VERSION

    @staticmethod
    def get(self, request_name, url, params=None, auth=None):
        params.update({'service': self.SERVICE})

        if self.authorized_requests(self, request_name):
            params['request'] = request_name
        else:
            raise UnexpectedError(
                'Request value \'{0}\' not authorized.'.format(request_name))

        if 'version' in params and not self.authorized_versions(
                self, params['version']):
            raise UnexpectedError(
                'Version value \'{0}\' not authorized.'.format(params['version']))

        for i in range(0, 10):
            try:
                r = requests.get(url, params=params, auth=auth)
            except Exception as e:
                error = e
                continue
            else:
                break
        else:
            raise error

        if r.status_code == 200:
            r.raise_for_status()

        pattern = '^(text|application)\/((\w+)\+?)+\;?((\s?\w+\=[\w\d\D]+);?)*$'
        s = re.search(pattern, r.headers['Content-Type'])
        if s and s.group(2) == 'json':
            return r.json()
        elif s and s.group(2) == 'xml':
            return r.text
        else:
            raise Exception('Error service response.', r.text)

    @classmethod
    def get_capabilities(cls, url, params, auth=None):
        return cls.get(cls, 'GetCapabilities', url, params=params, auth=auth)

    @classmethod
    def describe_feature_type(cls, url, params, auth=None):
        return \
            cls.get(cls, 'DescribeFeatureType', url, params=params, auth=auth)

    @classmethod
    def get_feature(cls, url, params, auth=None):
        return cls.get(cls, 'GetFeature', url, params=params, auth=auth)


class Resource(AbstractResource):

    GEOMETRY_TYPE = ['Point', 'MultiPoint', 'Polygon', 'MultiPolygon',
                     'LineString', 'MultiLineString', 'GeometryCollection']

    def __init__(self, source, name=None):
        super().__init__(source, name=name)

        capacity = self.source._retreive_ft_meta(name)

        self.title = capacity.get('Title')
        self.abstract = capacity.get('Abstract')
        self.metadata_url = browse(capacity, 'MetadataURL', '@(xlink:)?href')

        self.geometry = 'GeometryCollection'

    def authorized_geometry_type(self, val):
        return val in self.GEOMETRY_TYPE

    @staticmethod
    def column_type_mapper(val):
        return {
            'decimal': 'float',
            'int': 'integer',
            'dateTime': 'date',
            'TimeInstantType': 'date',  # TODO -> Date + format
            'string': 'text'
            }.get(val, val)

    @staticmethod
    def geometry_type_mapper(val):
        return {'PointPropertyType': 'Point',
                'MultiPointPropertyType': 'MultiPoint',
                'SurfacePropertyType': 'Polygon',
                'MultiSurfacePropertyType': 'MultiPolygon',
                'CurvePropertyType': 'LineString',
                'MultiCurvePropertyType': 'MultiLineString',
                'GeometryPropertyType': 'GeometryCollection'}.get(val, None)

    def set_geometry_column(self, geom_type):
        t = self.geometry_type_mapper(geom_type)
        if not self.authorized_geometry_type(t):
            raise UnexpectedError(
                "'{0}' is not an authorized geometry type".format(geom_type))
        self.geometry = t

    def add_column(self, name, column_type=None,
                   occurs=(0, 1), count=None, rule=None):
        if self.geometry_type_mapper(column_type):
            self.set_geometry_column(column_type)
        else:
            column_type = \
                column_type and self.column_type_mapper(column_type) or None
            super().add_column(name, column_type=column_type,
                               occurs=occurs, count=count, rule=rule)

    def get_collection(self, step=100):
        raise NotImplementedError('TODO')


class Source(AbstractSource):

    def __init__(self, url, username=None, password=None):
        super().__init__(url)

        self.username = username
        self.password = password

        self.capabilities = browse(self.__get_capabilities(), '(wfs:)?WFS_Capabilities')

        _service_ident = browse(self.capabilities, '(ows:)?ServiceIdentification')
        self.title = browse(_service_ident, '(ows:)?Title')
        self.abstract = browse(_service_ident, '(ows:)?Abstract')

        self.metadata_url = ''

    def _retreive_ft_meta(self, ft_name):
        for f in iter(self.capabilities['FeatureTypeList']['FeatureType']):
            if f['Name'].split(':')[-1] == ft_name:
                return f
        raise ValueError('{0} not found.'.format(ft_name))

    def get_resources(self, *args, **kwargs):
        names = kwargs.pop('names', [])

        desc = self.__describe_feature_type(
            version=self.capabilities['@version'],
            typename=','.join(names) or None)

        sch_elts = browse(desc, '(xsd:)?schema', '(xsd:)?element')
        sch_cplx_types = browse(desc, '(xsd:)?schema', '(xsd:)?complexType')

        resources = []
        for sch_elt in iter([
                (m['@name'], m['@type'].split(':')[-1])
                for m in isinstance(sch_elts, list)
                and sch_elts or [sch_elts]]):

            try:
                resource = Resource(self, sch_elt[0])
            except ValueError:
                # TODO log this
                continue

            ct = None
            for cplx_type in iter(
                    isinstance(sch_cplx_types, list)
                    and sch_cplx_types or [sch_cplx_types]):

                if cplx_type['@name'] == sch_elt[1]:
                    ct = cplx_type
                    break

            seq_elt = browse(
                ct, '(xsd:)?complexContent',
                '(xsd:)?extension', '(xsd:)?sequence', '(xsd:)?element')
            for e in isinstance(seq_elt, dict) and [seq_elt] or seq_elt:
                n = '@name' in e and str(e['@name']) or None
                t = '@type' in e and str(e['@type']).split(':')[-1] or None
                o = ('@minOccurs' in e and int(e['@minOccurs']) or 0,
                     '@maxOccurs' in e and int(e['@maxOccurs']) or 1)
                resource.add_column(n, column_type=t, occurs=o)

            resources.append(resource)
        return resources

    def get_collection(self, resource_name, step=500):

        capacity = self._retreive_ft_meta(resource_name)

        params = {'version': self.capabilities['@version']}

        if params['version'] != '2.0.0':
            raise UnexpectedError(
                'Version {0} not implemented.'.format(params['version']))

        try:
            outputformats = ', '.join(capacity['OutputFormats']['Format'])
        except KeyError:
            for op in browse(self.capabilities, '(ows:)?OperationsMetadata', '(ows:)?Operation'):
                if op['@name'] == 'GetFeature':
                    for p in browse(op, '(ows:)?Parameter'):
                        if p['@name'] == 'outputFormat':
                            outputformats = ', '.join(
                                browse(p, '(ows:)?AllowedValues', '(ows:)?Value'))
                            break
        regex = '(((text|application)\/)?json\;?\s?((\w+=[^,;]+|(subtype\=geojson))\;?\s?)*)'
        s = re.search(regex, outputformats)
        if not s:
            raise NotYetImplemented('GeoJSON is mandatory.')
        params['outputformat'] = s.group(0)

        ## TODO
        # other_crs = capacity.get('OtherCRS', [])
        # crs_str = ', '.join(
        #     operator.add(
        #         isinstance(other_crs, str) and [other_crs] or other_crs,
        #         [capacity['DefaultCRS']]))
        #
        # s = re.search('((^|((\w*\:+)+))4326)', crs_str)
        # if not s:
        #     raise UnexpectedError('')
        # params['srsname'] = s.group(0)
        params['srsname'] = 'urn:ogc:def:crs:EPSG::4326'
        ##

        params.update(
            {'startindex': 0, 'count': step, 'typenames': resource_name})

        while True:
            data = self.__get_feature(**params)['features']
            yield from data
            if len(data) < step:
                break
            params['startindex'] += step

    @response_converter
    def __get_capabilities(self, **params):
        auth = self.username and self.password \
            and (self.username, self.password) or None
        return Method.get_capabilities(self.uri, params, auth=auth)

    @response_converter
    def __describe_feature_type(self, **params):
        auth = self.username and self.password \
            and (self.username, self.password) or None
        return Method.describe_feature_type(self.uri, params, auth=auth)

    @response_converter
    def __get_feature(self, **params):
        auth = self.username and self.password \
            and (self.username, self.password) or None
        return Method.get_feature(self.uri, params, auth=auth)


class IndexProfile(AbstractIndexProfile):

    def __init__(self, name, resource):
        super().__init__(name, resource)

    def _format(fun):

        @wraps(fun)
        def wrapper(self, *args, **kwargs):
            for record in fun(self, *args, **kwargs):

                properties, _backuped = {}, {}
                for k, v in record['properties'].items():
                    prop = self.get_property(k)
                    if prop:
                        if prop.rejected:
                            _backuped[prop.name] = v
                        else:
                            properties[prop.alias or prop.name] = v

                yield {
                    '_backup': _backuped,
                    '_md5': digest_object(record),
                    'geometry': record.get('geometry'),
                    'lineage': {
                        'resource': {
                            'name': self.resource.name},
                        'source': {
                            'protocol': self.resource.source.protocol,
                            'uri': self.resource.source.uri}},
                    'properties': properties}

        return wrapper

    @_format
    def get_collection(self, **opts):
        return self.resource.source.get_collection(self.resource.name, **opts)

    def generate_elastic_mapping(self):

        # if self.resource.geometry in ('Point', 'MultiPoint'):
        #     geometry_mapping = {'type': 'geo_point', 'ignore_malformed': True}
        # else:
        geometry_mapping = {'type': 'geo_shape',
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
                            'resource': {
                                'properties': {
                                    'name': not_searchable('keyword')}},
                            'source': {
                                'properties': {
                                    'protocol': not_searchable('keyword'),
                                    'uri': not_searchable('keyword')}}}},
                    'properties': {'properties': props}}}})
