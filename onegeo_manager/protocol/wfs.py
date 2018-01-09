from functools import wraps
from onegeo_manager.index_profile import AbstractIndexProfile
from onegeo_manager.index_profile import fetch_mapping
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import clean_my_obj
from onegeo_manager.utils import execute_http_get
from onegeo_manager.utils import obj_browser
from onegeo_manager.utils import ows_response_converter
from onegeo_manager.utils import StaticClass
from re import search


class Method(metaclass=StaticClass):

    SERVICE = 'WFS'
    VERSION = ('1.1.0', '2.0.0')
    REQUEST = ('GetCapabilities', 'DescribeFeatureType', 'GetFeature')

    def authorized_requests(self, val):
        return val in self.REQUEST

    def authorized_versions(self, val):
        return val in self.VERSION

    @staticmethod
    def get(self, request_name, url, **params):

        params.update({'SERVICE': self.SERVICE})

        if self.authorized_requests(self, request_name):
            params['REQUEST'] = request_name
        else:
            raise ValueError(
                'Request value \'{0}\' not authorized.'.format(request_name))

        if 'VERSION' in params and not self.authorized_versions(
                self, params['VERSION']):
            raise ValueError(
                'Version value \'{0}\' not authorized.'.format(params['VERSION']))

        return execute_http_get(url, **params)

    @classmethod
    def get_capabilities(cls, url, **params):
        return cls.get(cls, 'GetCapabilities', url, **params)

    @classmethod
    def describe_feature_type(cls, url, **params):
        return cls.get(cls, 'DescribeFeatureType', url, **params)

    @classmethod
    def get_feature(cls, url, **params):
        return cls.get(cls, 'GetFeature', url, **params)


class Resource(AbstractResource):

    GEOMETRY_TYPE = ['Point', 'MultiPoint', 'Polygon', 'MultiPolygon',
                     'LineString', 'MultiLineString', 'GeometryCollection']

    def __init__(self, source, name):
        super().__init__(source, name)

        capacity = self.source._retreive_ft_meta(name)

        self.title = obj_browser(capacity, 'Title')
        self.abstract = obj_browser(capacity, 'Abstract')
        self.metadata_url = obj_browser(capacity, 'MetadataURL', '@href')

        self.geometry = 'GeometryCollection'

    def authorized_geometry_type(self, val):
        return val in self.GEOMETRY_TYPE

    @staticmethod
    def column_type_mapper(val):
        switcher = {'string': 'text'}
        return switcher.get(val, val)

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
            raise Exception(
                "'{0}' is not an authorized geometry type".format(geom_type))
        self.geometry = t

    def add_column(self, name, column_type=None,
                   occurs=(0, 1), count=None, rule=None):
        if self.geometry_type_mapper(column_type):
            self.set_geometry_column(column_type)
        else:
            column_type and self.column_type_mapper(column_type)
            super().add_column(
                name, column_type=None, occurs=(0, 1), count=None, rule=rule)


class Source(AbstractSource):

    def __init__(self, url, name, protocol):
        super().__init__(url, name, protocol)

        self.capabilities = self.__get_capabilities()['WFS_Capabilities']

        self.title = obj_browser(
            self.capabilities, 'ServiceIdentification', 'Title')

        self.abstract = obj_browser(
            self.capabilities, 'ServiceIdentification', 'Abstract')

        self.metadata_url = ''

    def _retreive_ft_meta(self, ft_name):
        for f in iter(self.capabilities['FeatureTypeList']['FeatureType']):
            if f['Name'].split(':')[-1] == ft_name:
                return f
        raise ValueError('{0} not found.'.format(ft_name))

    def get_resources(self):

        desc = self.__describe_feature_type(
            version=self.capabilities['@version'])

        resources = []
        for elt in iter([(m['@name'], m['@type'].split(':')[-1])
                         for m in desc['schema']['element']]):

            resource = Resource(self, elt[0])

            ct = None
            for complex_type in iter(desc['schema']['complexType']):
                if complex_type['@name'] == elt[1]:
                    ct = complex_type
                    break

            for e in ct['complexContent']['extension']['sequence']['element']:
                n = '@name' in e and str(e['@name']) or None
                t = '@type' in e and str(e['@type']).split(':')[-1] or None
                o = ('@minOccurs' in e and int(e['@minOccurs']) or 0,
                     '@maxOccurs' in e and int(e['@maxOccurs']) or 1)
                resource.add_column(n, column_type=t, occurs=o)

            resources.append(resource)
        return resources

    def get_collection(self, resource_name, count=100):
        """Retourne la collection de documents.

        :param resource_name: Le nom du type d'objets à retourner.
        :param count: Le pas de pagination du GetFeature (opt).
        :return: Un générateur contenant des GeoJSON.
        """
        capacity = self._retreive_ft_meta(resource_name)

        params = {'version': self.capabilities['@version']}

        if params['version'] != '2.0.0':
            raise NotImplemented(
                'Version {0} not implemented.'.format(params['VERSION']))

        crs_str = ','.join(capacity['OtherCRS'] + [capacity['DefaultCRS']])
        format_str = ','.join(capacity['OutputFormats']['Format'])

        testing = {
            'srsname': {
                'pattern': '((^|((\w*\:+)+))4326)',
                'string': crs_str},
            'outputformat': {
                'pattern': '((text|application)\/json\;?\s?subtype\=geojson)',
                'string': format_str}}

        for k, v in testing.items():
            s = search(v['pattern'], v['string'])
            if not s:
                raise ValueError('TODO')  # TODO
            params[k] = s.group(0)

        params.update({'typenames': resource_name,
                       'startindex': 0,
                       'count': count})

        while True:
            # Boucle sur le GetFeature tant que tous
            # les objets ne sont pas recupérés.
            data = self.__get_feature(**params)['features']
            yield from data
            if len(data) < count:
                break
            params['startindex'] += count

    @ows_response_converter
    def __get_capabilities(self, **params):
        return Method.get_capabilities(self.uri, **params)

    @ows_response_converter
    def __describe_feature_type(self, **params):
        return Method.describe_feature_type(self.uri, **params)

    @ows_response_converter
    def __get_feature(self, **params):
        return Method.get_feature(self.uri, **params)


class IndexProfile(AbstractIndexProfile):

    def __init__(self, name, elastic_index, resource):
        super().__init__(name, elastic_index, resource)

    def _format(f):

        @wraps(f)
        def wrapper(self, *args, **kwargs):

            def alias(properties):
                new = {}
                for k, v in properties.items():
                    prop = self.get_property(k)
                    if prop.rejected:
                        continue
                    new[prop.alias or prop.name] = v
                return new

            for doc in f(self, *args, **kwargs):
                yield {
                    'origin': {
                        'resource': {
                            'name': self.resource.name,
                            'title': self.resource.title,
                            'abstract': self.resource.abstract,
                            'metadata_url': self.resource.metadata_url},
                        'source': {
                            'name': self.resource.source.name,
                            'title': self.resource.source.title,
                            'abstract': self.resource.source.abstract,
                            'metadata_url': self.resource.source.metadata_url,
                            'uri': self.resource.source.uri,
                            'type': self.resource.source.protocol}},
                    'properties': alias(doc['properties']),
                    'raw_data': doc}

        return wrapper

    @_format
    def get_collection(self, **opts):
        return self.resource.source.get_collection(self.resource.name, **opts)

    def generate_elastic_mapping(self):

        analyzer = self.elastic_index.analyzer
        search_analyzer = self.elastic_index.search_analyzer

        if self.resource.geometry in ('Point', 'MultiPoint'):
            geometry_mapping = {'type': 'geo_point',
                                'ignore_malformed': True}
        else:
            geometry_mapping = {'type': 'geo_shape',
                                'tree': 'quadtree',
                                # 'precision': '',
                                # 'tree_levels': '',
                                # 'strategy': '',
                                'distance_error_pct': 0,
                                'orientation': 'counterclockwise',
                                'points_only': False}

        mapping = {self.name: {
            'properties': {
                'raw_data': {
                    'properties': {
                        'geometry': geometry_mapping,
                        'properties': {
                            'dynamic': False,
                            'enabled': False,
                            'include_in_all': False,
                            'type': 'object'},
                        'type': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'}}},
                'origin': {
                    'properties': {
                        'resource': {
                            'properties': {
                                'abstract': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'metadata_url': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'name': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'title': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'}}},
                        'source': {
                            'properties': {
                                'abstract': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'metadata_url': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'name': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'title': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'type': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'uri': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'}}}}}}}}

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
