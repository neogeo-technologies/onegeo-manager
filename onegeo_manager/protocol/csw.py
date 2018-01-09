from onegeo_manager.index_profile import AbstractIndexProfile
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import execute_http_get
from onegeo_manager.utils import ows_response_converter
from onegeo_manager.utils import StaticClass


class Method(metaclass=StaticClass):

    SERVICE = 'CSW'
    VERSION = ('2.0.2')
    REQUEST = ('GetCapabilities', 'GetRecords')

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
    def get_records(cls, url, **params):
        return cls.get(cls, 'GetRecords', url, **params)


class Resource(AbstractResource):

    def __init__(self, source, name):
        super().__init__(source, name)

    def authorized_column_type(self, val):
        return val in self.COLUMN_TYPE + ['object']


class Source(AbstractSource):

    def __init__(self, url, name, protocol):
        super().__init__(url, name, protocol)

        self.capabilities = self.__get_capabilities()['Capabilities']

    def get_resources(self):
        return [Resource(self, 'dataset'),
                Resource(self, 'nonGeographicDataset'),
                Resource(self, 'series'),
                Resource(self, 'service')]

    def get_collection(self, resource_name, count=100):

        params = {'version': self.capabilities['@version']}

        if params['version'] != '2.0.2':
            raise NotImplemented(
                'Version {0} not implemented.'.format(params['VERSION']))

        params.update({
            'constraint': "type LIKE '{0}'".format(resource_name),
            'constraint_language_version': '1.0.0',
            'constraintlanguage': 'CQL_TEXT',
            'elementsetname': 'full',
            'maxrecords': count,
            'outputschema': 'http://www.isotc211.org/2005/gmd',
            'resulttype': 'results',
            'startposition': 1,
            'typenames': 'csw:Record'})  # gmd:MD_Metadata, csw:Record

        while True:
            data = self.__get_records(**params)['GetRecordsResponse']['SearchResults']['Record']
            yield from data
            if len(data) < count:
                break
            params['startposition'] += count

    @ows_response_converter
    def __get_capabilities(self, **params):
        return Method.get_capabilities(self.uri, **params)

    @ows_response_converter
    def __get_records(self, **params):
        return Method.get_records(self.uri, **params)


class IndexProfile(AbstractIndexProfile):

    def __init__(self, name, elastic_index, resource):

        if not resource.__class__.__qualname__ == 'CswResource':
            raise TypeError("Argument should be an instance of 'CswResource'.")

        super().__init__(name, elastic_index, resource)
