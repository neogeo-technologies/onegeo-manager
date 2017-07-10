# from .utils import execute_aiohttp_get
from .utils import execute_http_get
from .utils import StaticClass


class CswMethod(metaclass=StaticClass):

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

        # return execute_aiohttp_get(url, **params)
        return execute_http_get(url, **params)

    @classmethod
    def get_capabilities(cls, url, **params):
        return cls.get(cls, 'GetCapabilities', url, **params)

    @classmethod
    def get_records(cls, url, **params):
        return cls.get(cls, 'GetRecords', url, **params)


class GeonetMethod(metaclass=StaticClass):

    @staticmethod
    def get(self, url, **params):
        # return execute_aiohttp_get(url, **params)
        return execute_http_get(url, **params)

    @classmethod
    def search(cls, url, **params):
        return cls.get(cls, url, **params)


class WfsMethod(metaclass=StaticClass):

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

        # return execute_aiohttp_get(url, **params)
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
