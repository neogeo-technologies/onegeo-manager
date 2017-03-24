from asyncio import get_event_loop
from aiohttp import ClientSession
from async_timeout import timeout
from functools import wraps
from neogeo_xml_utils import XMLtoObj
from re import search
from xml.etree.ElementTree import XMLParser

from .exception import OGCExceptionReport
from .utils import StaticClass


def convert_response(f):

    @wraps(f)
    def wrapper(*args, **kwargs):

        response = f(*args, **kwargs)

        if not isinstance(response, str):
            return response

        target = XMLtoObj()
        parser = XMLParser(target=target)
        parser.feed(response)
        res = parser.close()

        if 'ExceptionReport' in res:
            report = res['ExceptionReport']
            if report['@version'] == '2.0.0':
                code = report['Exception']['@exceptionCode']
            else:
                code = report['Exception']['@exceptionCode']

            raise OGCExceptionReport(
                        code, report['Exception']['ExceptionText'])

        return res
    return wrapper


class WfsMethod(metaclass=StaticClass):

    SERVICE = 'WFS'
    VERSION = ('1.1.0', '2.0.0')
    REQUEST = ('GetCapabilities', 'DescribeFeatureType', 'GetFeature')

    def authorized_requests(self, val):
        return val in self.REQUEST

    def authorized_versions(self, val):
        return val in self.VERSION

    @staticmethod
    @convert_response
    def _exe_aiohttp_get(self, request_name, url, **params):

        params.update({'SERVICE': self.SERVICE})

        if self.authorized_requests(self, request_name):
            params['REQUEST'] = request_name
        else:
            raise ValueError('Request value \'{0}\' not authorized.'.format(
                                                                request_name))

        if 'VERSION' in params and not self.authorized_versions(
                                                    self, params['VERSION']):
            raise ValueError('Version value \'{0}\' not authorized.'.format(
                                                            params['VERSION']))

        async def fetch(_client, _url, **_params):
            with timeout(10):
                async with _client.get(_url, **_params) as r:
                    if not r.status == 200:
                        r.raise_for_status()

                    pattern = '^(text|application)\/((\w+)\+?)+\;?(\s?charset\=[\w\d\D]+)?$'
                    s = search(pattern, r.content_type)
                    if s and s.group(2) == 'json':
                        return await r.json()
                    elif s and s.group(2) == 'xml':
                        return await r.text()
                    else:  # TODO
                        raise Exception('Error service response.')

        async def main(_loop, _url, **_params):
            async with ClientSession(loop=_loop) as client:
                return await fetch(client, _url, **_params)

        loop = get_event_loop()
        return loop.run_until_complete(main(loop, url, params=params))

    @classmethod
    def get_capabilities(cls, url, **params):
        return cls._exe_aiohttp_get(cls, 'GetCapabilities', url, **params)

    @classmethod
    def describe_feature_type(cls, url, **params):
        return cls._exe_aiohttp_get(cls, 'DescribeFeatureType', url, **params)

    @classmethod
    def get_feature(cls, url, **params):
        return cls._exe_aiohttp_get(cls, 'GetFeature', url, **params)
