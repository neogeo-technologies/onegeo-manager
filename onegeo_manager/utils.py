from re import search, sub


def ows_response_converter(f):

    from functools import wraps
    from neogeo_xml_utils import XMLToObj

    from .exception import OGCExceptionReport

    @wraps(f)
    def wrapper(*args, **kwargs):

        response = f(*args, **kwargs)

        if not isinstance(response, str):
            return response

        data = XMLToObj(response, with_ns=False).data

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


# aiohttp stuffs

# async def aiohttp_fetcher(client, url, **params):

#     from async_timeout import timeout

#     with timeout(10):
#         async with client.get(url, **params) as r:
#             if not r.status == 200:
#                 r.raise_for_status()

#             pattern = '^(text|application)\/((\w+)\+?)+\;?((\s?\w+\=[\w\d\D]+);?)+$'
#             s = search(pattern, r.content_type)
#             if s and s.group(2) == 'json':
#                 return await r.json()
#             elif s and s.group(2) == 'xml':
#                 return await r.text()
#             else:
#                 # TODO
#                 raise Exception('Error service response.')


# async def aiohttp_client(loop, url, **params):

#     from aiohttp import ClientSession

#     async with ClientSession(loop=loop) as client:
#         return await aiohttp_fetcher(client, url, **params)


# def execute_aiohttp_get(url, **params):
#     from asyncio import get_event_loop as loop
#     return loop().run_until_complete(aiohttp_client(loop(), url, params=params))


# Requests stuffs

def execute_http_get(url, **params):

    from requests import get

    r = get(url, params=params)

    if not r.status_code == 200:
        r.raise_for_status()

    pattern = '^(text|application)\/((\w+)\+?)+\;?((\s?\w+\=[\w\d\D]+);?)+$'
    s = search(pattern, r.headers['Content-Type'])
    if s and s.group(2) == 'json':
        return r.json()
    elif s and s.group(2) == 'xml':
        return r.text
    else:
        # TODO
        raise Exception('Error service response.')


# Cool stuffs

def clean_my_dict(d):
    if not isinstance(d, dict):
        raise TypeError('Argument should be an instance of dict')
    return dict((k, clean_my_dict(v)) for k, v in d.items() if v is not None)


def clean_my_obj(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(clean_my_obj(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)(
                (clean_my_obj(k), clean_my_obj(v))
                    for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj


def from_camel_was_born_snake(txt):
    s1 = sub('(.)([A-Z][a-z]+)', '\g<1>_\g<2>', txt)
    return sub('([a-z0-9])([A-Z])', '\g<1>_\g<2>', s1).lower()


# Class types

class StaticClass(type):

    def __call__(cls):
        raise TypeError('\'{0}\' static class is not callable.'.format(
                                                            cls.__qualname__))


class Singleton(type):

    __instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.__instances:
            cls.__instances[cls] = super().__call__(*args, **kwargs)
        # else:
        #     cls._instances[cls].__init__(*args, **kwargs)
        return cls.__instances[cls]
