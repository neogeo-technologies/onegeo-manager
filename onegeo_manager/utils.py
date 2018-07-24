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


import hashlib
from io import StringIO
import itertools
import json
# import math
# import numpy as np
import operator
import re


# Cool stuffs about obj..

# def get_weighted_std_dev(val, wgt):
#     """Return the standard deviation of a weighted serie."""
#     return math.sqrt(
#         np.average((val - np.average(val, weights=wgt)) ** 2, weights=wgt))


def accumulate(obj):  # TODO: recursive
    for occur, subiter in itertools.groupby(obj, key=operator.itemgetter(0)):
        mapped = map(operator.itemgetter(1), subiter)
        reiter = itertools.groupby(
            mapped or subiter, key=operator.itemgetter(0))
        for parent, subiter in reiter:
            children = tuple(
                e for n in tuple(m[1:] for m in subiter) for e in n) or None
            yield occur, parent, children


def iterate(obj, parent=None, path=list()):
    """Iterate any obj and return value with his path."""
    parent and path.append(parent)
    if isinstance(obj, dict):
        for key, value in obj.items():
            yield from iterate(value, parent=key, path=path)
            path.pop()
    elif any(isinstance(obj, t) for t in (list, tuple)):
        for item in obj:
            yield from iterate(item, parent=not parent and 'root', path=path)
            not parent and path and path.pop()
    else:
        yield tuple(path), obj
        path = list()


def clean_my_dict(d):
    if not isinstance(d, dict):
        raise TypeError('Argument should be an instance of dict')
    return dict((k, clean_my_dict(v)) for k, v in d.items() if v is not None)


def clean_my_obj(obj, fading=False):
    if isinstance(obj, (list, tuple, set)):
        if not fading:
            return type(obj)(
                clean_my_obj(x, fading=fading) for x in obj if x is not None)
        if len(obj) > 1:
            return type(obj)(
                clean_my_obj(x, fading=fading) for x in obj if x is not None)
        if len(obj) == 1:
            obj = obj[0]
        if len(obj) < 1:
            obj = None
    if isinstance(obj, dict):
        return type(obj)(
            (clean_my_obj(k, fading=fading), clean_my_obj(v, fading=fading))
            for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj


def from_camel_was_born_snake(txt):
    return re.sub('([a-z0-9])([A-Z])', '\g<1>_\g<2>',
                  re.sub('(.)([A-Z][a-z]+)', '\g<1>_\g<2>', txt)).lower()


def browse(obj, *tag):
    if not isinstance(obj, dict):
        return None
    if len(tag) == 0:
        return None

    regex = tag[0]
    for cursor in obj.keys():
        if re.match(regex, cursor):
            if len(tag) == 1:
                return obj[cursor]
            if isinstance(obj[cursor], dict):
                return browse(obj[cursor], *tag[1:])
            if isinstance(obj[cursor], list):
                cache = []
                for i in range(len(obj[cursor])):
                    val = browse(obj[cursor][i], *tag[1:])
                    if val is not None:
                        cache.append(val)
                return cache


# Class types


class StaticClass(type):

    def __call__(cls):
        raise TypeError(
            '\'{0}\' static class is not callable.'.format(cls.__qualname__))


class Singleton(type):

    __instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.__instances:
            cls.__instances[cls] = super().__call__(*args, **kwargs)
        # else:
        #     cls._instances[cls].__init__(*args, **kwargs)
        return cls.__instances[cls]


def digest_object(obj, encoding='utf-8'):
    """Convert any object to md5 hex digest through a ordonned and minified JSON data."""
    io = StringIO()
    json.dump(
        obj, io, skipkeys=False, ensure_ascii=False,
        check_circular=True, allow_nan=True, cls=None, indent=None,
        separators=(',', ':'), default=None, sort_keys=True)

    return hashlib.md5(io.getvalue().encode(encoding)).hexdigest()
