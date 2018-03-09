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


import ast
from collections import OrderedDict
import operator
import re


# Cool stuffs about obj..


def iterate(obj, parent=None, path=list()):
    """Iterates any obj and returns value with his path."""
    parent and path.append(parent)
    if isinstance(obj, dict):
        for key, value in obj.items():
            yield from iterate(value, parent=key, path=path)
            path.pop()
    elif any(isinstance(obj, t) for t in (list, tuple)):
        for item in obj:
            yield from iterate(item, path=path)
    else:
        yield obj, path
        path = list()


def deconstruct(obj):
    """Deconstruct any obj then returns statistics of this one."""
    inputted = OrderedDict()
    for value, path in iterate(obj):
        path = path.__str__()
        if path in inputted.keys():
            inputted[path] += 1
        else:
            inputted.update({path: 1})

    outputted = OrderedDict()
    for k, v in inputted.items():
        k = tuple(ast.literal_eval(k))
        if v in outputted:
            outputted[v].append(k)
        else:
            outputted[v] = [k]

    return tuple(sorted((k, tuple(v)) for k, v in outputted.items()))


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
    if tag[0] in obj:
        if len(tag) == 1:
            return obj[tag[0]]
        if isinstance(obj[tag[0]], dict):
            return browse(obj[tag[0]], *tag[1:])
        if isinstance(obj[tag[0]], list):
            cache = []
            for i in range(len(obj[tag[0]])):
                val = browse(obj[tag[0]][i], *tag[1:])
                if val is not None:
                    cache.append(val)
            return cache


def browse2(obj, lst):
    if type(obj) == list:
        arr = []
        for e in obj:
            val = browse2(e, lst)
            if val:
                arr.append(val)
        return arr

    if len(lst) == 1:
        try:
            e, regex = tuple(lst[0].split('~'))
        except Exception:
            e = lst[0]
        else:
            if not re.match(regex, obj[e]):
                return
        if e not in obj:
            return None
        target = obj[e]
        if type(target) == list:
            n = []
            for m in target:
                if type(m) == str:
                    n.append(m or None)
                if type(m) == list:
                    operator.add(n, m)
                if type(m) == dict:
                    n.append(m['$'] or None)
            target = n
        return target

    try:
        k, v = tuple(lst[0].split('[')[-1][:-1].split('='))
    except Exception:
        e, k, v = lst[0], None, None
    else:
        e = lst[0].split('[')[0]

    if type(obj) == dict and e not in obj:
        return

    if type(obj) == dict and e in obj:
        if k and (k in obj and obj[k] != v):
            return
        return browse2(obj[e], lst[1:])


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
