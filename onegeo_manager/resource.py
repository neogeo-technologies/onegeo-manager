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


from abc import ABCMeta
from importlib import import_module
from onegeo_manager.exception import ProtocolNotFoundError
import re


__all__ = ['Resource']


class AbstractResource(metaclass=ABCMeta):

    COLUMN_TYPE = ['binary', 'boolean', 'byte', 'date', 'date_range',
                   'double', 'double_range', 'float', 'float_range',
                   'half_float', 'integer', 'integer_range', 'ip', 'keyword',
                   'long', 'long_range', 'scaled_float', 'short', 'text']

    def __init__(self, source, name=None):

        if not source.__class__.__qualname__ == 'Source':
            raise TypeError("Argument should be an instance of 'Source'.")

        self._source = source
        self._name = name
        self._columns = []

    def authorized_column_type(self, val):
        return val in self.COLUMN_TYPE

    @staticmethod
    def authorized_occurs(val):
        if not type(val) is tuple:
            return False
        if val.__len__() > 2:
            return False
        if not type(val[0]) is int or type(val[1]) is int:
            return False
        return True

    @property
    def source(self):
        return self._source

    @property
    def name(self):
        return self._name

    @property
    def columns(self):
        return self._columns

    def iter_columns(self):
        return iter(self._columns)

    def add_column(self, name, column_type=None,
                   occurs=(0, 1), count=None, rule=None):

        if self.is_existing_column(name):
            raise Exception(
                "Column '{0}' already exists.".format(name))

        if column_type and not self.authorized_column_type(column_type):
            raise TypeError(
                "Column type '{0}' not authorized.".format(column_type), name)

        if self.authorized_occurs(occurs):
            raise ValueError("'{0}' is malformed".format(occurs))

        self._columns.append({'name': name, 'occurs': occurs, 'count': count,
                              'type': column_type, 'rule': rule})

        if rule:
            try:
                for vcol in re.findall('\?\P\<(\w+)\>', rule):
                    self._columns.append({
                        'name': vcol, 'occurs': occurs, 'count': count,
                        'type': column_type, 'rule': None})
            except Exception:
                pass

    def add_columns(self, columns):
        for column in columns:
            self.add_column(**column)

    def iter_column_name(self):
        return iter([c['name'] for c in self._columns])

    def is_existing_column(self, val):
        return val in self.iter_column_name()

    # @abstractmethod
    # def get_collection(self, *args, **kwargs):
    #     raise NotImplementedError(
    #         "This is an abstract method. You can't do anything with it.")


class Resource(object):

    def __new__(self, source, name=None):

        protocol = source.protocol
        try:
            ext = import_module(
                'onegeo_manager.protocol.{0}'.format(protocol), __name__)
        except Exception as e:
            if e.__class__.__qualname__ == 'ModuleNotFoundError' \
                    and re.search("No module named 'onegeo_manager.protocol.\w+'", e.msg):
                raise ProtocolNotFoundError(
                    "No protocol named '{}'".format(protocol))
            raise e

        self = object.__new__(ext.Resource)
        self.__init__(source, name=name)
        return self
