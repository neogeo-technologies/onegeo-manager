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
from abc import abstractmethod
from importlib import import_module
from onegeo_manager.exception import ProtocolNotFoundError
import re


__all__ = ['IndexProfile', 'PropertyColumn']


not_searchable = lambda val: {
    'include_in_all': False,
    'index': 'not_analyzed',
    'store': False,
    'type': val}


def fetch_mapping(p):

    if p.column_type == 'text':
        if not p.searchable:
            return not_searchable(p.column_type)

        d = {
            'analyzer': p.analyzer,
            'boost': p.weight,
            # 'eager_global_ordinals'
            # 'fielddata'
            # 'fielddata_frequency_filter'
            'fields': {
                'keyword': {
                    # 'boost',
                    # 'doc_value'
                    # 'eager_global_ordinals'
                    # 'fields'
                    'ignore_above': 256,
                    # 'index': False,
                    # 'index_options': 'freqs',
                    # 'norms': True,
                    # 'null_value'
                    # 'store': False,
                    # 'similarity': 'classic',
                    'type': 'keyword'}},
            # 'include_in_all': False,
            # 'index': True,
            # 'index_options': 'offsets',
            # 'norms': True,
            # 'position_increment_gap': 100,
            # 'store': False,
            # 'search_analyzer': p.search_analyzer,
            # 'search_quote_analyzer'
            # 'similarity': 'classic',
            # 'term_vector': 'yes',
            'type': 'text'}

        if p.suggest:
            d['fields']['suggest'] = {'type': 'completion'}
        return d

    if p.column_type == 'keyword':
        return {
            'boost': p.weight,
            # 'doc_value'
            # 'eager_global_ordinals'
            # 'fields'
            'ignore_above': 256,
            # 'include_in_all': False,
            # 'index': True,
            # 'index_options': 'docs',
            # 'norms': True,
            # 'null_value'
            # 'store': False,
            # 'similarity': 'classic',
            'type': 'keyword'}

    if p.column_type in ('byte', 'double', 'double_range',
                         'float', 'float_range', 'half_float',
                         'integer', 'integer_range', 'long',
                         'long_range', 'scaled_float', 'short'):
        return {
            # 'coerce': True,
            # 'boost': p.weight,
            # 'doc_values': True,
            # 'ignore_malformed': True,
            # 'include_in_all': False,
            # 'index': True,
            # 'null_value'
            # 'store': False,
            'type': p.column_type}

        # if p.type == 'scaled_float':
        #     props[p.name]['scaling_factor'] = 10

    if p.column_type in ('date', 'date_range'):
        return {
            # 'boost': p.weight,
            # 'doc_values': True,
            'format': p.pattern or 'strict_date_optional_time||epoch_millis',
            # 'locale'
            'ignore_malformed': True,
            # 'include_in_all': False,
            # 'index': True,
            # 'null_value': 'null',
            # 'store': False,
            'type': p.column_type}

    if p.column_type == 'boolean':
        return {
            # 'boost': p.weight,
            # 'doc_values': True,
            # 'index': True,
            # 'null_value'
            # 'store': False,
            'type': p.column_type}

    if p.column_type == 'binary':
        return {
            # 'doc_values': True,
            # 'store': False,
            'type': p.column_type}

    if p.column_type == 'geo_shape':
        return {'type': p.column_type,
                'tree': 'quadtree',
                # 'precision': '',
                # 'tree_levels': '',
                # 'strategy': '',
                # 'distance_error_pct': 0,
                'orientation': 'counterclockwise',
                'points_only': False}


class PropertyColumn(object):

    COLUMN_TYPE = ['binary', 'boolean', 'byte', 'date', 'date_range',
                   'double', 'double_range', 'float', 'float_range',
                   'half_float', 'integer', 'integer_range', 'ip',
                   'keyword', 'long', 'long_range', 'pdf', 'scaled_float',
                   'short', 'text', 'object']

    def __init__(self, name, alias=None, column_type=None, occurs=None,
                 rejected=False, searchable=True, weight=None, pattern=None,
                 analyzer=None, search_analyzer=None, count=None, rule=None,
                 suggest=False):

        self._rule = rule
        self._name = name
        self._count = count

        self._alias = alias
        self._column_type = column_type or 'text'
        self._occurs = occurs
        self._rejected = rejected
        self._searchable = searchable
        self._weight = weight
        self._pattern = pattern
        self._analyzer = analyzer
        self._search_analyzer = search_analyzer
        self._suggest = suggest

    def authorized_column_type(self, val):
        return val in self.COLUMN_TYPE

    @property
    def name(self):
        return self._name

    @property
    def count(self):
        return self._count

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, val):
        self._alias = val

    @property
    def column_type(self):
        return self._column_type

    @column_type.setter
    def column_type(self, val):
        if not self.authorized_column_type(val):
            raise ValueError("Column type '{0}' not authorized.".format(val))
        self._column_type = val

    @property
    def occurs(self):
        return self._occurs

    @occurs.setter
    def occurs(self, val):
        self._occurs = val

    @property
    def rejected(self):
        return self._rejected

    @rejected.setter
    def rejected(self, val):
        if not type(val) is bool:
            raise TypeError('Input should be a boolean.')
        self._rejected = val

    @property
    def searchable(self):
        return self._searchable

    @searchable.setter
    def searchable(self, val):
        if not type(val) is bool:
            raise TypeError('Input should be a boolean.')
        self._searchable = val

    @property
    def suggest(self):
        return self._suggest

    @property
    def weight(self):
        return self._weight

    @weight.setter
    def weight(self, val):
        if val is None:
            return
        if not type(val) in [float, int]:
            raise TypeError('Input should be a float or int.')
        self._weight = val

    @property
    def pattern(self):
        return self._pattern

    @pattern.setter
    def pattern(self, val):
        # if not self._column_type == 'date':
        #     raise Exception('Pattern attribute does not exist in this context.')
        self._pattern = val

    @property
    def analyzer(self):
        return self._analyzer

    @analyzer.setter
    def analyzer(self, val):
        if val == '':
            val = None
        self._analyzer = val

    @property
    def search_analyzer(self):
        return self._search_analyzer

    @search_analyzer.setter
    def search_analyzer(self, val):
        if val == '':
            val = None
        self._search_analyzer = val

    def all(self):
        # TODO: Utiliser vars() ou self.__dict__
        return {'name': self._name,
                'count': self._count,
                'alias': self._alias,
                'type': self._column_type,
                'occurs': self._occurs,
                'rejected': self._rejected,
                'searchable': self._searchable,
                'weight': self._weight,
                'pattern': self._pattern,
                'analyzer': self._analyzer,
                'search_analyzer': self._search_analyzer,
                'suggest': self._suggest}


class AbstractIndexProfile(metaclass=ABCMeta):

    def __init__(self, name, resource):

        self._name = name

        if not resource.__class__.__qualname__ == 'Resource':
            raise TypeError("Argument should be an instance of 'Resource'.")
        self._resource = resource

        self._properties = []
        for c in self.resource.iter_columns():
            self._properties.append(PropertyColumn(
                c['name'], column_type=c['type'],
                count=c['count'], occurs=c['occurs'], rule=c['rule']))

        self._tags = []
        # self._preview = []

    @property
    def name(self):
        return self._name

    @property
    def resource(self):
        return self._resource

    def get_properties(self):
        return [prop.all() for prop in self.iter_properties()]

    def iter_properties(self, ignore=[]):
        if isinstance(ignore, list) and ignore:
            return iter(p for p in self._properties if p.name not in ignore)
        return iter(self._properties)

    def set_property(self, p):
        if not p.__class__.__qualname__ == 'PropertyColumn':
            raise TypeError(
                "Argument should be an instance of 'PropertyColumn'.")
        self._properties.append(p)

    def get_property(self, name):
        for p in self.iter_properties():
            if p.name == name:
                return p

    def update_property(self, name, param, value):
        for p in self.iter_properties():
            if p.name == name:
                if param == 'alias':
                    p.alias(value)
                if param in ('column_type', 'type'):
                    p.column_type(value)
                if param == 'occurs':
                    p.occurs(value)
                if param == 'rejected':
                    p.rejected(value)
                if param == 'searchable':
                    p.searchable(value)
                if param == 'weight':
                    p.weight(value)
                if param == 'pattern':
                    p.pattern(value)
                if param == 'rule':
                    p.rule(value)
                if param == 'analyzer':
                    p.analyzer(value)
                if param == 'search_analyzer':
                    p.search_analyzer(value)
                if param == 'suggest':
                    p.suggest(value)

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, lst):
        if not isinstance(lst, list):
            raise TypeError('Input should be a list.')
        self._tags = lst

    def iter_tags(self):
        return iter(self._tags)

    @abstractmethod
    def generate_elastic_mapping(self):
        raise NotImplementedError(
            "This is an abstract method. You can't do anything with it.")

    @abstractmethod
    def get_collection(self, *args, **kwargs):
        raise NotImplementedError(
            "This is an abstract method. You can't do anything with it.")


class IndexProfile(object):

    def __new__(self, name, resource):

        protocol = resource.source.protocol
        try:
            ext = import_module(
                'onegeo_manager.protocol.{0}'.format(protocol), __name__)
        except Exception as e:
            if e.__class__.__qualname__ == 'ModuleNotFoundError' \
                    and re.search("No module named 'onegeo_manager.protocol.\w+'", e.msg):
                raise ProtocolNotFoundError(
                    "No protocol named '{}'".format(protocol))
            raise e

        self = object.__new__(ext.IndexProfile)
        self.__init__(name, resource)
        return self
