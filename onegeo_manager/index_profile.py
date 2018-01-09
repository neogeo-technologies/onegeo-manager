from abc import ABCMeta
from abc import abstractmethod
from importlib import import_module


__all__ = ['IndexProfile', 'PropertyColumn']


def fetch_mapping(p):

    if p.column_type == 'text':
        if not p.searchable:
            return {'include_in_all': False,
                    'index': 'not_analyzed',
                    'store': False,
                    'type': 'text'}

        return {
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
                    # 'ignore_above'
                    'index': False,
                    'index_options': 'freqs',
                    'norms': True,
                    # 'null_value'
                    'store': False,
                    'similarity': 'classic',
                    'type': 'keyword'}},
            'include_in_all': False,
            'index': True,
            'index_options': 'offsets',
            'norms': True,
            'position_increment_gap': 100,
            'store': False,
            'search_analyzer': p.search_analyzer,
            # 'search_quote_analyzer'
            'similarity': 'classic',
            'term_vector': 'yes',
            'type': 'text'}

    if p.column_type == 'keyword':
        return {
            'boost': p.weight,
            # 'doc_value'
            # 'eager_global_ordinals'
            # 'fields'
            # 'ignore_above'
            'include_in_all': False,
            'index': True,
            'index_options': 'docs',
            'norms': True,
            # 'null_value'
            'store': False,
            'similarity': 'classic',
            'type': 'keyword'}

    if p.column_type in ('byte', 'double', 'double_range',
                         'float', 'float_range', 'half_float',
                         'integer', 'integer_range', 'long',
                         'long_range', 'scaled_float', 'short'):
        return {
            'coerce': True,
            'boost': p.weight,
            'doc_values': True,
            'ignore_malformed': True,
            'include_in_all': False,
            'index': True,
            # 'null_value'
            'store': False,
            'type': p.column_type}

        # if p.type == 'scaled_float':
        #     props[p.name]['scaling_factor'] = 10

    if p.column_type in ('date', 'date_range'):
        return {
            'boost': p.weight,
            'doc_values': True,
            'format': p.pattern,
            # 'locale'
            'ignore_malformed': True,
            'include_in_all': False,
            'index': True,
            # 'null_value'
            'store': False,
            'type': p.column_type}

    if p.column_type == 'boolean':
        return {
            'boost': p.weight,
            'doc_values': True,
            'index': True,
            # 'null_value'
            'store': False,
            'type': p.column_type}

    if p.column_type == 'binary':
        return {
            'doc_values': True,
            'store': False,
            'type': p.column_type}


class PropertyColumn(object):

    COLUMN_TYPE = ['binary', 'boolean', 'byte', 'date', 'date_range',
                   'double', 'double_range', 'float', 'float_range',
                   'half_float', 'integer', 'integer_range', 'ip',
                   'keyword', 'long', 'long_range', 'pdf', 'scaled_float',
                   'short', 'text']

    def __init__(self, name, alias=None, column_type=None, occurs=None,
                 rejected=False, searchable=True, weight=None, pattern=None,
                 analyzer=None, search_analyzer=None, count=None, rule=None):

        self.__rule = rule
        self.__name = name
        self.__count = count

        self.__alias = None
        self.__column_type = None
        self.__occurs = None
        self.__rejected = False
        self.__searchable = True
        self.__weight = None
        self.__pattern = None
        self.__analyzer = None
        self.__search_analyzer = None

        self.set_alias(alias)
        self.set_column_type(column_type or 'text')
        self.set_occurs(occurs)
        self.is_rejected(rejected)
        self.is_searchable(searchable)
        if weight:
            self.set_weight(weight)
        if pattern:
            self.set_pattern(pattern)
        self.set_analyzer(analyzer)
        self.set_search_analyzer(search_analyzer)

    def authorized_column_type(self, val):
        return val in self.COLUMN_TYPE

    @property
    def rule(self):
        return self.__rule

    @property
    def name(self):
        return self.__name

    @property
    def count(self):
        return self.__count

    @property
    def alias(self):
        return self.__alias

    @property
    def column_type(self):
        return self.__column_type

    @property
    def occurs(self):
        return self.__occurs

    @property
    def rejected(self):
        return self.__rejected

    @property
    def searchable(self):
        return self.__searchable

    @property
    def weight(self):
        return self.__weight

    @property
    def pattern(self):
        return self.__pattern

    @property
    def analyzer(self):
        return self.__analyzer

    @property
    def search_analyzer(self):
        return self.__search_analyzer

    def set_alias(self, val):
        self.__alias = val

    def set_rule(self, val):
        self.__rule = val

    def set_column_type(self, val):
        if not self.authorized_column_type(val):
            raise TypeError(
                "Column type '{0}' is not authorized.".format(val))
        self.__column_type = val

    def set_occurs(self, val):
        self.__occurs = val

    def is_rejected(self, val):
        if not type(val) is bool:
            raise TypeError('Input should be a boolean.')
        self.__rejected = val

    def is_searchable(self, val):
        if not type(val) is bool:
            raise TypeError('Input should be a boolean.')
        self.__searchable = val

    def set_weight(self, val):
        if val is None:
            return
        if not type(val) in [float, int]:
            raise TypeError('Input should be a float or int.')
        self.__weight = val

    def set_pattern(self, val):
        # if not self.__column_type == 'date':
        #     raise Exception('Pattern attribute does not exist in this context.')
        self.__pattern = val

    def set_analyzer(self, val):
        if val == '':
            val = None
        self.__analyzer = val

    def set_search_analyzer(self, val):
        if val == '':
            val = None
        self.__search_analyzer = val

    def all(self):
        return {'name': self.name,
                'count': self.count,
                'alias': self.alias,
                'type': self.column_type,
                'occurs': self.occurs,
                'rejected': self.rejected,
                'searchable': self.searchable,
                'weight': self.weight,
                'pattern': self.pattern,
                'analyzer': self.analyzer,
                'search_analyzer': self.search_analyzer}


class AbstractIndexProfile(metaclass=ABCMeta):

    def __init__(self, name, elastic_index, resource):

        self.__name = name

        self.__tags = []
        # self.__preview = []
        self.__elastic_index = None
        self.__resource = None
        self.__properties = []

        if not resource.__class__.__qualname__ == 'Resource':
            raise TypeError("Argument should be an instance of 'Resource'.")

        if not elastic_index.__class__.__qualname__ == 'Index':
            raise TypeError("Argument should be an instance of 'Index'.")

        self.set_resource(resource)
        self.set_elastic_index(elastic_index)

        for c in self.resource.iter_columns():
            self.__properties.append(PropertyColumn(c['name'],
                                                    column_type=c['type'],
                                                    count=c['count'],
                                                    occurs=c['occurs'],
                                                    rule=c['rule']))

    @property
    def name(self):
        return self.__name

    @property
    def resource(self):
        return self.__resource

    @property
    def elastic_index(self):
        return self.__elastic_index

    @property
    def tags(self):
        return self.__tags

    def set_resource(self, resource):
        self.__resource = resource

    def set_elastic_index(self, elastic_index):
        self.__elastic_index = elastic_index

    def set_property(self, p):
        if not p.__class__.__qualname__ == 'PropertyColumn':
            raise TypeError("Argument should be an "
                            "instance of 'PropertyColumn'.")
        self.__properties.append(p)

    def iter_properties(self):
        return iter(self.__properties)

    def get_properties(self):
        return [prop.all() for prop in self.iter_properties()]

    def get_property(self, name):
        for p in self.iter_properties():
            if p.name == name:
                return p

    def update_property(self, name, param, value):
        for p in self.iter_properties():
            if p.name == name:
                if param == 'alias':
                    p.set_alias(value)
                if param in ('column_type', 'type'):
                    p.set_column_type(value)
                if param == 'occurs':
                    p.set_occurs(value)
                if param == 'rejected':
                    p.is_rejected(value)
                if param == 'searchable':
                    p.is_searchable(value)
                if param == 'weight':
                    p.set_weight(value)
                if param == 'pattern':
                    p.set_pattern(value)
                if param == 'rule':
                    p.set_rule(value)
                if param == 'analyzer':
                    p.set_analyzer(value)
                if param == 'search_analyzer':
                    p.set_search_analyzer(value)

    # def update_property(self, name, **params):
    #     for p in self.iter_properties():
    #         if p.name == name:
    #             for k, v in params.items():
    #                 if k == 'alias':
    #                     p.set_alias(v)
    #                 if k in ('column_type', 'type'):
    #                     p.set_column_type(v)
    #                 if k == 'occurs':
    #                     p.set_occurs(v)
    #                 if k == 'rejected':
    #                     p.is_rejected(v)
    #                 if k == 'searchable':
    #                     p.is_searchable(v)
    #                 if k == 'weight':
    #                     p.set_weight(v)
    #                 if k == 'pattern':
    #                     p.set_pattern(v)
    #                 if k == 'analyzer':
    #                     p.set_analyzer(v)
    #                 if k == 'search_analyzer':
    #                     p.set_search_analyzer(v)

    def iter_tags(self):
        return iter(self.__tags)

    def set_tags(self, lst):
        if type(lst) is not list:
            raise TypeError('Input should be a list.')
        self.__tags = lst

    # def set_previews(self, l):
    #
    #     if type(l) is not list:
    #         raise TypeError('Input should be a list.')
    #
    #     for name in iter(l):
    #
    #         if type(name) is not str:
    #             raise TypeError('List values should be strings.')
    #
    #         if not self.resource.is_existing_column(name):
    #             raise Exception(
    #                         'Property \'{0}\' does not exist.'.format(name))
    #
    #         self.__preview.append(name)

    @abstractmethod
    def generate_elastic_mapping(self):
        raise NotImplementedError(
            "This is an abstract method. You can't do anything with it.")

    @abstractmethod
    def get_collection(self, *args, **kwargs):
        raise NotImplementedError(
            "This is an abstract method. You can't do anything with it.")


class IndexProfile(object):

    def __new__(self, name, elastic_index, resource):

        ext = import_module(
            'onegeo_manager.protocol.{0}'.format(resource.source.protocol), __name__)

        self = object.__new__(ext.IndexProfile)
        self.__init__(name, elastic_index, resource)
        return self
