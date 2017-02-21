from abc import ABCMeta, abstractmethod

from .exception import GenericException


__all__ = ['PropertyColumn', 'PdfContext', 'FeatureContext']


class PropertyColumn:

    __alias = None
    __column_type = None
    __occurs = None
    __enabled = None
    __searchable = None
    __weight = None
    __pattern = None
    __index_analyzer = None
    __search_analyzer = None

    def __init__(self, name, alias=None, column_type=None, occurs=None,
                 enabled=True, searchable=False, weight=None, pattern=None,
                 index_analyzer=None, search_analyzer=None):

        self.__name = name

        self.set_alias(alias)
        self.set_column_type(column_type)
        self.set_occurs(occurs)
        self.is_enabled(enabled)
        self.is_searchable(searchable)
        if weight:
            self.set_weight(weight)
        if pattern:
            self.set_pattern(pattern)
        self.set_index_analyzer(index_analyzer)
        self.set_search_analyzer(search_analyzer)

    @property
    def name(self):
        return self.__name

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
    def enabled(self):
        return self.__enabled

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
    def index_analyzer(self):
        return self.__index_analyzer

    @property
    def search_analyzer(self):
        return self.__search_analyzer

    def set_alias(self, val):
        self.__alias = val

    def set_column_type(self, val):
        # TODO: Vérifier si 'type' est une valeur autorisée
        self.__column_type = val

    def set_occurs(self, val):
        self.__occurs = val

    def is_enabled(self, val):
        if not type(val) is bool:
            raise TypeError('Input should be a boolean.')
        self.__enabled = val

    def is_searchable(self, val):
        if not type(val) is bool:
            raise TypeError('Input should be a boolean.')
        self.__searchable = val

    def set_weight(self, val):
        if not type(val) in [float, int]:
            raise TypeError('Input should be a \'float\' or `int\'.')
        self.__weight = val

    def set_pattern(self, val):
        if not self.__column_type == 'date':
            raise GenericException(
                'Pattern attribute does not exist in this context.')
        self.__pattern = val

    def set_index_analyzer(self, val):
        self.__index_analyzer = val

    def set_search_analyzer(self, val):
        self.__search_analyzer = val

    def all(self):
        return {'name': self.name,
                'alias': self.alias,
                'type': self.column_type,
                'occurs': self.occurs,
                'enabled': self.enabled,
                'searchable': self.searchable,
                'weight': self.weight,
                'pattern': self.pattern,
                'index_analyzer': self.index_analyzer,
                'search_analyzer': self.search_analyzer}


class GenericContext(metaclass=ABCMeta):

    __tags = []
    __properties = []

    def __init__(self, elastic_index, elastic_type):

        if not elastic_index.__class__.__qualname__ == 'Index':
            raise TypeError('Argument should be an instance of \'Index\'. ')

        self.__elastic_index = elastic_index
        self.__elastic_type = elastic_type

        for c in self.__elastic_type.iter_columns():
            properties = PropertyColumn(c['name'], column_type=c['type'], occurs=c['occurs'])
            self.__properties.append(properties)

    @property
    def elastic_type(self):
        return self.__elastic_type

    @property
    def elastic_index(self):
        return self.__elastic_index

    @property
    def tags(self):
        return self.__tags

    def iter_properties(self):
        return iter(self.__properties)

    def get_property(self, name):
        for p in self.iter_properties():
            if p.name == name:
                return p

    def iter_tags(self):
        return iter(self.__tags)

    def set_tags(self, lst):
        if type(lst) is not list:
            raise TypeError('Input should be a list.')
        self.__tags = lst

    def set_previews(self, l):

        if type(l) is not list:
            raise TypeError('Input should be a list.')

        for name in iter(l):

            if type(name) is not str:
                raise TypeError('List values should be strings.')

            if not self.elastic_type.is_existing_column(name):
                raise Exception(
                            'Property \'{0}\' does not exist.'.format(name))

    @abstractmethod
    def generate_elastic_mapping(self):
        raise NotImplementedError('This is an abstract method. '
                                  'You can\'t do anything with it.')


class PdfContext(GenericContext):

    def __init__(self, elastic_index, elastic_type):

        if not elastic_type.__class__.__qualname__ == 'PdfType':
            raise TypeError('Argument should be an instance of \'PdfType\'.')

        if not elastic_index.__class__.__qualname__ == 'Index':
            raise TypeError('Argument should be an instance of \'Index\'. ')

        self.__elastic_index = elastic_index
        self.__elastic_type = elastic_type

        for c in self.__elastic_type.iter_columns():
            if c['name'] == 'file' and c['type'] == 'binary':
                continue
            properties = PropertyColumn(c['name'], column_type=c['type'], occurs=c['occurs'])
            self.__properties.append(properties)

    def generate_elastic_mapping(self):

        m = self.elastic_type.name
        mapping = {m: {
            'properties': {
                'pdf': {
                    'type': 'attachment',
                    'fields': {
                        # TODO: date, title, name, author...
                        'content': {
                            'store': True,
                            'index_analyzer': self.elastic_index.index_analyzer,
                            'search_analyzer': self.elastic_index.search_analyzer,
                            'term_vector': 'with_positions_offsets'}}},
                'tags': {
                    'type': 'string',
                    'include_in_all': True,
                    'store': True,
                    'index': 'analyzed',
                    'index_analyzer': self.elastic_index.index_analyzer,
                    'search_analyzer': self.elastic_index.search_analyzer,
                    'index_options': 'docs',
                    'term_vector': True},
                'preview': {
                    'type': 'string',
                    'include_in_all': False,
                    'store': False,
                    'index': 'not_analyzed'}}}}

        props = {}
        for p in self.iter_properties():
            if not p.enabled:
                continue

            props[p.name] = {'type': p.column_type}

            if not p.searchable:
                props[p.name] = {'include_in_all': False,
                                 'store': False,
                                 'index': 'not_analyzed'}

            elif p.type == 'string':
                props[p.name] = {'include_in_all': True,
                                 'store': True,
                                 'index': 'analyzed',
                                 'index_analyzer': self.elastic_index.index_analyzer,
                                 'search_analyzer': self.elastic_index.search_analyzer,
                                 'term_vector': True,
                                 'index_options': 'docs',
                                 'boost': p.weight}

            elif p.type in ['byte', 'double', 'integer', 'float', 'long', 'short']:
                props[p.name] = {'include_in_all': True,
                                 'index': True,
                                 'precision_step': 4,
                                 'null_value': None,
                                 'boost': p.weight}

            elif p.type == 'date':
                props[p.name] = {
                        'include_in_all': True,
                        'index': True,
                        'precision_step': 4,
                        'null_value': None,
                        'format': p.pattern,
                        'boost': p.weight}

            elif p.type == 'boolean':  # TODO
                props[p.name] = {}

        if props:
            mapping[m]['properties']['meta'] = {'properties': props}

        return mapping


class FeatureContext(GenericContext):

    def __init__(self, elastic_index, elastic_type):

        if not elastic_type.__class__.__qualname__ == 'FeatureType':
            raise TypeError('Argument should be an instance of \'FeatureType\'. ')

        super().__init__(elastic_index, elastic_type)

    def generate_elastic_mapping(self):
        # TODO
        pass
