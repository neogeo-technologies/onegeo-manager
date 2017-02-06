from abc import ABCMeta, abstractmethod
from json import dumps

from .utils import clean_my_dict
from .exception import GenericException


__all__ = ['PropertyColumn', 'PdfContext', 'FeatureContext']


class PropertyColumn:

    __name = None
    __occurs = None
    __c_type = None
    __alias = None
    __enabled = None
    __searchable = None
    __weight = None

    def __init__(self, name, column_type=None, occurs=None, alias=None,
                 enabled=True, searchable=False, weight=None, pattern=None):

        self.__name = name

        self.set_occurs(occurs)
        self.set_column_type(column_type)
        self.set_alias(alias)
        self.is_enabled(enabled)
        self.is_searchable(searchable)
        if weight:
            self.set_weight(weight)

        self.__pattern = None
        if pattern:
            self.set_pattern(pattern)

        self.__fields = []

    @property
    def name(self):
        return self.__name

    @property
    def occurs(self):
        return self.__occurs

    @property
    def column_type(self):
        return self.__c_type

    @property
    def alias(self):
        return self.__alias

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
    def fields(self):
        return self.__fields

    @property
    def pattern(self):
        return self.__pattern

    def set_column_type(self, val):
        # TODO: Vérifier si 'type' est une valeur autorisée
        self.__c_type = val

    def set_occurs(self, val):
        self.__occurs = val

    def set_alias(self, val):
        self.__alias = val

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
        if not self.__c_type == 'date':
            raise GenericException(
                'Pattern attribute does not exist in this context.')
        self.__pattern = val


class GenericContext(metaclass=ABCMeta):

    def __init__(self, i, t):

        if not i.__class__.__qualname__ == 'Index':
            raise TypeError('Argument should be an instance of \'Index\'. ')

        self.__i = i
        self.__t = t

        self.__tags = []
        self.__properties = []

        for c in self.__t.iter_columns():
            properties = PropertyColumn(c['name'], column_type=c['type'], occurs=c['occurs'])
            self.__properties.append(properties)

    @property
    def elastic_index(self):
        return self.__i.name

    @property
    def elastic_type(self):
        return self.__t.name

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

    def set_previews(self, lst):

        if type(lst) is not list:
            raise TypeError('Input should be a list.')

        for name in iter(lst):

            if type(name) is not str:
                raise TypeError('List values should be strings.')

            if not self.__t.is_existing_column(name):
                raise Exception(
                            'Property \'{0}\' does not exist.'.format(name))

    @abstractmethod
    def generate_elastic_mapping(self):
        raise NotImplementedError('This is an abstract method. '
                                  'You can\'t do anything with it.')


class PdfContext(GenericContext):

    def __init__(self, i, t):
        if not t.__class__.__qualname__ == 'PdfType':
            raise TypeError('Argument should be an instance of \'PdfType\'.')
        super().__init__(i, t)

    def generate_elastic_mapping(self):

        mapping = {
            'index': self.elastic_index,
            'type': self.elastic_type,
            'body': {
                'properties': {
                    'pdf': {
                        'type': 'attachment',
                        'fields': {
                            # TODO: date, title, name, author...
                            'content': {
                                'store': True,
                                'analyzer': self.__i.analyzer,
                                'search_analyzer': self.__i.search_analyzer,
                                'term_vector': 'with_positions_offsets'}}},
                    'tags': {
                        'type': 'string',
                        'include_in_all': True,
                        'store': True,
                        'index': 'analyzed',
                        'analyzer': self.__i.analyzer,
                        'search_analyzer': self.__i.search_analyzer,
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

            props[p.name] = {'type': p.type}

            if not p.searchable:
                props[p.name] = {'include_in_all': False,
                                 'store': False,
                                 'index': 'not_analyzed'}

            elif p.type == 'string':
                props[p.name] = {'include_in_all': True,
                                 'store': True,
                                 'index': 'analyzed',
                                 'analyzer': self.__i.analyzer,
                                 'search_analyzer': self.__i.search_analyzer,
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
            mapping['body']['properties']['meta'] = {'properties': props}

        return dumps(clean_my_dict(mapping))


class FeatureContext(GenericContext):

    def __init__(self, i, t):
        if not t.__class__.__qualname__ == 'FeatureType':
            raise TypeError('Argument should be an instance of \'FeatureType\'. ')
        super().__init__(i, t)

    def generate_elastic_mapping(self):
        # TODO
        pass
