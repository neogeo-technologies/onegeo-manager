from abc import ABCMeta, abstractmethod

from .exception import GenericException
from .utils import clean_my_obj


__all__ = ['PropertyColumn', 'PdfContext', 'FeatureContext']


class PropertyColumn:

    __alias = None
    __column_type = None
    __occurs = None
    __rejected = False
    __searchable = True
    __weight = None
    __pattern = None
    __analyzer = None
    __search_analyzer = None

    def __init__(self, name, alias=None, column_type=None, occurs=None,
                 rejected=False, searchable=True, weight=None, pattern=None,
                 analyzer=None, search_analyzer=None):

        self.__name = name

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

    def set_column_type(self, val):
        # TODO: Vérifier si 'type' est une valeur autorisée
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
        if not type(val) in [float, int]:
            raise TypeError('Input should be a \'float\' or `int\'.')
        self.__weight = val

    def set_pattern(self, val):
        if not self.__column_type == 'date':
            raise GenericException(
                'Pattern attribute does not exist in this context.')
        self.__pattern = val

    def set_analyzer(self, val):
        self.__analyzer = val

    def set_search_analyzer(self, val):
        self.__search_analyzer = val

    def all(self):
        return {'name': self.name,
                'alias': self.alias,
                'type': self.column_type,
                'occurs': self.occurs,
                'rejected': self.rejected,
                'searchable': self.searchable,
                'weight': self.weight,
                'pattern': self.pattern,
                'analyzer': self.analyzer,
                'search_analyzer': self.search_analyzer}


class GenericContext(metaclass=ABCMeta):

    __tags = []
    __properties = []
    # __preview = []
    __elastic_index = None
    __elastic_type = None

    def __init__(self, elastic_index, elastic_type):

        if not elastic_index.__class__.__qualname__ == 'Index':
            raise TypeError('Argument should be an instance of \'Index\'. ')

        self.set_elastic_index(elastic_index)
        self.set_elastic_type(elastic_type)

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

    def set_elastic_type(self, elastic_type):
        self.__elastic_type = elastic_type

    def set_elastic_index(self, elastic_index):
        self.__elastic_index = elastic_index

    def set_property(self, property):
        if not property.__class__.__qualname__ == 'PropertyColumn':
            raise TypeError('Argument should be an instance of \'PropertyColumn\'.')
        self.__properties.append(property)

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
    #         if not self.elastic_type.is_existing_column(name):
    #             raise Exception(
    #                         'Property \'{0}\' does not exist.'.format(name))
    #
    #         self.__preview.append(name)

    @abstractmethod
    def generate_elastic_mapping(self):
        raise NotImplementedError('This is an abstract method. '
                                  'You can\'t do anything with it.')


class PdfContext(GenericContext):

    def __init__(self, elastic_index, elastic_type):

        if not elastic_type.__class__.__qualname__ == 'PdfType':
            raise TypeError('Argument should be an instance of \'PdfType\'.')

        self.set_elastic_index(elastic_index)

        if not elastic_index.__class__.__qualname__ == 'Index':
            raise TypeError('Argument should be an instance of \'Index\'. ')

        self.set_elastic_type(elastic_type)

        for c in self.elastic_type.iter_columns():
            properties = PropertyColumn(c['name'], column_type=c['type'], occurs=c['occurs'])
            self.set_property(properties)

    def generate_elastic_mapping(self):

        analyzer = self.elastic_index.analyzer
        search_analyzer = self.elastic_index.search_analyzer

        type_name = self.elastic_type.name

        mapping = {type_name: {'properties': {}}}

        if self.tags:
            mapping[type_name]['properties'].update({
                'tags': {
                    'analyzer': analyzer,
                    'boost': 1.0,
                    # 'doc_value'
                    # 'eager_global_ordinals'
                    # 'fields'
                    # 'ignore_above'
                    # 'include_in_all'
                    'index': True,
                    'index_options': 'docs',
                    'norms': True,
                    # 'null_value'
                    'store': False,
                    'search_analyzer': search_analyzer,
                    'similarity': 'classic',
                    'term_vector': 'yes'}})

        props = {}
        for p in self.iter_properties():
            if p.rejected:
                continue

            props[p.name] = {'type': p.column_type}

            if not p.searchable:
                props[p.name] = {
                    'include_in_all': False,
                    'store': False,
                    'index': 'not_analyzed'}

            elif p.column_type == 'text':

                props[p.name] = {
                    'analyzer': p.analyzer,
                    'boost': p.weight,
                    # 'eager_global_ordinals'
                    # 'fielddata'
                    # 'fielddata_frequency_filter'
                    # 'fields'
                    # 'include_in_all'
                    'index': True,
                    'index_options': 'docs',
                    'norms': True,
                    'position_increment_gap': 100,
                    'store': False,
                    'search_analyzer': p.search_analyzer,
                    # 'search_quote_analyzer'
                    'similarity': 'classic',
                    'term_vector': 'yes'}

            elif p.column_type == 'keyword':

                props.update({p.name: {
                                'analyzer': p.analyzer,
                                'boost': p.weight,
                                # 'doc_value'
                                # 'eager_global_ordinals'
                                # 'fields'
                                # 'ignore_above'
                                # 'include_in_all'
                                'index': True,
                                'index_options': 'docs',
                                'norms': True,
                                # 'null_value'
                                'store': False,
                                'search_analyzer': p.search_analyzer,
                                'similarity': 'classic',
                                'term_vector': 'yes'}})

            elif p.column_type in ('byte', 'double', 'double_range',
                            'float', 'float_range', 'half_float',
                            'integer', 'integer_range', 'long',
                            'long_range', 'scaled_float', 'short'):

                props.update({p.name: {
                                'coerce': True,
                                'boost': p.weight,
                                'doc_values': True,
                                'ignore_malformed': True,
                                # 'include_in_all'
                                'index': True,
                                # 'null_value'
                                'store': False}})

                # if p.type == 'scaled_float':
                #     props[p.name]['scaling_factor'] = 10

            elif p.column_type in ('date', 'date_range'):

                props.update({p.name: {
                                'boost': p.weight,
                                'doc_values': True,
                                'format': p.pattern,
                                # 'locale'
                                'ignore_malformed': True,
                                # 'include_in_all'
                                'index': True,
                                # 'null_value'
                                'store': False}})

            elif p.column_type == 'boolean':

                props.update({p.name: {
                                'boost': p.weight,
                                'doc_values': True,
                                'index': True,
                                # 'null_value'
                                'store': False}})

            elif p.column_type == 'binary':

                props.update({p.name: {
                                'doc_values': True,
                                'store': False}})

            elif p.column_type == 'pdf':

                mapping[type_name]['attachment.{0}'.format(p.name)] = {
                        'type': 'text',
                        'fields': {
                            'content': {
                                'analyzer': p.analyzer,
                                'search_analyzer': p.search_analyzer,
                                'term_vector': 'with_positions_offsets'}}}

        if props:
            mapping[type_name]['properties'].update({
                                        'meta': {'properties': props}})

        return clean_my_obj(mapping)


class FeatureContext(GenericContext):

    def __init__(self, elastic_index, elastic_type):

        if not elastic_type.__class__.__qualname__ == 'FeatureType':
            raise TypeError('Argument should be an instance of \'FeatureType\'. ')

        super().__init__(elastic_index, elastic_type)

    def generate_elastic_mapping(self):
        # TODO
        pass
