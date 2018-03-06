from .utils import clean_my_obj
from abc import ABCMeta
from abc import abstractmethod
from functools import wraps
import re
from urllib.parse import urlparse


__all__ = ['Context', 'PropertyColumn']


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


class AbstractContext(metaclass=ABCMeta):

    def __init__(self, name, elastic_index, resource):

        self.__name = name

        self.__tags = []
        # self.__preview = []
        self.__elastic_index = None
        self.__resource = None
        self.__properties = []

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
        raise NotImplementedError('This is an abstract method. '
                                  "You can't do anything with it.")

    @abstractmethod
    def get_collection(self, *args, **kwargs):
        raise NotImplementedError("This is an abstract method. "
                                  "You can't do anything with it.")


class CswContext(AbstractContext):

    def __init__(self, name, elastic_index, resource):

        if not resource.__class__.__qualname__ == 'CswResource':
            raise TypeError("Argument should be an instance of 'CswResource'.")

        super().__init__(name, elastic_index, resource)


class GeonetContext(AbstractContext):

    def __init__(self, name, elastic_index, resource):

        if not resource.__class__.__qualname__ == 'GeonetResource':
            raise TypeError("Argument should be an "
                            "instance of 'GeonetResource'.")

        super().__init__(name, elastic_index, resource)

    def _format(f):

        @wraps(f)
        def wrapper(self, *args, **kwargs):

            def set_aliases(properties):
                new = {}
                for k, v in properties.items():
                    prop = self.get_property(k)
                    if prop.rejected:
                        continue
                    new[prop.alias or prop.name] = v
                return new

            def ypath(obj, lst):
                if type(obj) == list:
                    arr = []
                    for e in obj:
                        val = ypath(e, lst)
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
                                n + m
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
                    return ypath(obj[e], lst[1:])

            for doc in f(self, *args, **kwargs):

                properties = {}
                for p in self.iter_properties():
                    res = ypath(doc, p.rule and p.rule.split('/') or [p.name])
                    if not res:
                        continue
                    properties[p.name] = (len(res) == 1) and res[0] or res

                url = urlparse(self.resource.source.uri, allow_fragments=False)

                # TODO: Gestion du basicAuth en amont
                source_uri = '{schema}://{hostname}{port}{path}{params}'.format(
                    schema=url.scheme,
                    hostname=url.hostname,
                    port=(url.port and ':{0}'.format(url.port) or ''),
                    path=url.path,
                    params=(url.params and '?{0}'.format(url.params) or ''))

                metadata_uri = '{schema}://{hostname}{port}{path}'.format(
                    schema=url.scheme,
                    hostname=url.hostname,
                    port=(url.port and ':{0}'.format(url.port) or ''),
                    path='{0}.metadata.get?uuid={1}'.format(
                        url.path.split('.search')[0], doc['info']['uuid']))

                yield {
                    'properties': set_aliases(properties),
                    'origin': {
                        'resource': {
                            'name': self.resource.name},
                        'source': {
                            'name': self.resource.source.name,
                            'type': self.resource.source.mode,
                            'uri': source_uri},
                        'uri': metadata_uri,
                        'uuid': doc['info']['uuid']},
                    'raw_data': doc}

        return wrapper

    @_format
    def get_collection(self):
        return self.resource.source.get_collection(self.resource.name)

    def generate_elastic_mapping(self):

        analyzer = self.elastic_index.analyzer
        search_analyzer = self.elastic_index.search_analyzer

        mapping = {self.name: {
            'properties': {
                'raw_data': {
                    'dynamic': False,
                    'enabled': False,
                    'include_in_all': False,
                    'type': 'object'},
                'origin': {
                    'properties': {
                        'resource': {
                            'properties': {
                                'name': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'}}},
                        'source': {
                            'properties': {
                                'type': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'name': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'uri': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'}}},
                        'uri': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'},
                        'uuid': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'}}}}}}

        if self.tags:
            mapping[self.name]['properties']['tags'] = {
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
                'term_vector': 'yes',
                'type': 'keyword'}

        props = {}
        for p in self.iter_properties():
            if p.rejected:
                continue
            props[p.alias or p.name] = fetch_mapping(p)

        if props:
            mapping[self.name]['properties']['properties'] = {
                'properties': props}

        return clean_my_obj(mapping)


class PdfContext(AbstractContext):

    META_FIELD = ('Author', 'CreationDate', 'Creator', 'Keywords',
                  'ModDate', 'Producer', 'Subject', 'Title')

    def __init__(self, name, elastic_index, resource):

        if not resource.__class__.__qualname__ == 'PdfResource':
            raise TypeError("Argument should be an instance of 'PdfResource'.")

        super().__init__(name, elastic_index, resource)

    def _format(f):

        @wraps(f)
        def wrapper(self, *args, **kwargs):

            def set_aliases(properties):
                new = {}
                for k, v in properties.items():
                    prop = self.get_property(k)
                    if prop.rejected:
                        continue
                    new[prop.alias or prop.name] = v
                return new

            for doc in f(self, *args, **kwargs):
                yield {
                    'origin': {
                        'filename': doc['filename'],
                        'resource': {
                            'name': self.resource.name},
                        'source': {
                            'name': self.resource.source.name,
                            'uri': self.resource.source.uri,
                            'type': self.resource.source.mode}},
                    'properties': doc['properties'] and set_aliases(doc['properties']) or None,
                    'raw_data': doc['file']}

        return wrapper

    @_format
    def get_collection(self):
        return self.resource.source.get_collection(self.resource.name)

    def generate_elastic_mapping(self):

        analyzer = self.elastic_index.analyzer
        search_analyzer = self.elastic_index.search_analyzer

        mapping = {
            self.name: {
                'properties': {
                    'filename': {
                        'include_in_all': False,
                        'index': 'not_analyzed',
                        'store': False,
                        'type': 'keyword'}}}}

        if self.tags:
            mapping[self.name]['properties']['tags'] = {
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
                'term_vector': 'yes',
                'type': 'keyword'}

        props = {}
        for p in self.iter_properties():

            if p.column_type == 'pdf':
                mapping[self.name]['properties']['attachment'] = {
                    'properties': {
                        'content': {
                            'analyzer': p.analyzer,
                            'boost': p.weight,
                            # 'eager_global_ordinals'
                            # 'fielddata'
                            # 'fielddata_frequency_filter'
                            'fields': {
                                'keyword': {
                                    'index': 'not_analyzed',
                                    'type': 'keyword',
                                    'store': False}},
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
                            'type': 'text'}}}
                continue

            if p.rejected:
                continue

            props[p.alias or p.name] = fetch_mapping(p)

        if props:
            mapping[self.name]['properties']['properties'] = {
                'properties': props}

        mapping[self.name]['properties']['origin'] = {
            'properties': {
                'resource': {
                    'properties': {
                        'name': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'}}},
                'source': {
                    'properties': {
                        'name': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'},
                        'type': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'},
                        'uri': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'}}}}}

        return clean_my_obj(mapping)


class WfsContext(AbstractContext):

    def __init__(self, name, elastic_index, resource):

        if not resource.__class__.__qualname__ == 'WfsResource':
            raise TypeError("Argument should be an instance of 'WfsResource'.")

        super().__init__(name, elastic_index, resource)

    def _format(f):

        @wraps(f)
        def wrapper(self, *args, **kwargs):

            def alias(properties):
                new = {}
                for k, v in properties.items():
                    prop = self.get_property(k)
                    if prop.rejected:
                        continue
                    new[prop.alias or prop.name] = v
                return new

            for doc in f(self, *args, **kwargs):
                yield {
                    'origin': {
                        'resource': {
                            'name': self.resource.name,
                            'title': self.resource.title,
                            'abstract': self.resource.abstract,
                            'metadata_url': self.resource.metadata_url},
                        'source': {
                            'name': self.resource.source.name,
                            'title': self.resource.source.title,
                            'abstract': self.resource.source.abstract,
                            'metadata_url': self.resource.source.metadata_url,
                            'uri': self.resource.source.uri,
                            'type': self.resource.source.mode}},
                    'properties': alias(doc['properties']),
                    'raw_data': doc}

        return wrapper

    @_format
    def get_collection(self, **opts):
        return self.resource.source.get_collection(self.resource.name, **opts)

    def generate_elastic_mapping(self):

        analyzer = self.elastic_index.analyzer
        search_analyzer = self.elastic_index.search_analyzer

        if self.resource.geometry in ('Point', 'MultiPoint'):
            geometry_mapping = {'type': 'geo_point',
                                'ignore_malformed': True}
        else:
            geometry_mapping = {'type': 'geo_shape',
                                'tree': 'quadtree',
                                # 'precision': '',
                                # 'tree_levels': '',
                                # 'strategy': '',
                                'distance_error_pct': 0,
                                'orientation': 'counterclockwise',
                                'points_only': False}

        mapping = {self.name: {
            'properties': {
                'raw_data': {
                    'properties': {
                        'geometry': geometry_mapping,
                        'properties': {
                            'dynamic': False,
                            'enabled': False,
                            'include_in_all': False,
                            'type': 'object'},
                        'type': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'}}},
                'origin': {
                    'properties': {
                        'resource': {
                            'properties': {
                                'abstract': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'metadata_url': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'name': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'title': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'}}},
                        'source': {
                            'properties': {
                                'abstract': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'metadata_url': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'name': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'title': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'type': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'},
                                'uri': {
                                    'include_in_all': False,
                                    'index': 'not_analyzed',
                                    'store': False,
                                    'type': 'keyword'}}}}}}}}

        if self.tags:
            mapping[self.name]['properties']['tags'] = {
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
                'term_vector': 'yes',
                'type': 'keyword'}

        props = {}
        for p in self.iter_properties():
            if p.rejected:
                continue
            props[p.alias or p.name] = fetch_mapping(p)

        if props:
            mapping[self.name]['properties']['properties'] = {
                'properties': props}

        return clean_my_obj(mapping)


class Context(object):

    def __new__(cls, name, elastic_index, resource):

        modes = {'CswResource': CswContext,
                 'GeonetResource': GeonetContext,
                 'PdfResource': PdfContext,
                 'WfsResource': WfsContext}

        cls = modes.get(resource.__class__.__qualname__, None)
        if not cls:
            raise ValueError('Unrecognized mode.')

        self = object.__new__(cls)
        self.__init__(name, elastic_index, resource)
        return self
