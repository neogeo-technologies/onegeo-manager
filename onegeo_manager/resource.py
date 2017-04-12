from abc import ABCMeta


__all__ = ['Resource']


class AbstractResource(metaclass=ABCMeta):

    COLUMN_TYPE = ['binary', 'boolean', 'byte', 'date', 'date_range',
                   'double', 'double_range', 'float', 'float_range',
                   'half_float', 'integer', 'integer_range', 'ip', 'keyword',
                   'long', 'long_range', 'scaled_float', 'short', 'text']

    def __init__(self, source, name):
        self.__source = source
        self.__name = name
        self.__columns = []

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
        return self.__source

    @property
    def name(self):
        return self.__name

    @property
    def columns(self):
        return self.__columns

    def iter_columns(self):
        return iter(self.__columns)

    def add_column(self, name, column_type=None, occurs=(0, 1), count=None):

        if self.is_existing_column(name):
            raise Exception(
                "Column '{0}' already exists.".format(name))

        if column_type and not self.authorized_column_type(column_type):
            raise TypeError(
                "Column type '{0}' is not authorized.".format(column_type),
                name)

        if self.authorized_occurs(occurs):
            raise Exception("'{0}' is malformed".format(occurs))

        self.__columns.append({'name': name, 'occurs': occurs,
                               'type': column_type, 'count': count})

    def iter_column_name(self):
        return iter([c['name'] for c in self.__columns])

    def is_existing_column(self, name):
        return name in self.iter_column_name() and True or False


class CswResource(AbstractResource):

    def __init__(self, source, name):
        super().__init__(source, name)

    def authorized_column_type(self, val):
        return val in self.COLUMN_TYPE + ['object']


class GeonetResource(AbstractResource):

    def __init__(self, source, name):
        super().__init__(source, name)

    def authorized_column_type(self, val):
        return val in self.COLUMN_TYPE + ['object']


class PdfResource(AbstractResource):

    def __init__(self, source, name):
        super().__init__(source, name)
        self.add_column('data', column_type='pdf', occurs=(1, 1))

    def authorized_column_type(self, val):
        return val in self.COLUMN_TYPE + ['pdf']


class WfsResource(AbstractResource):

    GEOMETRY_TYPE = ['Point', 'MultiPoint', 'Polygon', 'MultiPolygon',
                     'LineString', 'MultiLineString', 'GeometryCollection']

    def __init__(self, source, name):
        super().__init__(source, name)

        self.title = ''
        self.abstract = ''
        self.metadata_url = ''

        self.geometry = 'GeometryCollection'

    def authorized_geometry_type(self, val):
        return val in self.GEOMETRY_TYPE

    @staticmethod
    def column_type_mapper(val):
        switcher = {'string': 'text'}
        return switcher.get(val, val)

    @staticmethod
    def geometry_type_mapper(val):
        switcher = {'PointPropertyType': 'Point',
                    'MultiPointPropertyType': 'MultiPoint',
                    'SurfacePropertyType': 'Polygon',
                    'MultiSurfacePropertyType': 'MultiPolygon',
                    'CurvePropertyType': 'LineString',
                    'MultiCurvePropertyType': 'MultiLineString',
                    'GeometryPropertyType': 'GeometryCollection'}
        return switcher.get(val, val)

    def set_geometry_column(self, geom_type):
        t = self.geometry_type_mapper(geom_type)
        if not self.authorized_geometry_type(t):
            raise Exception("'{0}' is not an authorized geometry type".format(
                                                                    geom_type))
        self.geometry = t

    def add_column(self, name, column_type=None, occurs=(0, 1), count=None):
        column_type and self.column_type_mapper(column_type)
        super().add_column(name, column_type=None, occurs=(0, 1), count=None)


class Resource:

    def __new__(cls, source, name):

        modes = {'CswSource': CswResource,
                 'GeonetSource': GeonetResource,
                 'PdfSource': PdfResource,
                 'WfsSource': WfsResource}

        cls = modes.get(source.__class__.__qualname__, None)
        if not cls:
            raise ValueError('Unrecognized mode.')

        self = object.__new__(cls)
        self.__init__(source, name)
        return self
