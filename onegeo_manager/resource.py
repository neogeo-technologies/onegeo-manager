from abc import ABCMeta
from importlib import import_module


__all__ = ['Resource']


class AbstractResource(metaclass=ABCMeta):

    COLUMN_TYPE = ['binary', 'boolean', 'byte', 'date', 'date_range',
                   'double', 'double_range', 'float', 'float_range',
                   'half_float', 'integer', 'integer_range', 'ip', 'keyword',
                   'long', 'long_range', 'scaled_float', 'short', 'text']

    def __init__(self, source, name):

        if not source.__class__.__qualname__ == 'Source':
            raise TypeError("Argument should be an instance of 'Source'.")

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

    def add_column(self, name, column_type=None,
                   occurs=(0, 1), count=None, rule=None):

        if self.is_existing_column(name):
            raise Exception(
                "Column '{0}' already exists.".format(name))

        if column_type and not self.authorized_column_type(column_type):
            raise TypeError(
                "Column type '{0}' is not authorized.".format(column_type),
                name)

        if self.authorized_occurs(occurs):
            raise Exception("'{0}' is malformed".format(occurs))

        self.__columns.append({'name': name, 'occurs': occurs, 'count': count,
                               'type': column_type, 'rule': rule})

    def add_columns(self, columns):
        for column in columns:
            self.add_column(**column)

    def iter_column_name(self):
        return iter([c['name'] for c in self.__columns])

    def is_existing_column(self, name):
        return name in self.iter_column_name()


class Resource(object):

    def __new__(self, source, name):

        ext = import_module(
            'onegeo_manager.protocol.{0}'.format(source.protocol), __name__)

        self = object.__new__(ext.Resource)
        self.__init__(source, name)
        return self
