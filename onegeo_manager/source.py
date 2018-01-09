from abc import ABCMeta
from abc import abstractmethod
from importlib import import_module
from re import search


__all__ = ['Source']


class AbstractSource(metaclass=ABCMeta):

    def __init__(self, uri, name, protocol):
        self.uri = uri
        self.protocol = protocol

        s = search('^[a-z0-9_]{2,100}$', name)
        if not s:
            raise ValueError("Malformed value for 'name'.")
        self.name = name

    @abstractmethod
    def get_resources(self, *args, **kwargs):
        raise NotImplementedError(
            "This is an abstract method. You can't do anything with it.")

    @abstractmethod
    def get_collection(self, *args, **kwargs):
        raise NotImplementedError(
            "This is an abstract method. You can't do anything with it.")


class Source(object):

    def __new__(self, uri, name, protocol):

        ext = import_module(
            'onegeo_manager.protocol.{0}'.format(protocol), __name__)

        self = object.__new__(ext.Source)
        self.__init__(uri, name, protocol)
        return self
