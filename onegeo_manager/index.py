__all__ = ['Index']


class Index:
    __name = None
    __index_analyzer = None
    __search_analyzer = None

    def __init__(self, name):
        self.__name = name

    @property
    def name(self):
        return self.__name

    @property
    def index_analyzer(self):
        return self.__index_analyzer

    @property
    def search_analyzer(self):
        return self.__search_analyzer

    def set_alias(self, val):
        self.__name = val

    def set_index_analyzer(self, obj):
        self.__index_analyzer = obj

    def set_search_analyzer(self, obj):
        self.__search_analyzer = obj
