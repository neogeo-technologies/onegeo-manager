__all__ = ['Index']


class Index:
    __name = None
    __analyzer = None
    __search_analyzer = None

    def __init__(self, name):
        self.__name = name

    @property
    def name(self):
        return self.__name

    @property
    def analyzer(self):
        return self.__analyzer

    @property
    def search_analyzer(self):
        return self.__search_analyzer

    def set_alias(self, val):
        self.__name = val

    def set_analyzer(self, obj):
        self.__analyzer = obj

    def set_search_analyzer(self, obj):
        self.__search_analyzer = obj
