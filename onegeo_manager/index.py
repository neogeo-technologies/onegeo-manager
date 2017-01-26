__all__ = ['Index']


class Index:

    __name = 'temporary'
    __analyzer = None
    __search_analyzer = None

    def __init__(self):
        pass

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

    def create_analyzer(self, _):
        # TODO
        pass

    def create_search_analyzer(self, _):
        # TODO
        pass

    def __create_analyzer(self, search=False):
        # TODO
        pass

    def set_analyzer(self, val):
        # TODO Vérifier si existe
        self.__analyzer = val

    def set_search_analyzer(self, val):
        # TODO Vérifier si existe

        self.__search_analyzer = val