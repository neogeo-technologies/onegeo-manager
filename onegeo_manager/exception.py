class GenericException(Exception):
    def __init__(self, *args):
        self.args = args

    def __str__(self):
        return str(self.args)


class OGCExceptionReport(GenericException):
    def __init__(self, *args):
        super().__init__(*args)


class OGCDocumentError(GenericException):
    def __init__(self, *args):
        super().__init__(*args)
