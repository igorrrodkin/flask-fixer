class Error(Exception):
    """Base class for other exceptions"""
    pass

class IncorrectFileError(Error):
    """Incorrect json/csv file given in the function argument"""
    pass

class IncorrectFileExtensionError(Error):
    """"Incorrect extension file given in the function argument"""
    pass

class InvalidSchemaError(Error):
    """Invalid schema error"""
    pass