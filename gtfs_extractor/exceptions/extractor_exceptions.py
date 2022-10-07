from gtfs_extractor import logger


class BaseException(Exception):
    """Base custom exception class"""

    pass


class GtfsIncompleteException(BaseException):
    def __init__(self):
        self.message = "Your GTFS input is missing required files."
        logger.error(self.message)
        super().__init__(self.message)

    def __str__(self):
        return self.message


class GtfsFileNotFound(BaseException):
    def __init__(self, file_path: str):
        self.message = "Couldn't find the given file"
        self.file_path = file_path
        logger.error(self.message)
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message}: {self.file_path}"
