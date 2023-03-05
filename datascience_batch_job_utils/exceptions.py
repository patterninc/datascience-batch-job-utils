from typing import Optional


class SheetParsingError(Exception):
    """
    Exception raised when a Google Sheet from the SEO team cannot be parsed.
    """

    def __init__(self, more_info: Optional[str] = None):
        self.msg = f"Google Sheet cannot be parsed. "
        if more_info is not None:
            self.msg += more_info
        super().__init__(self.msg)

    def __str__(self):
        return self.msg


class NoGoogleSheetFound(Exception):
    """
    Exception raised when a Google Sheet from the SEO team cannot be found.
    """

    def __init__(self, brand: str):
        self.msg = f'Google Sheet cannot be found for brand "{brand}".'
        super().__init__(self.msg)

    def __str__(self):
        return self.msg
