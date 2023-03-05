from typing import Optional, List


class EmptyQueryResults(Exception):
    """
    Exception raised when SQL queries return an empty dataframe.
    """

    def __init__(self, fn_name: Optional[str] = None):
        self.msg = f"SQL Query results are empty"
        if fn_name is not None:
            self.msg += f' for function {fn_name}'
        super().__init__(self.msg)

    def __str__(self):
        return self.msg


class SheetParsingError(Exception):
    """
    Exception raised when a Google Sheet from the SEO team cannot be parsed.

    note: this happens when a column name cannot be found.
    """

    def __init__(self,
                 col_std: Optional[str] = None,
                 col_vars: Optional[List[str]] = None,
                 ):
        self.msg = f" Google Sheet cannot be parsed. "
        self.col_std = col_std
        self.col_vars = col_vars

        if col_std is not None:
            self.msg += f'Did not find variations of the column {col_std}. '
        if col_vars is not None:
            self.msg += f'Accepted variations are {col_vars}. '

        self.msg += 'In addition, ensure that all column names are on the same row. ' \
                    ''
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
