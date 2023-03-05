import os
import time
import itertools
from ratelimit import limits, sleep_and_retry
from ssl import SSLError
from typing import Dict, List, Tuple
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.exceptions import TransportError

from datascience_batch_job_utils import configs
from datascience_batch_job_utils.exceptions import SheetParsingError
from datascience_batch_job_utils.exceptions import NoGoogleSheetFound


def get_google_auth_credentials() -> Credentials:

    private_key = os.getenv('GOOGLE_PRIVATE_KEY', None)
    assert private_key is not None

    info = {
        "type": "service_account",
        "project_id": "access-sheets-from-seo-team",
        "private_key_id": "47452f078d3c46574d54f73db6a22acdff4bc0d0",
        "private_key": private_key,
        "client_email": "service-1@access-sheets-from-seo-team.iam.gserviceaccount.com",
        "client_id": "110659737561597462218",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-1%40access-sheets-from-seo-team.iam.gserviceaccount.com"
    }
    creds = Credentials.from_service_account_info(info)

    return creds


def get_name2spreadsheet_id() -> Dict[str, str]:
    """
    get IDs and names of Google sheet containing SEO Content
    """

    creds = get_google_auth_credentials()  # authenticate by looking for private key in environment variables
    service = build('drive', 'v3', credentials=creds)

    results = service.files().list(
        q=f'name contains "{configs.GoogleSheets.name_contains}"',
        pageSize=128,  # TODO how to determine the number of files to show? show all?
        spaces='drive',
        fields="nextPageToken, files(id, name)").execute()

    items = results.get('files', [])
    res = {item['name']: item['id'] for item in items}

    res[configs.GoogleSheets.name_for_testing] = configs.GoogleSheets.id_for_testing

    return res


def get_name2sheet_id(spreadsheet_id: str,
                      ) -> Dict[str, int]:
    """
    a sheet is a tab within a spreadsheet. each sheet has its own unique name and ID
    """

    # authenticate by looking for private key in environment variables
    creds = get_google_auth_credentials()
    service = build('sheets', 'v4', credentials=creds)
    service_spreadsheets = service.spreadsheets()

    # get information about the spreadsheet
    http_request = service_spreadsheets.get(spreadsheetId=spreadsheet_id)
    result = execute_request_with_rate_limit(http_request)

    # get names and IDs of all sheets/tabs in the spreadsheet
    res = {sheet['properties']['title']: sheet['properties']['sheetId']
           for sheet in result['sheets']
           }

    return res


def get_values_from_google_sheet(spreadsheet_range: str,
                                 spreadsheet_id: str,
                                 max_num_retry: int = 3,
                                 verbose: bool = False,
                                 ) -> List[List]:
    if verbose:
        print(f'Getting values from spreadsheet with range={spreadsheet_range}')

    # authenticate by looking for private key in environment variables
    creds = get_google_auth_credentials()
    service = build('sheets', 'v4', credentials=creds)
    service_spreadsheets = service.spreadsheets()

    http_get_request = service_spreadsheets.values().get(spreadsheetId=spreadsheet_id, range=spreadsheet_range)

    result = False
    retry = 0
    while not result:
        retry += 1
        try:
            result = execute_request_with_rate_limit(http_get_request)
        except SSLError as ex:
            if retry < max_num_retry:
                print(f'Encountered {ex}. Waiting 1s and then retrying.')
                print(retry, max_num_retry)
                time.sleep(1)
            else:
                raise ex
        except TransportError as ex:
            if retry < max_num_retry:
                print(f'Encountered {ex}. Waiting 1s and then retrying.')
                print(retry, max_num_retry)
                time.sleep(1)
        except TimeoutError as ex:
            if retry < max_num_retry:
                print(f'Encountered {ex}. Waiting 1s and then retrying.')
                print(retry, max_num_retry)
                time.sleep(1)
            else:
                raise ex
        except Exception as ex:
            raise ex

    try:
        res = result['values']
    except KeyError:  # empty range
        print(f'Did not find values in spreadsheet with range {spreadsheet_range}.')
        return []
    else:
        return res


# note: Google API read rate-limit is 60 per user per minute
@sleep_and_retry
@limits(calls=60, period=60)
def execute_request_with_rate_limit(http_get_request):
    result = http_get_request.execute()
    return result


def read_from_google_sheets(spreadsheet_id: str,
                            brand: str,
                            column_name2variations: Dict[str, List[str]],
                            row_idx_with_column_names: int = 2,  # 3rd row
                            start_row: int = 4,  # in which row does the data start?
                            ) -> pd.DataFrame:
    """
    get data from Google Sheet.

    note: by omitting last row number, Google API returns range up to last non-empty row.
    """

    # make sure variations are lower-cased, because all column names in sheet will be lower-cased
    for variations in column_name2variations.values():
        for v in variations:
            assert v == v.lower()

    # get values from Google sheet
    try:
        gs_values = get_values_from_google_sheet(spreadsheet_range=configs.GoogleSheets.range,
                                                 spreadsheet_id=spreadsheet_id)
    except HttpError:  # sheet cannot be found
        raise NoGoogleSheetFound(brand=brand)

    # get column names
    # note: a column name may be empty. we must fill it so that it is not dropped and raises an error below
    columns = ['<empty>'] * max([len(row) for row in gs_values])
    for n, v in enumerate(gs_values[row_idx_with_column_names]):
        columns[n] = v

    # get values. insert empty values when a row is not as long as the longest row
    gs_values_parsed = []
    for row in gs_values[row_idx_with_column_names + 1:]:
        gs_values_parsed.append([None if i >= len(row) else row[i] for i in range(len(columns))])
    df_gs = pd.DataFrame.from_records(data=gs_values_parsed,
                                      columns=columns,
                                      )

    # fill empty column names, and lowercase
    df_gs.columns = [c if c else alpha
                     for alpha, c in zip(column_range_cycler(), df_gs.columns)]
    # lowercase column names
    df_gs.columns = [c.lower() for c in df_gs.columns]
    # add information about the row where listing text for an ASIN should be written to
    df_gs['Row'] = df_gs.index + start_row
    # fill empty column names or bad columns
    df_gs.columns = [c if c and not c.startswith('<')  # e.g. "<100 characters"
                     else gs_range
                     for gs_range, c in zip(column_range_cycler(), df_gs.columns)]
    # rename column variants to standard name,
    # and get alphabetic character to locate the column in the sheet (e.g. A, B, )
    # note: we get column of titles and ASINs so that we can write to those columns later
    header2range = {cn: None for cn in column_name2variations}
    for col_std, col_vars in column_name2variations.items():
        for gs_range, header_in_sheet in zip(column_range_cycler(), df_gs.columns):
            try:
                col_vars.index(header_in_sheet.lower().strip())
            except ValueError:
                continue
            else:
                df_gs.rename(columns={header_in_sheet: col_std}, inplace=True)
                # also save info where each header is in the sheet for later
                header2range[col_std] = gs_range
                break
        else:
            raise SheetParsingError(col_std=col_std, col_vars=col_vars)

    # drop rows with empty string
    for name in column_name2variations:
        df_gs[name] = df_gs[name].apply(lambda value: None if value == '' else value)
        df_gs = df_gs.dropna(axis=0, how='any', subset=[name])

    # exclude irrelevant columns
    df_gs = df_gs[['Row'] + list(column_name2variations.keys())]

    return df_gs


def column_range_cycler():
    """
    generates 'A', 'B', ..., 'AA', 'AB', ... until ZZ
    """

    alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
                'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

    # first, loop over alphabet once
    for a in alphabet:
        yield a

    alphabet_cycle = itertools.cycle(alphabet)

    for a1_repeated in (itertools.repeat(a, len(alphabet)) for a in alphabet):
        for a1 in a1_repeated:
            yield f'{a1}{next(alphabet_cycle)}'


def find_spreadsheet_with_seo_content(brand: str,
                                      ) -> Tuple[str, str]:

    # get all sheets that can be found on Google Drive
    name2spreadsheet_id = get_name2spreadsheet_id()

    # find all matching sheets
    spreadsheet_names_matched = []
    for _spreadsheet_name in name2spreadsheet_id:

        if _spreadsheet_name.startswith(brand):
            print(f'Found spreadsheet "{_spreadsheet_name}" that contains "{brand}".')
            spreadsheet_names_matched.append(_spreadsheet_name)

    if not spreadsheet_names_matched:
        raise NoGoogleSheetFound(brand=brand)

    # select spreadsheet name with the shortest name
    spreadsheet_name = min(spreadsheet_names_matched, key=lambda name: len(name))
    spreadsheet_id = name2spreadsheet_id[spreadsheet_name]

    return spreadsheet_name, spreadsheet_id
