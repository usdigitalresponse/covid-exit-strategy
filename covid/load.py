import logging

import gspread
from df2gspread import df2gspread
from oauth2client.service_account import ServiceAccountCredentials


logger = logging.getLogger(__name__)


def post_dataframe_to_google_sheets(
    df, workbook_key, tab_name, credentials, nan_replacement_value=""
):
    if nan_replacement_value is not None:
        df = df.fillna(value=nan_replacement_value)

    logger.info(
        f"Beginning to upload data to workbook {workbook_key} and tab {tab_name}..."
    )
    df2gspread.upload(
        df=df,
        gfile=workbook_key,
        wks_name=tab_name,
        credentials=credentials,
        # Do not include the index in the upload.
        row_names=False,
        col_names=True,
    )
    logger.info("Finished uploading data.")


# TODO(lbrown): this was created when I was using the Sheets API, at this point we may only need the credentials.
def get_sheets_client(credential_file_path):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        credential_file_path, scope
    )
    client = gspread.authorize(credentials)

    return client, credentials
