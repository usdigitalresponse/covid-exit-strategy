import gspread
from df2gspread import df2gspread
from oauth2client.service_account import ServiceAccountCredentials


def post_covidtracking_data(df, workbook_key, tab_name, credentials):
    print(f"Beginning to upload data to workbook {workbook_key} and tab {tab_name}...")
    df2gspread.upload(
        df=df,
        gfile=workbook_key,
        wks_name=tab_name,
        credentials=credentials,
        # Do not include the index in the upload.
        row_names=False,
        col_names=True,
    )
    print("Finished uploading data.")


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
