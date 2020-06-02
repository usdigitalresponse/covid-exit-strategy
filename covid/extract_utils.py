import zipfile
from io import BytesIO


def unzip_string(string):
    # Unzip the response content.
    with zipfile.ZipFile(BytesIO(string)) as zip_file:
        zipfile_contents = {name: zip_file.read(name) for name in zip_file.namelist()}

    return zipfile_contents
