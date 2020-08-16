from enum import IntEnum

PATH_TO_SERVICE_ACCOUNT_KEY = "service-account-key.json"


# Define the frontend colors.
class Color(IntEnum):
    GREEN = 0
    YELLOW = 1
    RED = 2
    DARK_RED = 3


COLOR_NAME_MAP = {
    Color.GREEN: "Green",
    Color.YELLOW: "Yellow",
    Color.RED: "Red",
    Color.DARK_RED: "Dark Red",
}
