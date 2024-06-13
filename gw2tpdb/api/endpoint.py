from enum import StrEnum

datawars_v1_base_url = "https://api.datawars2.ie/gw2/v1"
datawars_v2_base_url = "https://api.datawars2.ie/gw2/v2"

class Endpoint(StrEnum):
    HISTORY_DAILY_JSON = f"{datawars_v2_base_url}/history/json"
    HISTORY_HOURLY_JSON = f"{datawars_v2_base_url}/history/hourly/json"
    ITEMS_JSON = f"{datawars_v1_base_url}/items/json"
