from datetime import datetime
import unittest
from urllib.parse import urlencode

datawars_base_url = "https://api.datawars2.ie/gw2/v2/"

class BuildRequestUrlTest(unittest.TestCase):
    def test_historic_daily(self):
        self.assertEqual(build_request_url(Endpoint.HISTORIC_DAILY, 123),
        f"{datawars_base_url}historic/json?itemID=123")

    def test_historic_hourly(self):
        self.assertEqual(build_request_url(Endpoint.HISTORIC_HOURLY, 456),
        f"{datawars_base_url}historic/hourly/json?itemID=456")

    def test_with_start(self):
        self.assertEqual(build_request_url(Endpoint.HISTORIC_DAILY, item_id123, start=datetime(2023, 04, 10)),
        f"{datawars_base_url}historic/json?itemID=123&start=2023-04-1T00:00:00Z")

    def test_with_end(self):
        self.assertEqual(build_request_url(Endpoint.HISTORIC_DAILY, 123, end=datetime(2023, 04, 10)),
        f"{datawars_base_url}historic/json?itemID=123&end=2023-04-10T00:00:00Z")

    def test_with_start_and_end(self):
        start = datetime(2023, 4, 1)
        end = datetime(2023, 4, 10)
        params = {
            "itemID": 123,
            "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),  # Note: Potential bug, should be using 'end'
        }
        expected = f"{datawars_base_url}historic/json?{urlencode(params)}"
        self.assertEqual(build_request_url(Endpoint.HISTORIC_DAILY, 123, start=start, end=end), expected)
