import unittest

from datetime import datetime
from gw2tpdb.api.datawars import build_request_url, Endpoint

class BuildRequestUrlTest(unittest.TestCase):
    def test_historic_daily(self):
        self.assertEqual(build_request_url(Endpoint.HISTORIC_DAILY, 123),
        "https://api.datawars2.ie/gw2/v2/historic/json?itemID=123")

    def test_historic_hourly(self):
        self.assertEqual(build_request_url(Endpoint.HISTORIC_HOURLY, 456),
        "https://api.datawars2.ie/gw2/v2/historic/hourly/json?itemID=456")

    def test_with_start(self):
        self.assertEqual(build_request_url(Endpoint.HISTORIC_DAILY, 123, start=datetime(2023, 4, 10)),
        "https://api.datawars2.ie/gw2/v2/historic/json?itemID=123&start=2023-04-10T00%3A00%3A00Z")

    def test_with_end(self):
        self.assertEqual(build_request_url(Endpoint.HISTORIC_DAILY, 123, end=datetime(2023, 4, 10)),
        "https://api.datawars2.ie/gw2/v2/historic/json?itemID=123&end=2023-04-10T00%3A00%3A00Z")

    def test_with_start_and_end(self):
        self.assertEqual(build_request_url(Endpoint.HISTORIC_DAILY, 123, start=datetime(2023, 4, 1), end=datetime(2023, 4, 2)),
                         "https://api.datawars2.ie/gw2/v2/historic/json?itemID=123&start=2023-04-01T00%3A00%3A00Z&end=2023-04-02T00%3A00%3A00Z")

if __name__ == "__main__":
    unittest.main ()
