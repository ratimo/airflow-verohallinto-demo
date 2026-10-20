import unittest
from unittest.mock import patch

from tax_values.source import discover_latest_passenger_workbook, download_latest_workbook


class SourceDiscoveryTests(unittest.TestCase):
    def test_selects_latest_passenger_workbook_and_excludes_other_vehicle_classes(self):
        html = """
        <a href="/contentassets/x/henkiloautojen-sovelletut-verotusarvot-2025.xls">vuosi 2025 henkilöauto (xls)</a>
        <a href="/contentassets/x/moottoripyorien-sovelletut-verotusarvot-2026.xls">vuosi 2026 moottoripyörät (xls)</a>
        <a href="/contentassets/x/henkiloautojen-sovelletut-verotusarvot-2026.xls">vuosi 2026 henkilöauto (xls)</a>
        """

        latest = discover_latest_passenger_workbook(html)

        self.assertEqual(latest.decision_year, 2026)
        self.assertEqual(
            latest.url,
            "https://www.vero.fi/contentassets/x/henkiloautojen-sovelletut-verotusarvot-2026.xls",
        )
        self.assertIn("henkilöauto", latest.label)

    def test_raises_when_no_passenger_xls_link_is_present(self):
        with self.assertRaisesRegex(ValueError, "No passenger-car XLS"):
            discover_latest_passenger_workbook('<a href="/motorcycles-2026.xls">2026 moottoripyörät</a>')

    def test_downloads_only_the_discovered_workbook_and_records_metadata(self):
        page = '<a href="/contentassets/x/henkiloauto-2026.xls">2026 henkilöauto</a>'.encode("utf-8")

        class FakeResponse:
            def __init__(self, content, headers):
                self.content = content
                self.headers = headers

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self, limit=-1):
                return self.content if limit < 0 else self.content[:limit]

            def geturl(self):
                return "https://www.vero.fi/"

        responses = [
            FakeResponse(page, {"Content-Type": "text/html"}),
            FakeResponse(b"workbook-bytes", {"ETag": '"abc"', "Last-Modified": "Tue, 06 Oct 2026 11:34:52 GMT"}),
        ]
        with patch("tax_values.source.urllib.request.urlopen", side_effect=responses) as open_url:
            fetched = download_latest_workbook()

        self.assertEqual(fetched.link.decision_year, 2026)
        self.assertEqual(fetched.content, b"workbook-bytes")
        self.assertEqual(fetched.sha256, "977ae00c34126a1c073815f483d1869ee09894533641771199f8803ff0967cc1")
        self.assertEqual(fetched.etag, '"abc"')
        self.assertEqual(open_url.call_count, 2)

    def test_rejects_redirect_outside_official_host(self):
        page = '<a href="/contentassets/x/henkiloauto-2026.xls">2026 henkilöauto</a>'.encode("utf-8")

        class FakeResponse:
            def __init__(self, content, headers, url="https://www.vero.fi/"):
                self.content = content
                self.headers = headers
                self.url = url

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self, limit=-1):
                return self.content if limit < 0 else self.content[:limit]

            def geturl(self):
                return self.url

        responses = [
            FakeResponse(page, {}),
            FakeResponse(b"not-vero", {}, "https://example.com/redirect.xls"),
        ]
        with patch("tax_values.source.urllib.request.urlopen", side_effect=responses):
            with self.assertRaisesRegex(ValueError, "outside the official"):
                download_latest_workbook()


if __name__ == "__main__":
    unittest.main()
