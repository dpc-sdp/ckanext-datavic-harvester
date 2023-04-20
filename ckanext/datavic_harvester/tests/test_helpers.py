from bs4 import BeautifulSoup

import ckanext.datavic_harvester.helpers as h


class TestHelpers:
    def test_get_update_frequencies(self):
        result = h.get_datavic_update_frequencies()

        assert result
        assert isinstance(result, list)
        assert isinstance(result[0], dict)
        assert result[0]["label"]
        assert result[0]["value"]

    def test_map_update_frequency(self):
        """We're not going to test actual values, because they could change
        Instead, check if random value doesn't break anything"""
        assert h.map_update_frequency("random") == "unknown"

    def test_get_tags(self):
        assert h.get_tags("test1;test2") == [{"name": "test1"}, {"name": "test2"}]
        assert h.get_tags("test1,test2") == [{"name": "test1"}, {"name": "test2"}]
        assert h.get_tags("test1&test2") == [{"name": "test1&test2"}]

    def test_get_from_to(self):
        assert h.get_from_to(1, 500) == (1, 500)
        assert h.get_from_to(2, 500) == (501, 1000)

    def test_convert_date_to_isoformat(self):
        """We are using key and dataset name params only for logging."""
        assert not h.convert_date_to_isoformat(None, "", "")
        assert h.convert_date_to_isoformat(
            "2019-10-08T02:12:36.428Z", "", "", strip_tz=False
        )
        assert h.convert_date_to_isoformat("2019-12-30T13:00:00+00:00", "", "")

    def test_extract_metadata_url(self, dcat_description: str):
        soup = BeautifulSoup(dcat_description, "html.parser")
        assert h.extract_metadata_url(soup, "https://localhost/metadata")
        assert not h.extract_metadata_url(soup, "https://127.0.0.1/metadata")

    def test_remote_attrs(self, dcat_description: str):
        soup = BeautifulSoup(dcat_description)
        assert "style" in soup.find("span").attrs # type: ignore

        result: BeautifulSoup = h.remove_all_attrs_except_for(soup)

        assert "style" not in soup.find("span").attrs # type: ignore
        assert "href" in result.find("a").attrs  # type: ignore

    def test_unwrap(self, dcat_description: str):
        soup = BeautifulSoup(dcat_description)
        assert soup.find("span")

        result: str = h.unwrap_all_except(h.remove_all_attrs_except_for(soup))

        assert "span" not in result
        assert "<a" in result
