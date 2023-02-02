from ckanext.datavic_harvester.harvesters import DataVicODSHarvester


class TestODSHarvester(object):
    def test_description_refine_markdown(self):
        harvester = DataVicODSHarvester()

        assert not harvester._description_refine(None)
        assert not harvester._description_refine("")
        assert (
            harvester._description_refine(
                "'<b>Yay</b> <a href=\"http://github.com\">GitHub</a>'"
            )
            == "'**Yay** [GitHub](http://github.com)'"
        )
