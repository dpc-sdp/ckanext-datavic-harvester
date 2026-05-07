from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

import json
import os
import pytest

from ckan import model

import ckanext.harvest.model as harvest_model
from ckanext.harvest.harvesters import CKANHarvester

from ckanext.datavic_harvester.harvesters import DataVicODPHarvester


@pytest.fixture
def harvester():
    return DataVicODPHarvester()


@pytest.fixture
def odp_config():
    config_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "data",
        "harvest_config_for_odp.json",
    )
    with open(config_path) as f:
        return json.load(f)


@pytest.fixture
def odp_package_search_sample():
    sample_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "data",
        "datavic_odp_sample.json",
    )
    with open(sample_path) as f:
        return json.load(f)


@pytest.fixture
def odp_dataset(odp_package_search_sample):
    return odp_package_search_sample["result"]["results"][0]


@pytest.fixture
def odp_source_config(odp_config):
    config = dict(odp_config)
    config.pop("user", None)
    return config


class TestDataVicODPHarvester:
    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_import_applies_tsm_schema_defaults(
        self,
        harvester: DataVicODPHarvester,
        odp_config,
        odp_dataset,
    ):
        assert odp_config["user"] == "ckan_admin"
        assert odp_config["force_all"] is True
        assert odp_config["purge_missing"] is True
        assert odp_config["remote_groups"] == "create"
        assert odp_config["remote_orgs"] == "create"
        assert odp_config["tsm_schema"]["root"] == "Dataset"
        assert (
            odp_config["tsm_schema"]["types"]["Dataset"]["fields"]["private"]["value"]
            == "false"
        )
        assert (
            odp_config["tsm_schema"]["types"]["Dataset"]["fields"][
                "skip_syndication"
            ]["value"]
            == "true"
        )

        harvest_object = SimpleNamespace(
            content=json.dumps(odp_dataset),
            source=SimpleNamespace(config=json.dumps(odp_config)),
        )

        def fake_transmute_data(package_dict, schema):
            fields = schema["types"][schema["root"]]["fields"]
            for key, field in fields.items():
                package_dict[key] = field["value"]

        with mock.patch(
            "ckanext.datavic_harvester.harvesters.datavic_odp.CKANHarvester.import_stage",
            return_value=True,
        ) as imp:
            with mock.patch.object(
                harvester,
                "transmute_data",
                side_effect=fake_transmute_data,
            ) as transmute:
                result = harvester.import_stage(harvest_object)

        assert result is True
        transmute.assert_called_once()
        mutated_dataset = json.loads(harvest_object.content)
        assert mutated_dataset["private"] == "false"
        assert mutated_dataset["skip_syndication"] == "true"
        assert mutated_dataset["workflow_status"] == "published"
        assert mutated_dataset["access"] == "yes"
        imp.assert_called_once_with(harvest_object)

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_purge_missing(
        self,
        harvester: DataVicODPHarvester,
        dataset_factory,
        harvest_source_factory,
        harvest_job_factory,
        odp_config,
        odp_dataset,
        odp_source_config,
    ):
        assert odp_config["purge_missing"] is True

        dataset = dataset_factory()
        source = harvest_source_factory(
            config=json.dumps(odp_source_config),
            source_type="datavic_odp",
        )
        existing_job = harvest_job_factory(source=source)
        existing_object = harvest_model.HarvestObject(
            guid="missing-guid",
            job=existing_job,
            package_id=dataset["id"],
            current=True,
            content=json.dumps(odp_dataset),
        )
        existing_object.save()

        existing_job.status = "Finished"
        existing_job.gather_started = datetime.now(timezone.utc)
        existing_job.gather_finished = datetime.now(timezone.utc)
        existing_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        new_job = harvest_model.HarvestJob(source=source)
        model.Session.add(new_job)
        model.Session.commit()

        with mock.patch.object(CKANHarvester, "gather_stage", return_value=[999]):
            object_ids = harvester.gather_stage(new_job)

        delete_objects = (
            model.Session.query(harvest_model.HarvestObject)
            .filter(harvest_model.HarvestObject.harvest_job_id == new_job.id)
            .filter(harvest_model.HarvestObject.package_id == dataset["id"])
            .all()
        )

        assert len(object_ids) == 2
        assert len(delete_objects) == 1
        assert json.loads(delete_objects[0].content) == {
            "status": "delete",
            "package_id": dataset["id"],
            "guid": "missing-guid",
        }
        model.Session.refresh(existing_object)
        assert existing_object.current is False

        result = harvester.import_stage(delete_objects[0])
        assert result is True

        deleted_package = model.Package.get(dataset["id"])
        assert deleted_package
        assert deleted_package.state == "deleted"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_purge_missing_survives_unchanged_runs(
        self,
        harvester: DataVicODPHarvester,
        dataset_factory,
        harvest_source_factory,
        harvest_job_factory,
        odp_dataset,
        odp_source_config,
    ):
        # "Not modified" runs leave current=False, package_id=NULL rows;
        # a naive ORDER BY gathered.desc would miss the original mapping.
        dataset = dataset_factory()
        source = harvest_source_factory(
            config=json.dumps(odp_source_config),
            source_type="datavic_odp",
        )

        first_job = harvest_job_factory(source=source)
        first_object = harvest_model.HarvestObject(
            guid="missing-guid",
            job=first_job,
            package_id=dataset["id"],
            current=True,
            content=json.dumps(odp_dataset),
            gathered=datetime.now(timezone.utc) - timedelta(hours=2),
            report_status="added",
        )
        first_object.save()
        first_job.status = "Finished"
        first_job.gather_started = datetime.now(timezone.utc)
        first_job.gather_finished = datetime.now(timezone.utc)
        first_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        unchanged_job = harvest_job_factory(source=source)
        unchanged_object = harvest_model.HarvestObject(
            guid="missing-guid",
            job=unchanged_job,
            package_id=None,
            current=False,
            content=json.dumps(odp_dataset),
            gathered=datetime.now(timezone.utc) - timedelta(hours=1),
            report_status="not modified",
        )
        unchanged_object.save()
        unchanged_job.status = "Finished"
        unchanged_job.gather_started = datetime.now(timezone.utc)
        unchanged_job.gather_finished = datetime.now(timezone.utc)
        unchanged_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        new_job = harvest_model.HarvestJob(source=source)
        model.Session.add(new_job)
        model.Session.commit()

        with mock.patch.object(CKANHarvester, "gather_stage", return_value=[999]):
            object_ids = harvester.gather_stage(new_job)

        delete_objects = (
            model.Session.query(harvest_model.HarvestObject)
            .filter(harvest_model.HarvestObject.harvest_job_id == new_job.id)
            .filter(harvest_model.HarvestObject.package_id == dataset["id"])
            .all()
        )

        assert len(object_ids) == 2
        assert len(delete_objects) == 1
        assert json.loads(delete_objects[0].content) == {
            "status": "delete",
            "package_id": dataset["id"],
            "guid": "missing-guid",
        }

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_purge_missing_skips_already_deleted_packages(
        self,
        harvester: DataVicODPHarvester,
        dataset_factory,
        harvest_source_factory,
        harvest_job_factory,
        odp_dataset,
        odp_source_config,
    ):
        # Trashed packages must not be re-queued for delete.
        dataset = dataset_factory()
        source = harvest_source_factory(
            config=json.dumps(odp_source_config),
            source_type="datavic_odp",
        )
        existing_job = harvest_job_factory(source=source)
        harvest_model.HarvestObject(
            guid="already-deleted-guid",
            job=existing_job,
            package_id=dataset["id"],
            current=True,
            content=json.dumps(odp_dataset),
            report_status="added",
        ).save()
        existing_job.status = "Finished"
        existing_job.gather_started = datetime.now(timezone.utc)
        existing_job.gather_finished = datetime.now(timezone.utc)
        existing_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        deleted_package = model.Package.get(dataset["id"])
        assert deleted_package is not None
        deleted_package.state = "deleted"
        model.Session.commit()

        new_job = harvest_model.HarvestJob(source=source)
        model.Session.add(new_job)
        model.Session.commit()

        with mock.patch.object(CKANHarvester, "gather_stage", return_value=[999]):
            object_ids = harvester.gather_stage(new_job)

        delete_objects = (
            model.Session.query(harvest_model.HarvestObject)
            .filter(harvest_model.HarvestObject.harvest_job_id == new_job.id)
            .filter(harvest_model.HarvestObject.guid == "already-deleted-guid")
            .all()
        )

        assert object_ids == [999]
        assert delete_objects == []
