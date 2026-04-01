from __future__ import annotations

from typing import Any

import json
import pytest
from unittest import mock

from ckanext.datavic_harvester.harvesters import DelwpHarvester as Base
from ckanext.datavic_harvester.harvesters.base import DataVicBaseHarvester


@pytest.fixture
def harvester():
    return Base(test=True)


class TestDelwpConfig:
    @mock.patch.object(DataVicBaseHarvester, "validate_config")
    def test_validate(self, mock_validate, harvester: Base):
        """Testing config validation for delwp harvester, skipping super class
        validation."""
        config: dict[str, Any] = {}
        mock_validate.side_effect = lambda x: x

        with pytest.raises(ValueError, match="full_metadata_url_prefix must be set"):  # type: ignore
            harvester.validate_config(json.dumps(config))

        config["full_metadata_url_prefix"] = "test"
        with pytest.raises(
            ValueError, match="full_metadata_url_prefix must have the \{UUID\} identifier in the URL"  # type: ignore
        ):
            harvester.validate_config(json.dumps(config))

        config["full_metadata_url_prefix"] = "test{UUID}"
        with pytest.raises(
            ValueError, match="resource_url_prefix must be set"  # type: ignore
        ):
            harvester.validate_config(json.dumps(config))

        config["resource_url_prefix"] = "test"
        with pytest.raises(ValueError, match="license_id must be set"):  # type: ignore
            harvester.validate_config(json.dumps(config))

        config["license_id"] = "test"
        with pytest.raises(
            ValueError, match="resource_attribution must be set"  # type: ignore
        ):
            harvester.validate_config(json.dumps(config))

        config["resource_attribution"] = "test"
        with pytest.raises(
            ValueError, match="dataset_type must be set"  # type: ignore
        ):
            harvester.validate_config(json.dumps(config))

        config["dataset_type"] = "test"
        with pytest.raises(ValueError, match="api_auth must be set"):  # type: ignore
            harvester.validate_config(json.dumps(config))

        config["api_auth"] = "test"

        harvester.validate_config(json.dumps(config))

    def test_validate_organisation_mapping(self, harvester: Base):
        config: dict[str, Any] = {"organisation_mapping": {}}
        with pytest.raises(ValueError, match="organisation_mapping must be a \*list\* of organisations"):  # type: ignore
            harvester._validate_organisation_mapping(config)

        config["organisation_mapping"] = [()]
        with pytest.raises(ValueError, match="organisation_mapping item must be a \*dict\*"):  # type: ignore
            harvester._validate_organisation_mapping(config)

        config["organisation_mapping"] = [{"resowner": "test-title"}]
        with pytest.raises(ValueError, match='organisation_mapping item must have property "org-name"'):  # type: ignore
            harvester._validate_organisation_mapping(config)

        config["organisation_mapping"] = [{"org-name": "test-name"}]
        with pytest.raises(ValueError, match='organisation_mapping item must have property "resowner"'):  # type: ignore
            harvester._validate_organisation_mapping(config)

        config["organisation_mapping"] = [{"org-name": "test", "resowner": "test"}]
        with pytest.raises(ValueError, match="Organisation test not found"):  # type: ignore
            harvester._validate_organisation_mapping(config)

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_validate_return_dumped_json(self, harvester: Base, delwp_config):
        config: str = harvester.validate_config(json.dumps(delwp_config))

        assert config
        assert isinstance(config, str)

    def test_validate_deletion_safeguard_optional_config(self, harvester: Base):
        config: dict[str, Any] = {
            "deletion_safeguard_enabled": "yes",
        }
        harvester._validate_optional_deletion_safeguard_config(config)
        assert config["deletion_safeguard_enabled"] is True

        config = {
            "deletion_safeguard_allow_bulk_delete": "no",
        }
        harvester._validate_optional_deletion_safeguard_config(config)
        assert config["deletion_safeguard_allow_bulk_delete"] is False

        with pytest.raises(
            ValueError, match="deletion_safeguard_enabled must be a boolean"
        ):
            harvester._validate_optional_deletion_safeguard_config(
                {"deletion_safeguard_enabled": "maybe"}
            )

        config = {
            "deletion_safeguard_drop_threshold_percent": "invalid",
        }
        with pytest.raises(
            ValueError,
            match="deletion_safeguard_drop_threshold_percent must be a number",
        ):
            harvester._validate_optional_deletion_safeguard_config(config)

        config = {
            "deletion_safeguard_drop_threshold_percent": 120,
        }
        with pytest.raises(
            ValueError,
            match="deletion_safeguard_drop_threshold_percent must be >= 0 and <= 100",
        ):
            harvester._validate_optional_deletion_safeguard_config(config)

        config = {
            "deletion_safeguard_min_previous_count": -1,
        }
        with pytest.raises(
            ValueError,
            match="deletion_safeguard_min_previous_count must be >= 0",
        ):
            harvester._validate_optional_deletion_safeguard_config(config)

        config = {
            "deletion_safeguard_notify_ok_url": 123,
        }
        with pytest.raises(
            ValueError, match="deletion_safeguard_notify_ok_url must be a string"
        ):
            harvester._validate_optional_deletion_safeguard_config(config)

        config = {
            "deletion_safeguard_notify_anomaly_url": 123,
        }
        with pytest.raises(
            ValueError, match="deletion_safeguard_notify_anomaly_url must be a string"
        ):
            harvester._validate_optional_deletion_safeguard_config(config)

        # Non-string, non-bool types for boolean fields should raise
        with pytest.raises(
            ValueError, match="deletion_safeguard_enabled must be a boolean"
        ):
            harvester._validate_optional_deletion_safeguard_config(
                {"deletion_safeguard_enabled": 1}
            )

        with pytest.raises(
            ValueError, match="deletion_safeguard_allow_bulk_delete must be a boolean"
        ):
            harvester._validate_optional_deletion_safeguard_config(
                {"deletion_safeguard_allow_bulk_delete": 0}
            )

        # Float string is not a valid integer for min_previous_count
        with pytest.raises(
            ValueError, match="deletion_safeguard_min_previous_count must be an integer"
        ):
            harvester._validate_optional_deletion_safeguard_config(
                {"deletion_safeguard_min_previous_count": "1.5"}
            )

    def test_validate_deletion_safeguard_boundary_values(self, harvester: Base):
        """Threshold at 0 and 100 are both valid boundary values."""
        harvester._validate_optional_deletion_safeguard_config(
            {"deletion_safeguard_drop_threshold_percent": 0}
        )
        harvester._validate_optional_deletion_safeguard_config(
            {"deletion_safeguard_drop_threshold_percent": 100}
        )

    def test_validate_deletion_safeguard_none_url_is_valid(self, harvester: Base):
        """A None URL value is explicitly permitted (means no notification)."""
        harvester._validate_optional_deletion_safeguard_config(
            {
                "deletion_safeguard_notify_ok_url": None,
                "deletion_safeguard_notify_anomaly_url": None,
            }
        )
