from importlib import metadata, util

import pytest


@pytest.mark.parametrize(
    ("distribution_name", "namespace", "former_distribution", "former_namespace"),
    [
        ("mslb-api", "mslb_api", "lb-api", "lb_api"),
        ("mslb-application", "mslb_application", "lb-application", "lb_application"),
        ("mslb-contracts", "mslb_contracts", "lb-contracts", "lb_contracts"),
        ("mslb-controller", "mslb_controller", "lb-controller", "lb_controller"),
        ("mslb-engine", "mslb_engine", "lb-engine", "lb_engine"),
        ("mslb-mock-server", "mslb_mock_server", "lb-mock-server", "lb_mock_server"),
        ("mslb-persistence", "mslb_persistence", "lb-persistence", "lb_persistence"),
        ("mslb-worker", "mslb_worker", "lb-worker", "lb_worker"),
    ],
)
def test_only_mslb_workspace_distributions_and_namespaces_are_installed(
    distribution_name: str,
    namespace: str,
    former_distribution: str,
    former_namespace: str,
) -> None:
    installed = metadata.distribution(distribution_name=distribution_name)
    assert installed.metadata["Name"] == distribution_name
    assert util.find_spec(name=namespace) is not None
    assert util.find_spec(name=former_namespace) is None
    with pytest.raises(expected_exception=metadata.PackageNotFoundError):
        metadata.distribution(distribution_name=former_distribution)
