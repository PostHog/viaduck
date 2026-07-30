import pytest

from viaduck import source


@pytest.fixture(autouse=True)
def _spill_dirs_under_tmp_path(tmp_path, monkeypatch):
    """Keep per-connection spill dirs out of the real /tmp.

    with_connection_defaults() mkdtemps a spill dir on EVERY call — many
    tests reach it indirectly (pool/registry/reconciler tests mock Catalog
    but run the real helper), and without this fixture each full run leaves
    ~40 real directories under /tmp/viaduck-spill on the dev machine.
    """
    defaults = dict(source._CONNECTION_DEFAULTS)
    defaults["temp_directory"] = str(tmp_path)
    monkeypatch.setattr(source, "_CONNECTION_DEFAULTS", defaults)
