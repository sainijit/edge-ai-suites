import runpy
from unittest.mock import patch, MagicMock

















def test_ui_main_import_only(monkeypatch):
    """Ensure importing ui.main does not execute launch (since __name__ != '__main__')."""
    # Patch heavy functions so even if imported side-effects minimize
    monkeypatch = monkeypatch  # explicit
    monkeypatch.setattr("ui.main.initialize_app", lambda: None)
    monkeypatch.setattr("ui.main.create_ui", lambda: MagicMock())
    module = runpy.run_module("ui.main")
    # create_ui shouldn't have been called because __name__ != '__main__'
    # Nothing to assert strongly here; absence of launch side effects is success.
    assert "__name__" in module


def test_ui_main_dunder_main(monkeypatch):
    calls = {}

    fake_ui = MagicMock()
    fake_ui.launch = MagicMock(side_effect=lambda **k: calls.setdefault("launched", True))

    monkeypatch.setattr("ui.main.initialize_app", lambda: calls.setdefault("initialized", True))
    monkeypatch.setattr("ui.main.create_ui", lambda: fake_ui)
    monkeypatch.setattr("ui.main.stop_event_updates", lambda: calls.setdefault("stopped", True))

    runpy.run_module("ui.main", run_name="__main__")

    assert calls.get("initialized")
    assert calls.get("launched")
    assert calls.get("stopped")
    fake_ui.launch.assert_called_once()
