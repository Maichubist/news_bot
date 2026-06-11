from __future__ import annotations

import pytest

from app.config import _load_moderation_cfg


def test_defaults_when_section_absent():
    cfg = _load_moderation_cfg({})
    assert cfg.enabled is False  # old configs keep old behaviour
    assert cfg.timeout_minutes == 45
    assert cfg.on_timeout == "skip"


def test_explicit_values():
    cfg = _load_moderation_cfg({"enabled": True, "timeout_minutes": 10, "on_timeout": "publish"})
    assert cfg.enabled is True
    assert cfg.timeout_minutes == 10
    assert cfg.on_timeout == "publish"


def test_invalid_on_timeout_rejected():
    with pytest.raises(ValueError):
        _load_moderation_cfg({"on_timeout": "explode"})


def test_ranking_defaults_when_section_absent():
    from app.config import _load_ranking_cfg

    cfg = _load_ranking_cfg({})
    assert cfg.enabled is False  # legacy threshold path stays the default
    assert cfg.cycle_minutes == 25
    assert cfg.window_hours == 3
    assert cfg.model == "gpt-4o"
    assert cfg.max_picks == 2
    assert cfg.max_age_hours == 8
