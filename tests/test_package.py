from __future__ import annotations

import importlib.metadata

import vstack as m


def test_version():
    assert importlib.metadata.version("vstack") == m.__version__
