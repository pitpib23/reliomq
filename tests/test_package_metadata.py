from __future__ import annotations

import tomllib
import unittest
from pathlib import Path

import reliomq


class PackageMetadataTests(unittest.TestCase):
    def test_runtime_version_matches_project_metadata(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        with (project_root / "pyproject.toml").open("rb") as file:
            project = tomllib.load(file)["project"]

        self.assertEqual(reliomq.__version__, project["version"])


if __name__ == "__main__":
    unittest.main()
