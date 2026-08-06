import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COLLECTOR_PATH = ROOT / "collect_airflow_workflow_metadata_vasilis_claud_working.py"
SPEC = importlib.util.spec_from_file_location("airflow_metadata_collector", COLLECTOR_PATH)
COLLECTOR = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(COLLECTOR)


class InputMetadataTests(unittest.TestCase):
    def test_repository_configuration_loads(self):
        metadata = COLLECTOR.load_input_metadata(str(ROOT / "input_datasets.json"))

        self.assertIn("workflow_dataset", metadata)
        self.assertIn("run_dataset", metadata)
        self.assertEqual(
            metadata["run_dataset"]["is_part_of"],
            metadata["workflow_dataset"]["uri"],
        )

    def test_invalid_configuration_is_rejected(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json") as handle:
            json.dump({"run_dataset": {"label": "Missing URI"}}, handle)
            handle.flush()

            with self.assertRaisesRegex(RuntimeError, "missing required field"):
                COLLECTOR.load_input_metadata(handle.name)

    def test_rendered_dataset_contains_forward_and_source_links(self):
        dataset = COLLECTOR.load_input_metadata(
            str(ROOT / "input_datasets.json")
        )["run_dataset"]
        run_uri = "<http://example.org/run/example>"
        rendered = "\n".join(
            COLLECTOR.render_input_dataset(dataset, "rm:usedByWorkflowRun", run_uri)
        )

        self.assertIn("rdf:type rm:InputDataset", rendered)
        self.assertIn("rm:upstreamSourceURL", rendered)
        self.assertIn(f"rm:usedByWorkflowRun {run_uri}", rendered)
        self.assertTrue(rendered.rstrip().endswith("."))


if __name__ == "__main__":
    unittest.main()
