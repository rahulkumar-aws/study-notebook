import argparse
import pathlib
import sys
from typing import Dict, Any
import yaml


class ConfigReader:
    def __init__(self):
        # Unified argument parser for all inputs
        self.parser = argparse.ArgumentParser(description="Load config and options for ETL job.")
        self.parser.add_argument(
            "--env-config",
            type=str,
            required=False,
            help="Path to environment config YAML file"
        )
        self.parser.add_argument(
            "--data-config",
            type=str,
            required=False,
            help="Path to data config YAML file"
        )
        self.parser.add_argument(
            "--load-type",
            type=str,
            required=True,
            help="Type of load: e.g. Delta or Full"
        )
        self.parser.add_argument(
            "--workspace-base-path",
            type=str,
            required=True,
            help="Base path for the workspace"
        )

        # Parse args once and reuse
        self.args = self.parser.parse_args()

        # Extract file paths
        self.env_conf_file_path = self.args.env_config
        self.data_conf_file_path = self.args.data_config

    def _read_config(self, conf_file: str) -> Dict[str, Any]:
        """Safely read a YAML config file"""
        if conf_file:
            config_path = pathlib.Path(conf_file)
            if not config_path.exists():
                raise FileNotFoundError(f"Config file not found: {conf_file}")
            return yaml.safe_load(config_path.read_text())
        return {}

    def get_configs(self) -> Dict[str, Dict[str, Any]]:
        """Return parsed environment and data configs"""
        return {
            "env_config": self._read_config(self.env_conf_file_path),
            "data_config": self._read_config(self.data_conf_file_path)
        }

    def get_param_options(self) -> argparse.Namespace:
        """Return parsed command-line arguments"""
        print(f"*** Parsed Arguments: {self.args}")
        return self.args


# Optional usage block if used as a script
if __name__ == "__main__":
    reader = ConfigReader()
    configs = reader.get_configs()
    params = reader.get_param_options()

    print("✅ Load type:", params.load_type)
    print("✅ Workspace base path:", params.workspace_base_path)
    print("✅ Env config:", configs["env_config"])
    print("✅ Data config:", configs["data_config"])
