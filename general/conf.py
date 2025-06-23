import argparse
import pathlib
import sys
from argparse import ArgumentParser
from typing import Dict, Any

import yaml


class ConfigReader:

    def __init__(self):
        self.parser = None
        self.data_conf_file_path = None
        self.env_conf_file_path = None

    def _get_conf_file(self):
        self.param = ArgumentParser()
        self.param.add_argument("--env-config", required=False, type=str)
        self.param.add_argument("--data-config", required=False, type=str)
        namespace = self.param.parse_known_args(sys.argv[1:])[0]
        return namespace.env_config, namespace.data_config

    def _read_config(self, conf_file) -> Dict[str, Any]:
        if conf_file is not None:
            return yaml.safe_load(pathlib.Path(conf_file).read_text())
        return {}

    def get_configs(self):
        configs = {}
        self.env_conf_file_path, self.data_conf_file_path = self._get_conf_file()
        configs["env_config"] = self._read_config(self.env_conf_file_path)
        configs["data_config"] = self._read_config(self.data_conf_file_path)
        return configs

    def get_param_options(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--load-type", type=str, help='Type of load e.g. Delta/Full')
        self.parser.add_argument("--workspace-base-path", type=str, help='Base path for the workspace')
        opt = self.parser.parse_args()
        print(f"*** {opt}")
        return opt
