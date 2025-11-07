from dataclasses import dataclass
from typing import *

from pyflink.table import TableEnvironment, EnvironmentSettings, StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration


@dataclass
class FlinkService:
    """
        jars_path: full path to jar files
        settings: a dicts of flink's settings
    """
    jars_path: List[str]
    settings: dict

    def __post_init__(self):
        config = Configuration()
        for k, v in self.settings.items():
            config.set_string(k, v)
        env_settings = EnvironmentSettings.in_streaming_mode()
        self.env =StreamExecutionEnvironment.get_execution_environment()
        self.env.configure(config)

        self.t_env = StreamTableEnvironment.create(environment_settings=env_settings)
        # self.t_env.get_config().add_configuration(config)