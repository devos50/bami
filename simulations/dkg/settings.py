from dataclasses import dataclass

from simulations.skipgraph.settings import SkipGraphSimulationSettings


@dataclass
class DKGSimulationSettings(SkipGraphSimulationSettings):
    replication_factor: int = 2
    offline_fraction: int = 0
    data_file_name: str = "torrents_1000.txt"
    fast_data_injection: bool = False  # Whether we sidestep the content injection
