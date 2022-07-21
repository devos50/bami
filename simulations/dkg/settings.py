from dataclasses import dataclass

from simulations.skipgraph.settings import SkipGraphSimulationSettings


@dataclass
class DKGSimulationSettings(SkipGraphSimulationSettings):
    replication_factor: int = 2
    offline_fraction: int = 0
