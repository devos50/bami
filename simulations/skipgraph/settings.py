from dataclasses import dataclass

from simulations.settings import SimulationSettings


@dataclass
class SkipGraphSimulationSettings(SimulationSettings):
    num_searches: int = 1000
    nb_size: int = 1
    assign_sequential_sg_keys: bool = False
    offline_fraction: int = 0
