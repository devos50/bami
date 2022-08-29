from dataclasses import dataclass

from simulations.settings import SimulationSettings


@dataclass
class SkipGraphSimulationSettings(SimulationSettings):
    num_searches: int = 1000
    nb_size: int = 1
    track_failing_nodes_in_rts: bool = False
    assign_sequential_sg_keys: bool = False
