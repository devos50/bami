from dataclasses import dataclass

from simulations.settings import SimulationSettings


@dataclass
class SkipGraphSimulationSettings(SimulationSettings):
    cache_intermediate_search_results: bool = True
    num_searches: int = 1000
