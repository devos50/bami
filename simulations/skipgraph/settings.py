from dataclasses import dataclass


@dataclass
class SkipGraphSimulationSettings:
    cache_intermediate_search_results: bool = True
