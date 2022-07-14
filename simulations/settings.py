from dataclasses import dataclass


@dataclass
class SimulationSettings:
    peers: int = 100  # Number of IPv8 peers
    duration: int = 120  # Simulation duration in sections
    profile: bool = False  # Whether to run the Yappi profiler
    logging_level: str = "INFO"  # Logging level
    enable_community_statistics: bool = False

    # The IPv8 ticker is responsible for community walking and discovering other peers, but can significantly limit
    # performance. Setting this option to False cancels the IPv8 ticker, improving performance.
    enable_ipv8_ticker: bool = True
