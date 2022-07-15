from asyncio import ensure_future

from simulations.dkg.dkg_simulation import DKGSimulation
from simulations.settings import SimulationSettings

if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = 100
    settings.duration = 600
    settings.logging_level = "ERROR"
    settings.profile = False
    settings.enable_community_statistics = True
    settings.enable_ipv8_ticker = False
    settings.latencies_file = "data/latencies.txt"
    simulation = DKGSimulation(settings)
    simulation.MAIN_OVERLAY = "DKGCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()
