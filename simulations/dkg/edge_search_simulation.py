import os
from asyncio import ensure_future
from multiprocessing.context import Process

from simulations.dkg.dkg_simulation import DKGSimulation
from simulations.settings import SimulationSettings


PEERS = [100, 200, 400, 800, 1600, 3200, 6400, 12800]


def combine_edge_search_latencies():
    with open(os.path.join("data", "edge_search_latencies.csv"), "w") as out_file:
        out_file.write("peers,time\n")
        for num_peers in PEERS:
            with open(os.path.join("data", "n_%d" % num_peers, "edge_search_latencies.csv")) as latencies_file:
                parsed_header = False
                for line in latencies_file.readlines():
                    if not parsed_header:
                        parsed_header = True
                        continue

                    out_file.write(line)


def run(settings):
    simulation = DKGSimulation(settings)
    simulation.MAIN_OVERLAY = "DKGCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()


if __name__ == "__main__":
    for num_peers in PEERS:
        print("Running experiment with %d peers..." % num_peers)
        settings = SimulationSettings()
        settings.peers = num_peers
        settings.duration = 600
        settings.logging_level = "ERROR"
        settings.profile = False
        settings.enable_community_statistics = True
        settings.enable_ipv8_ticker = False
        settings.latencies_file = "data/latencies.txt"

        p = Process(target=run, args=(settings,))
        p.start()
        p.join()

    combine_edge_search_latencies()