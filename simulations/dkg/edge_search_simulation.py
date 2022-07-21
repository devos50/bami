import os
from asyncio import ensure_future
from multiprocessing.context import Process

from simulations.dkg.dkg_simulation import DKGSimulation
from simulations.dkg.settings import DKGSimulationSettings

# PEERS = [1000]
# OFFLINE_FRACTIONS = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
# REPLICATION_FACTORS = [1, 2, 3, 4, 5]
# EXPERIMENT_REPLICATION = 5
PEERS = [100, 200, 400, 800, 1600]
OFFLINE_FRACTIONS = [0]
REPLICATION_FACTORS = [1]
EXPERIMENT_REPLICATION = 1


class EdgeSearchDKGSimulation(DKGSimulation):

    def on_simulation_finished(self):
        super().on_simulation_finished()

        # Write search statistics away
        with open(os.path.join("data", "edge_searches.csv"), "a") as out_file:
            out_file.write("%d,%d,%d,%d,%d\n" % (len(self.nodes), self.settings.offline_fraction,
                                                 self.settings.replication_factor, self.searches_done,
                                                 self.failed_searches))


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
    simulation = EdgeSearchDKGSimulation(settings)
    simulation.MAIN_OVERLAY = "DKGCommunity"

    ensure_future(simulation.run())

    simulation.loop.run_forever()


if __name__ == "__main__":
    with open(os.path.join("data", "edge_searches.csv"), "w") as out_file:
        out_file.write("peers,offline_fraction,replication_factor,total_searches,failed_searches\n")

    for num_peers in PEERS:
        for offline_fraction in OFFLINE_FRACTIONS:
            for replication_factor in REPLICATION_FACTORS:

                processes = []
                for exp_num in range(EXPERIMENT_REPLICATION):
                    print("Running experiment with %d peers..." % num_peers)
                    settings = DKGSimulationSettings()
                    settings.peers = num_peers
                    settings.offline_fraction = offline_fraction
                    settings.replication_factor = replication_factor
                    settings.duration = 3600
                    settings.identifier = exp_num
                    settings.logging_level = "ERROR"
                    settings.profile = False
                    settings.enable_community_statistics = True
                    settings.enable_ipv8_ticker = False
                    settings.cache_intermediate_search_results = True
                    settings.latencies_file = "data/latencies.txt"

                    p = Process(target=run, args=(settings,))
                    p.start()
                    processes.append(p)

                for p in processes:
                    p.join()

    combine_edge_search_latencies()
