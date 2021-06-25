import logging
import os
import shutil
from asyncio import ensure_future, get_event_loop, set_event_loop, sleep

import networkx as nx
from networkx.drawing.nx_agraph import write_dot

from bami.plexus.backbone.block import PlexusBlock
from bami.plexus.backbone.settings import BamiSettings
from ipv8.configuration import ConfigBuilder
from ipv8.messaging.serialization import default_serializer
from ipv8_service import IPv8
from simulation.discrete_loop import DiscreteLoop
from simulation.simulation_endpoint import SimulationEndpoint
from simulations.community import KeyValueCommunity


NUM_PEERS = 50
EXP_DURATION = 30


async def start_communities():
    instances = []
    for i in range(NUM_PEERS):
        builder = ConfigBuilder().clear_keys().clear_overlays()
        builder.add_key("my peer", "curve25519", f"ec{i}.pem")
        settings = BamiSettings()
        settings.work_directory = "data/.peer%d" % i
        if os.path.exists(settings.work_directory):
            shutil.rmtree(settings.work_directory)
        builder.add_overlay("KeyValueCommunity", "my peer", [], [], {'settings': settings}, [('on_community_started',)])

        endpoint = SimulationEndpoint()
        instance = IPv8(builder.finalize(), endpoint_override=endpoint,
                        extra_communities={'KeyValueCommunity': KeyValueCommunity})
        await instance.start()

        delay = (1.0 / NUM_PEERS) * i
        instance.overlays[0].register_task("periodically_create_tx_%d" % i, instance.overlays[0].write_random_value,
                                           interval=1.0, delay=delay)
        instances.append(instance)

    # Introduce peers to each other
    for from_instance in instances:
        for to_instance in instances:
            if from_instance == to_instance:
                continue
            from_instance.overlays[0].walk_to(to_instance.endpoint.wan_address)

    return instances


async def run_simulation():
    instances = await start_communities()

    # Make data directory
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.mkdir("data")

    cur_sec = 0
    with open("data/frontier_stats.csv", "w") as frontier_stats_file:
        frontier_stats_file.write("time,peer,terminal_size\n")
        while cur_sec <= EXP_DURATION:
            print("Simulated %d seconds..." % cur_sec)

            # Get and write frontier statistics
            for index, instance in enumerate(instances):
                chain = instance.overlays[0].persistence.get_chain(b"a" * 20)
                if chain:
                    frontier_stats_file.write("%d,%d,%d\n" % (cur_sec, index, len(chain.frontier.terminal)))

            cur_sec += 1
            await sleep(1)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    G = nx.DiGraph()
    for instance in instances:
        for key, block_blob in instance.overlays[0].persistence.block_store.iterate_blocks():
            block = PlexusBlock.unpack(block_blob, default_serializer)
            for link in block.community_links:
                G.add_edge(block.short_hash, link[1])
            for previous in block.previous:
                G.add_edge(block.short_hash, previous[1])

    write_dot(G, "graph.dot")

    get_event_loop().stop()

loop = DiscreteLoop()
set_event_loop(loop)

ensure_future(run_simulation())

loop.run_forever()
