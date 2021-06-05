import logging
import os
import shutil
from asyncio import ensure_future, get_event_loop, set_event_loop, sleep

import networkx as nx
from networkx.drawing.nx_agraph import write_dot

from bami.backbone.block import BamiBlock
from bami.backbone.settings import BamiSettings
from ipv8.configuration import ConfigBuilder
from ipv8.messaging.serialization import default_serializer
from ipv8_service import IPv8
from simulation.discrete_loop import DiscreteLoop
from simulation.simulation_endpoint import SimulationEndpoint
from simulations.community import TestCommunity


NUM_PEERS = 2


async def start_communities():
    instances = []
    for i in range(NUM_PEERS):
        builder = ConfigBuilder().clear_keys().clear_overlays()
        builder.add_key("my peer", "curve25519", f"ec{i}.pem")
        settings = BamiSettings()
        settings.work_directory = ".peer%d" % i
        if os.path.exists(settings.work_directory):
            shutil.rmtree(settings.work_directory)
        builder.add_overlay("TestCommunity", "my peer", [], [], {'settings': settings}, [('on_community_started',)])

        endpoint = SimulationEndpoint()
        instance = IPv8(builder.finalize(), endpoint_override=endpoint,
                        extra_communities={'TestCommunity': TestCommunity})
        await instance.start()

        delay = 0 # (1.0 / NUM_PEERS) * i
        instance.overlays[0].register_task("periodically_create_tx_%d" % i, instance.overlays[0].create_new_tx,
                                           interval=2.0, delay=delay)
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
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    await sleep(10)

    G = nx.DiGraph()
    for instance in instances:
        for key, block_blob in instance.overlays[0].persistence.block_store.iterate_blocks():
            block = BamiBlock.unpack(block_blob, default_serializer)
            for link in block.links:
                G.add_edge(block.short_hash, link[1])
            for previous in block.previous:
                G.add_edge(block.short_hash, previous[1])

    write_dot(G, "graph.dot")

    get_event_loop().stop()

loop = DiscreteLoop()
set_event_loop(loop)

ensure_future(run_simulation())

loop.run_forever()
