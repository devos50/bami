import json
from binascii import unhexlify
from typing import Set

from bami.dkg.content import Content
from bami.dkg.db.triplet import Triplet
from bami.dkg.rule_execution_engine import RuleExecutionEngine
from bami.dkg.rules.rule import Rule


class EthereumBlockRule(Rule):
    RULE_NAME = "EthereumBlock"

    def apply_rule(self, engine: RuleExecutionEngine, content: Content) -> Set[Triplet]:
        triplets = set()
        block_json = json.loads(content.data)
        if "miner" not in block_json:
            return set()  # This doesn't seem to be a block

        for key, value in block_json.items():  # Parse the main block attributes
            if key == "hash" or key == "transactions":
                continue
            if isinstance(value, list):
                continue

            triplets.add(Triplet(content.identifier, Rule.convert_to_bytes(key), Rule.convert_to_bytes(value)))

        # We now parse all the transactions and add them as new content items to the rule execution engine
        for transaction in block_json["transactions"]:
            tx_hash = unhexlify(transaction["hash"][2:])
            content = Content(tx_hash, json.dumps(transaction).encode())
            engine.process_queue.append(content)

        return triplets


class EthereumTransactionRule(Rule):
    RULE_NAME = "EthereumTransaction"

    def apply_rule(self, engine: RuleExecutionEngine, content: Content) -> Set[Triplet]:
        triplets = set()
        tx_json = json.loads(content.data)
        if "from" not in tx_json:
            return set()  # This doesn't seem to be a transaction

        for key, value in tx_json.items():  # Parse the main tx attributes
            if key == "hash" or key == "accessList" or key == "input":
                continue

            if key == "to" and value is None:
                continue

            triplets.add(Triplet(content.identifier, Rule.convert_to_bytes(key), value.encode()))

        return triplets
