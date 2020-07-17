from python_project.backbone.settings import PlexusSettings


class PaymentSettings(PlexusSettings):
    def __init__(self):
        super().__init__()
        self.asset_precision = 10
        # Mint settings
        self.mint_value_range = (0, 100)
        self.mint_max_value = 10 ** 7
        # Spend settings
        self.spend_value_range = (0, 10 ** 7)