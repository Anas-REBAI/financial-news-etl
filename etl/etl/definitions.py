from dagster import Definitions, load_assets_from_modules

# Importe les modules contenant les assets
from etl.assets import news, pdf_generation, prices, returns

# Charge tous les assets depuis les modules
all_assets = load_assets_from_modules([news, pdf_generation, prices, returns])

# Définitions pour Dagster
defs = Definitions(
    assets=all_assets,
)
