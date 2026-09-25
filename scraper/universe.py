import pandas as pd
import logging
from typing import List, Set
from pathlib import Path

logger = logging.getLogger(__name__)

class UniverseManager:
    def __init__(self, universe_path: str = None):
        if universe_path is None:
            # Default to configs/idx_universe.csv relative to project root
            base_dir = Path(__file__).resolve().parent.parent
            self.universe_path = base_dir / "configs" / "idx_universe.csv"
        else:
            self.universe_path = Path(universe_path)
            
        self.fixed_symbols: Set[str] = set()
        self.dynamic_symbols: Set[str] = set()
        self.load_fixed_universe()

    def load_fixed_universe(self):
        try:
            df = pd.read_csv(self.universe_path)
            # Normalize column names
            df.columns = [c.strip().lower() for c in df.columns]
            
            if "symbol" in df.columns:
                self.fixed_symbols = set(df["symbol"].dropna().astype(str).str.strip().tolist())
                logger.info(f"Loaded {len(self.fixed_symbols)} fixed symbols from {self.universe_path}.")
            else:
                logger.warning(f"'symbol' column not found in {self.universe_path}")
        except Exception as e:
            logger.error(f"Failed to load universe from {self.universe_path}: {e}")

    def get_all_symbols(self) -> List[str]:
        return list(self.fixed_symbols.union(self.dynamic_symbols))

    def update_dynamic_universe(self, new_hot_stocks: List[str]):
        # check unusual volume/ara every 5-10 min
        self.dynamic_symbols = set(new_hot_stocks)
        logger.info(f"Updated dynamic universe with {len(self.dynamic_symbols)} symbols.")

    def run_dynamic_scanner(self):
        # placeholder for unusual volume/ara scanner api call
        pass
