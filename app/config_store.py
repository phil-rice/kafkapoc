"""
In-memory configuration store for stream-on-demand architecture
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class GenerationConfig:
    """Configuration for event generation"""

    parcels: int
    seed: int
    start_date: datetime
    end_date: datetime
    postcodes: List[str]
    site_ids: List[str]
    func_ids: List[str]

    # Stats
    total_events: int = 0
    generated_at: Optional[datetime] = None


class ConfigStore:
    """Thread-safe in-memory configuration store"""

    def __init__(self):
        self._config: Optional[GenerationConfig] = None

    def set_config(self, config: GenerationConfig):
        """Store generation configuration"""
        self._config = config

    def get_config(self) -> Optional[GenerationConfig]:
        """Retrieve generation configuration"""
        return self._config

    def has_config(self) -> bool:
        """Check if configuration exists"""
        return self._config is not None

    def clear(self):
        """Clear stored configuration"""
        self._config = None


# Global singleton instance
_config_store = ConfigStore()


def get_config_store() -> ConfigStore:
    """Get the global configuration store instance"""
    return _config_store
