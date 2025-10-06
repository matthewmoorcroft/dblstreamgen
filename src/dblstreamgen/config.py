"""Configuration management for dblstreamgen.

Load and manage YAML-based configuration files for event generation and publishing.
"""

import yaml
from typing import Any, Optional, Union
from pathlib import Path


class Config:
    """Configuration container with dot-notation access.
    
    Examples:
        >>> config = Config({'test_execution': {'rate': 1000}})
        >>> config.get('test_execution.rate')
        1000
        >>> config['test_execution']
        {'rate': 1000}
    """
    
    def __init__(self, data: dict) -> None:
        """Initialize config with dictionary data.
        
        Args:
            data: Configuration dictionary loaded from YAML
        """
        self.data = data
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with dot notation support.
        
        Args:
            key: Configuration key (supports dot notation like 'test_execution.rate')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
            
        Examples:
            >>> config.get('test_execution.base_throughput_per_sec', 1000)
            1000
        """
        keys = key.split('.')
        value = self.data
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def __getitem__(self, key: str) -> Any:
        """Dictionary-style access to config.
        
        Args:
            key: Configuration key
            
        Returns:
            Configuration value
            
        Raises:
            KeyError: If key not found
        """
        return self.data[key]
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in config.
        
        Args:
            key: Configuration key
            
        Returns:
            True if key exists
        """
        return key in self.data
    
    def __repr__(self) -> str:
        """String representation of config."""
        return f"Config({len(self.data)} keys)"


def load_config(
    generation_config: Union[str, Path],
    source_config: Optional[Union[str, Path]] = None
) -> Config:
    """Load and merge configuration from YAML files.
    
    Args:
        generation_config: Path to generation config YAML file.
                          Contains event schemas, distribution weights, etc.
        source_config: Optional path to source config YAML file.
                      Contains publisher configuration (Kinesis, Kafka, etc.)
    
    Returns:
        Config object with merged configuration
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If YAML is invalid
        
    Examples:
        >>> config = load_config('config_generation.yaml', 'config_source_kinesis.yaml')
        >>> config.get('event_types')
        [{'event_type_id': 'player.session.start', ...}]
        
        >>> # Load just generation config
        >>> config = load_config('config_generation.yaml')
    """
    # Load generation config
    gen_path = Path(generation_config)
    if not gen_path.exists():
        raise FileNotFoundError(f"Generation config not found: {generation_config}")
    
    with open(gen_path, 'r') as f:
        data = yaml.safe_load(f)
    
    if data is None:
        data = {}
    
    # Load and merge source config if provided
    if source_config:
        src_path = Path(source_config)
        if not src_path.exists():
            raise FileNotFoundError(f"Source config not found: {source_config}")
        
        with open(src_path, 'r') as f:
            source_data = yaml.safe_load(f)
        
        if source_data:
            data.update(source_data)
    
    return Config(data)

