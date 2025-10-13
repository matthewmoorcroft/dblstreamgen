"""Configuration management for dblstreamgen."""

import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigurationError(Exception):
    """Raised when configuration is invalid."""
    pass


class Config:
    """
    Configuration container with validation.
    
    Validates:
    - Event type weights sum to 1.0
    - Required fields present
    - Field types supported
    """
    
    def __init__(self, data: dict):
        """
        Initialize configuration.
        
        Args:
            data: Configuration dictionary from YAML
        """
        self.data = data
        self._validate()
    
    def _validate(self):
        """Validate configuration."""
        # Check required top-level fields
        required_fields = ['event_types', 'generation_mode', 'sink_config']
        for field in required_fields:
            if field not in self.data:
                raise ConfigurationError(f"Missing required field: {field}")
        
        # Validate event types
        event_types = self.data['event_types']
        if not event_types:
            raise ConfigurationError("At least one event type must be defined")
        
        # Validate weights sum to 1.0
        weights = [et.get('weight', 0) for et in event_types]
        weight_sum = sum(weights)
        if abs(weight_sum - 1.0) > 0.001:
            raise ConfigurationError(
                f"Event type weights must sum to 1.0, got {weight_sum:.4f}"
            )
        
        # Validate event type IDs are unique
        event_ids = [et['event_type_id'] for et in event_types]
        if len(event_ids) != len(set(event_ids)):
            raise ConfigurationError("Event type IDs must be unique")
        
        # Validate generation mode
        gen_mode = self.data['generation_mode']
        if gen_mode not in ['streaming', 'batch']:
            raise ConfigurationError(
                f"generation_mode must be 'streaming' or 'batch', got '{gen_mode}'"
            )
        
        # Validate mode-specific config
        if gen_mode == 'streaming' and 'streaming_config' not in self.data:
            raise ConfigurationError("streaming_config required for streaming mode")
        
        if gen_mode == 'batch' and 'batch_config' not in self.data:
            raise ConfigurationError("batch_config required for batch mode")
        
        # Validate sink config
        sink_config = self.data['sink_config']
        if 'type' not in sink_config:
            raise ConfigurationError("sink_config.type is required")
        
        # Validate supported field types
        self._validate_field_types()
        
        # Validate field type consistency across event types
        self._validate_field_type_consistency()
    
    def _validate_field_types(self):
        """Validate all field types are supported."""
        supported_types = {'uuid', 'int', 'float', 'string', 'timestamp'}
        
        # Check common fields
        for field_name, field_spec in self.data.get('common_fields', {}).items():
            field_type = field_spec.get('type')
            if field_type not in supported_types:
                raise ConfigurationError(
                    f"Unsupported field type '{field_type}' in common_fields.{field_name}. "
                    f"Supported types: {supported_types}"
                )
        
        # Check event type fields
        for event_type in self.data['event_types']:
            for field_name, field_spec in event_type.get('fields', {}).items():
                field_type = field_spec.get('type')
                if field_type not in supported_types:
                    raise ConfigurationError(
                        f"Unsupported field type '{field_type}' in "
                        f"{event_type['event_type_id']}.fields.{field_name}. "
                        f"Supported types: {supported_types}"
                    )
    
    def _validate_field_type_consistency(self):
        """Validate fields with the same name have consistent types (required for wide schema)."""
        # Build field type registry: field_name -> (type, event_type_id)
        field_registry = {}
        
        # Check common fields first (these set the baseline)
        for field_name, field_spec in self.data.get('common_fields', {}).items():
            field_type = field_spec.get('type')
            field_registry[field_name] = (field_type, 'common_fields')
        
        # Check event-specific fields
        for event_type in self.data['event_types']:
            event_id = event_type['event_type_id']
            
            for field_name, field_spec in event_type.get('fields', {}).items():
                field_type = field_spec.get('type')
                
                if field_name in field_registry:
                    # Check consistency
                    existing_type, existing_location = field_registry[field_name]
                    
                    if existing_type != field_type:
                        raise ConfigurationError(
                            f"Field '{field_name}' has inconsistent types: {existing_location} uses '{existing_type}' "
                            f"but {event_id} uses '{field_type}'. Use different field names (e.g., '{field_name}_{existing_type}' "
                            f"and '{field_name}_{field_type}') or standardize on one type."
                        )
                else:
                    field_registry[field_name] = (field_type, event_id)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value with dot notation support.
        
        Args:
            key: Configuration key (supports dot notation: 'sink_config.type')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
            
        Example:
            >>> config.get('sink_config.type')
            'kinesis'
            >>> config.get('streaming_config.total_rows_per_second', 1000)
            1000
        """
        keys = key.split('.')
        value = self.data
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        
        return value
    
    def __getitem__(self, key: str) -> Any:
        """
        Get configuration value using dict-like access.
        
        Args:
            key: Configuration key
            
        Returns:
            Configuration value
            
        Raises:
            KeyError: If key not found
        """
        return self.data[key]
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in configuration."""
        return key in self.data


def load_config(config_path: str) -> Config:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Validated Config object
        
    Raises:
        ConfigurationError: If configuration is invalid
        FileNotFoundError: If config file doesn't exist
        
    Example:
        >>> config = load_config('/Volumes/catalog/schema/volume/config.yaml')
        >>> config.get('generation_mode')
        'streaming'
    """
    path = Path(config_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    try:
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Invalid YAML: {e}")
    
    if not data:
        raise ConfigurationError("Empty configuration file")
    
    return Config(data)


def load_config_from_dict(data: dict) -> Config:
    """
    Load configuration from dictionary (for testing).
    
    Args:
        data: Configuration dictionary
        
    Returns:
        Validated Config object
    """
    return Config(data)