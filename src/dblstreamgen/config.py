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
    - Event type weights are positive integers
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
        
        # Validate weights are positive integers
        weights = [et.get('weight', 0) for et in event_types]
        if not all(isinstance(w, int) and w > 0 for w in weights):
            raise ConfigurationError(
                f"Event type weights must be positive integers (e.g., [6, 3, 1] for 60%/30%/10%)"
            )
        
        # Validate event type IDs are unique
        event_ids = [et['event_type_id'] for et in event_types]
        if len(event_ids) != len(set(event_ids)):
            raise ConfigurationError("Event type IDs must be unique")
        
        # Validate common_fields weights are positive integers (if present)
        common_fields = self.data.get('common_fields', {})
        for field_name, field_spec in common_fields.items():
            if 'weights' in field_spec:
                weights = field_spec['weights']
                if not all(isinstance(w, int) and w > 0 for w in weights):
                    raise ConfigurationError(
                        f"Weights for common_fields.{field_name} must be positive integers. "
                        f"Got: {weights}. Example: [2, 4, 4] instead of [0.2, 0.4, 0.4]"
                    )

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
        
        # Validate derived_fields (if present)
        derived_fields = self.data.get('derived_fields', {})
        for field_name, field_spec in derived_fields.items():
            if 'expr' not in field_spec:
                raise ConfigurationError(
                    f"derived_fields.{field_name} must specify 'expr'"
                )
            if 'type' not in field_spec:
                raise ConfigurationError(
                    f"derived_fields.{field_name} must specify 'type'"
                )
        
        # Validate supported field types
        self._validate_field_types()
        
        # Validate outliers and percent_nulls across all field specs
        self._validate_field_options()
        
        # Validate field type consistency across event types
        self._validate_field_type_consistency()
    
    def _validate_field_types(self):
        """Validate all field types are supported."""
        supported_types = {
            'uuid', 'int', 'float', 'string', 'timestamp',
            'boolean', 'long', 'double', 'date', 'decimal',
            'byte', 'short', 'binary', 'array', 'struct', 'map'
        }
        
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
    
    def _validate_field_options(self):
        """Validate outliers and percent_nulls across all field specs."""
        # Check common fields (including struct sub-fields)
        for field_name, field_spec in self.data.get('common_fields', {}).items():
            self._validate_single_field_options(field_spec, f"common_fields.{field_name}")
        
        # Check event-type fields
        for event_type in self.data['event_types']:
            et_id = event_type['event_type_id']
            for field_name, field_spec in event_type.get('fields', {}).items():
                self._validate_single_field_options(field_spec, f"{et_id}.fields.{field_name}")
        
        # Check derived fields
        for field_name, field_spec in self.data.get('derived_fields', {}).items():
            self._validate_single_field_options(field_spec, f"derived_fields.{field_name}")
    
    def _validate_single_field_options(self, field_spec: dict, location: str):
        """Validate outliers and percent_nulls on a single field spec, recursing into structs."""
        # Validate outliers
        outliers = field_spec.get('outliers', [])
        if not isinstance(outliers, list):
            raise ConfigurationError(f"{location}.outliers must be a list")
        for i, outlier in enumerate(outliers):
            if 'percent' not in outlier:
                raise ConfigurationError(
                    f"{location}.outliers[{i}] missing required key 'percent'"
                )
            if 'expr' not in outlier:
                raise ConfigurationError(
                    f"{location}.outliers[{i}] missing required key 'expr'"
                )
            pct = outlier['percent']
            if not isinstance(pct, (int, float)) or pct <= 0 or pct >= 1:
                raise ConfigurationError(
                    f"{location}.outliers[{i}].percent must be between 0 and 1 (exclusive), got {pct}"
                )
        
        # Validate percent_nulls
        pn = field_spec.get('percent_nulls')
        if pn is not None:
            if not isinstance(pn, (int, float)) or pn < 0 or pn >= 1:
                raise ConfigurationError(
                    f"{location}.percent_nulls must be between 0 and 1 (exclusive), got {pn}"
                )
        
        # Recurse into struct sub-fields
        if field_spec.get('type') == 'struct':
            for sub_name, sub_spec in field_spec.get('fields', {}).items():
                self._validate_single_field_options(sub_spec, f"{location}.fields.{sub_name}")
    
    def _compare_field_specs(self, spec1: dict, spec2: dict, field_name: str, loc1: str, loc2: str):
        """
        Compare two field specs for consistency (handles nested types).
        
        Raises ConfigurationError if specs are incompatible.
        """
        type1 = spec1.get('type')
        type2 = spec2.get('type')
        
        # Types must match
        if type1 != type2:
            raise ConfigurationError(
                f"Field '{field_name}' has inconsistent types: {loc1} uses '{type1}' "
                f"but {loc2} uses '{type2}'. Use different field names or standardize on one type."
            )
        
        # For arrays, check item_type consistency
        if type1 == 'array':
            item_type1 = spec1.get('item_type', 'string')
            item_type2 = spec2.get('item_type', 'string')
            if item_type1 != item_type2:
                raise ConfigurationError(
                    f"Array field '{field_name}' has inconsistent item types: "
                    f"{loc1} uses '{item_type1}' but {loc2} uses '{item_type2}'"
                )
        
        # For maps, check key_type and value_type consistency
        if type1 == 'map':
            key_type1 = spec1.get('key_type', 'string')
            key_type2 = spec2.get('key_type', 'string')
            value_type1 = spec1.get('value_type', 'string')
            value_type2 = spec2.get('value_type', 'string')
            
            if key_type1 != key_type2 or value_type1 != value_type2:
                raise ConfigurationError(
                    f"Map field '{field_name}' has inconsistent types: "
                    f"{loc1} uses map<{key_type1},{value_type1}> but {loc2} uses map<{key_type2},{value_type2}>"
                )
        
        # For structs, recursively check nested fields
        if type1 == 'struct':
            fields1 = spec1.get('fields', {})
            fields2 = spec2.get('fields', {})
            
            # Check that field names match
            keys1 = set(fields1.keys())
            keys2 = set(fields2.keys())
            
            if keys1 != keys2:
                missing_in_2 = keys1 - keys2
                missing_in_1 = keys2 - keys1
                msg = f"Struct field '{field_name}' has inconsistent nested fields: "
                if missing_in_2:
                    msg += f"{loc2} missing fields {missing_in_2}. "
                if missing_in_1:
                    msg += f"{loc1} missing fields {missing_in_1}."
                raise ConfigurationError(msg)
            
            # Recursively check each nested field
            for nested_field_name in keys1:
                self._compare_field_specs(
                    fields1[nested_field_name],
                    fields2[nested_field_name],
                    f"{field_name}.{nested_field_name}",
                    loc1,
                    loc2
                )
    
    def _validate_field_type_consistency(self):
        """Validate fields with the same name have consistent types (required for wide schema)."""
        # Build field type registry: field_name -> (spec, event_type_id)
        field_registry = {}
        
        # Check common fields first (these set the baseline)
        for field_name, field_spec in self.data.get('common_fields', {}).items():
            field_registry[field_name] = (field_spec, 'common_fields')
        
        # Check event-specific fields
        for event_type in self.data['event_types']:
            event_id = event_type['event_type_id']
            
            for field_name, field_spec in event_type.get('fields', {}).items():
                if field_name in field_registry:
                    # Check consistency (including nested type details)
                    existing_spec, existing_location = field_registry[field_name]
                    self._compare_field_specs(existing_spec, field_spec, field_name, existing_location, event_id)
                else:
                    field_registry[field_name] = (field_spec, event_id)
    
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