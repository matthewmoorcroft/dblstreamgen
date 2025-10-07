"""Field type mappers for dbldatagen column specifications."""

from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType
import dbldatagen as dg
from typing import Dict, Any


class FieldMapper:
    """
    Maps YAML field specifications to dbldatagen column specifications.
    
    Supported field types:
    - uuid: UUID v4 strings
    - int: Integers with range
    - float: Floats with range  
    - string: Strings with values/weights
    - timestamp: Current timestamp
    
    Example:
        >>> mapper = FieldMapper()
        >>> spec = dg.DataGenerator(spark, rows=1000)
        >>> spec = mapper.map_field(spec, 'user_id', {'type': 'int', 'range': [1, 1000000]})
    """
    
    def map_field(self, 
                   spec: dg.DataGenerator, 
                   field_name: str, 
                   field_spec: Dict[str, Any]) -> dg.DataGenerator:
        """
        Add a column to the DataGenerator spec based on field type.
        
        Args:
            spec: Existing DataGenerator spec
            field_name: Name of the field/column
            field_spec: Field specification from YAML config
            
        Returns:
            Updated DataGenerator spec with new column
            
        Raises:
            ValueError: If field type is unsupported
        """
        field_type = field_spec.get('type')
        
        if field_type == 'uuid':
            return self._map_uuid(spec, field_name, field_spec)
        
        elif field_type == 'int':
            return self._map_int(spec, field_name, field_spec)
        
        elif field_type == 'float':
            return self._map_float(spec, field_name, field_spec)
        
        elif field_type == 'string':
            return self._map_string(spec, field_name, field_spec)
        
        elif field_type == 'timestamp':
            return self._map_timestamp(spec, field_name, field_spec)
        
        else:
            raise ValueError(f"Unsupported field type: {field_type}")
    
    def _map_uuid(self, 
                   spec: dg.DataGenerator, 
                   field_name: str, 
                   field_spec: Dict[str, Any]) -> dg.DataGenerator:
        """Map UUID field type."""
        return spec.withColumn(
            field_name, 
            StringType(), 
            expr="uuid()"
        )
    
    def _map_int(self, 
                  spec: dg.DataGenerator, 
                  field_name: str, 
                  field_spec: Dict[str, Any]) -> dg.DataGenerator:
        """
        Map integer field type.
        
        Required: range: [min, max]
        """
        range_values = field_spec.get('range')
        if not range_values or len(range_values) != 2:
            raise ValueError(f"Field '{field_name}': 'range' must be [min, max]")
        
        min_value, max_value = range_values
        
        return spec.withColumn(
            field_name,
            IntegerType(),
            minValue=int(min_value),
            maxValue=int(max_value),
            random=True
        )
    
    def _map_float(self, 
                    spec: dg.DataGenerator, 
                    field_name: str, 
                    field_spec: Dict[str, Any]) -> dg.DataGenerator:
        """
        Map float field type.
        
        Required: range: [min, max]
        """
        range_values = field_spec.get('range')
        if not range_values or len(range_values) != 2:
            raise ValueError(f"Field '{field_name}': 'range' must be [min, max]")
        
        min_value, max_value = range_values
        
        return spec.withColumn(
            field_name,
            FloatType(),
            minValue=float(min_value),
            maxValue=float(max_value),
            random=True
        )
    
    def _map_string(self, 
                     spec: dg.DataGenerator, 
                     field_name: str, 
                     field_spec: Dict[str, Any]) -> dg.DataGenerator:
        """
        Map string field type.
        
        Required: values: [...]
        Optional: weights: [...]
        """
        values = field_spec.get('values')
        if not values:
            raise ValueError(f"Field '{field_name}': 'values' is required for string type")
        
        weights = field_spec.get('weights')
        
        # Validate weights if provided
        if weights:
            if len(weights) != len(values):
                raise ValueError(
                    f"Field '{field_name}': weights length ({len(weights)}) "
                    f"must match values length ({len(values)})"
                )
        
        return spec.withColumn(
            field_name,
            StringType(),
            values=values,
            weights=weights,
            random=True
        )
    
    def _map_timestamp(self, 
                        spec: dg.DataGenerator, 
                        field_name: str, 
                        field_spec: Dict[str, Any]) -> dg.DataGenerator:
        """Map timestamp field type to current timestamp."""
        return spec.withColumn(
            field_name,
            TimestampType(),
            expr="now()"
        )
