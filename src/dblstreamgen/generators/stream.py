"""Stream-based continuous event generation.

Python-based generator for continuous event streams with precise rate control.
Best for sustained tests (24+ hours) with moderate throughput (100-5K events/sec).
"""

import random
import time
import json
import uuid
from datetime import datetime, timezone
from typing import Iterator, Dict, Any, List


class StreamGenerator:
    """Generate synthetic events continuously using Python loops.
    
    Generates events one-by-one based on schemas defined in configuration.
    Supports rate limiting, weighted event type distribution, and schema-based
    payload generation.
    
    Attributes:
        config: Configuration object containing event schemas and execution params
        event_types: List of event type definitions from config
        _running: Internal flag to control generation loop
        
    Examples:
        >>> config = load_config('config_generation.yaml')
        >>> generator = StreamGenerator(config)
        >>> 
        >>> for event in generator.generate():
        ...     print(event['event_type_id'])
        ...     if some_condition:
        ...         generator.stop()
        ...         break
    """
    
    def __init__(self, config: Any) -> None:
        """Initialize stream generator.
        
        Args:
            config: Config object with event_types and test_execution settings
        """
        self.config = config
        self.event_types = config['event_types']
        self._running = False
        
        # Precompute event type selection (for performance)
        self.event_ids = [et['event_type_id'] for et in self.event_types]
        self.weights = [et['weight'] for et in self.event_types]
    
    def generate(self) -> Iterator[Dict[str, Any]]:
        """Generate events continuously until stopped.
        
        Yields events as dictionaries with the following structure:
        {
            'event_type_id': 'user.login',
            'event_timestamp': '2024-10-03T10:30:45.123456Z',
            'user_id': 12345,  # Common fields from config
            'payload': '{"session_id":"...","device":"mobile",...}'
        }
        
        Yields:
            Event dictionary matching the schema
            
        Examples:
            >>> generator = StreamGenerator(config)
            >>> events = []
            >>> for event in generator.generate():
            ...     events.append(event)
            ...     if len(events) >= 100:
            ...         generator.stop()
            ...         break
        """
        self._running = True
        
        # Get rate limiting config
        target_rate = self.config.get('test_execution.base_throughput_per_sec', 1000)
        sleep_time = 1.0 / target_rate if target_rate > 0 else 0
        
        while self._running:
            # Select event type based on weights
            event_type_id = random.choices(self.event_ids, weights=self.weights)[0]
            
            # Find schema for this event type
            schema = next(
                et for et in self.event_types 
                if et['event_type_id'] == event_type_id
            )
            
            # Generate event
            event = self._generate_event(event_type_id, schema)
            
            yield event
            
            # Rate limiting
            if sleep_time > 0:
                time.sleep(sleep_time)
    
    def _generate_event(self, event_type_id: str, schema: dict) -> Dict[str, Any]:
        """Generate a single event matching the schema.
        
        Args:
            event_type_id: Event type identifier
            schema: Event schema definition from config
            
        Returns:
            Complete event dictionary
        """
        # Generate base event structure (always include these)
        event = {
            'event_type_id': event_type_id,
            'event_timestamp': datetime.now(timezone.utc).isoformat(),
        }
        
        # Add common fields from config (if defined)
        common_fields = self.config.get('common_fields', {})
        for field_name, field_def in common_fields.items():
            event[field_name] = self._generate_field(field_def)
        
        # Build payload from event-specific schema fields
        payload = {}
        for field_name, field_def in schema.get('fields', {}).items():
            payload[field_name] = self._generate_field(field_def)
        
        # Add padding to reach target payload size
        target_bytes = int(schema.get('avg_payload_kb', 2) * 1024)
        current_bytes = len(json.dumps(payload))
        
        if current_bytes < target_bytes:
            padding_size = target_bytes - current_bytes - 20  # Account for JSON overhead
            if padding_size > 0:
                payload['_padding'] = 'x' * padding_size
        
        # Serialize payload as JSON string
        event['payload'] = json.dumps(payload)
        
        return event
    
    def _generate_field(self, field_def: dict) -> Any:
        """Generate value for a field based on its type definition.
        
        Args:
            field_def: Field definition from schema with 'type' and type-specific params
            
        Returns:
            Generated field value
            
        Supported types:
            - uuid: Random UUID string
            - string: String from values list (optionally weighted)
            - int: Integer in range
            - float: Float in range
            - timestamp: ISO timestamp string
        """
        field_type = field_def.get('type', 'string')
        
        if field_type == 'uuid':
            return str(uuid.uuid4())
        
        elif field_type == 'string':
            values = field_def.get('values', ['value1', 'value2'])
            weights = field_def.get('weights')
            
            if weights:
                return random.choices(values, weights=weights)[0]
            else:
                return random.choice(values)
        
        elif field_type == 'int':
            range_vals = field_def.get('range', [0, 100])
            return random.randint(range_vals[0], range_vals[1])
        
        elif field_type == 'float':
            range_vals = field_def.get('range', [0.0, 100.0])
            return round(random.uniform(range_vals[0], range_vals[1]), 2)
        
        elif field_type == 'timestamp':
            return datetime.now(timezone.utc).isoformat()
        
        else:
            # Default to simple string value
            return f"value_{random.randint(1000, 9999)}"
    
    def stop(self) -> None:
        """Stop event generation gracefully.
        
        Signals the generation loop to stop after the current event.
        
        Examples:
            >>> generator = StreamGenerator(config)
            >>> # In another thread or after some condition:
            >>> generator.stop()
        """
        self._running = False

