"""AWS Kinesis publisher for streaming events.

Publish generated events to AWS Kinesis Data Streams using boto3.
Supports single event and batch publishing with automatic batching up to
Kinesis API limits (500 records per PutRecords call).
"""

import json
from typing import List, Dict, Any, Optional, Union

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


class PublishResult:
    """Result of a publish operation.
    
    Attributes:
        records_sent: Number of successfully published records
        failed_records: Number of failed records
        errors: List of error messages (if any)
    """
    
    def __init__(
        self, 
        records_sent: int, 
        failed_records: int = 0,
        errors: Optional[List[str]] = None
    ) -> None:
        """Initialize publish result.
        
        Args:
            records_sent: Number of successfully sent records
            failed_records: Number of failed records
            errors: Optional list of error messages
        """
        self.records_sent = records_sent
        self.failed_records = failed_records
        self.errors = errors or []
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"PublishResult(sent={self.records_sent}, "
            f"failed={self.failed_records})"
        )


class KinesisPublisher:
    """Publish events to AWS Kinesis Data Streams.
    
    Uses boto3 to publish events to Kinesis. Automatically handles batching
    up to 500 records per API call (Kinesis PutRecords limit).
    
    Attributes:
        stream_name: Name of the Kinesis stream
        region: AWS region
        client: boto3 Kinesis client

    """
    
    def __init__(self, config: dict) -> None:
        """Initialize Kinesis publisher.
        
        Args:
            config: Configuration dictionary with:
                - stream_name: Kinesis stream name (required)
                - region: AWS region (default: us-east-1)
                - partition_key_field: Field name to use as partition key (default: 'event_type_id')
                - aws_access_key_id: AWS access key (optional, uses env/IAM if not provided)
                - aws_secret_access_key: AWS secret key (optional)
                
        Raises:
            ImportError: If boto3 is not installed
            ClientError: If Kinesis client cannot be initialized
        """
        if not BOTO3_AVAILABLE:
            raise ImportError(
                "boto3 is required for KinesisPublisher. "
                "Install it with: pip install boto3"
            )
        
        self.stream_name = config['stream_name']
        self.region = config.get('region', 'us-east-1')
        self.partition_key_field = config.get('partition_key_field', 'event_type_id')
        
        # Initialize boto3 Kinesis client
        client_kwargs = {'region_name': self.region}
        
        # Add credentials if provided (otherwise boto3 uses env/IAM)
        if 'aws_access_key_id' in config:
            client_kwargs['aws_access_key_id'] = config['aws_access_key_id']
        if 'aws_secret_access_key' in config:
            client_kwargs['aws_secret_access_key'] = config['aws_secret_access_key']
        
        try:
            self.client = boto3.client('kinesis', **client_kwargs)
        except (ClientError, BotoCoreError) as e:
            raise RuntimeError(f"Failed to initialize Kinesis client: {e}")
    
    def publish_single(self, event: Dict[str, Any]) -> PublishResult:
        """Publish a single event to Kinesis.
        
        Args:
            event: Event dictionary with at least:
                - event_type_id: Event type identifier
                - partition key field (configurable, defaults to event_type_id)
                - payload: Event payload (string or dict)
                
        Returns:
            PublishResult with send/failure counts
            
        Examples:
            >>> event = {
            ...     'event_type_id': 'user.login',
            ...     'user_id': 12345,
            ...     'payload': '{"session_id":"abc123"}'
            ... }
            >>> result = publisher.publish_single(event)
        """
        try:
            # Get partition key value from configured field
            partition_key = str(event.get(self.partition_key_field, event.get('event_type_id', '0')))
            
            self.client.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(event),
                PartitionKey=partition_key
            )
            return PublishResult(records_sent=1, failed_records=0)
        
        except (ClientError, BotoCoreError) as e:
            error_msg = f"Error publishing single event: {e}"
            print(error_msg)
            return PublishResult(
                records_sent=0, 
                failed_records=1,
                errors=[error_msg]
            )
    
    def publish_batch(
        self, 
        events: Union[List[Dict[str, Any]], Any]
    ) -> PublishResult:
        """Publish a batch of events to Kinesis.
        
        Automatically splits large batches into chunks of 500 (Kinesis limit).
        Handles both list of dicts and Spark DataFrames.
        
        Args:
            events: List of event dictionaries OR Spark DataFrame
            
        Returns:
            PublishResult with total send/failure counts
            
        Examples:
            >>> events = [event1, event2, event3]
            >>> result = publisher.publish_batch(events)
            >>> print(f"Sent {result.records_sent}/{len(events)} events")
        """
        # Handle Spark DataFrame (for future BatchGenerator compatibility)
        if hasattr(events, 'toPandas'):  # It's a Spark DataFrame
            events = [row.asDict() for row in events.collect()]
        
        total_sent = 0
        total_failed = 0
        all_errors = []
        
        # Split into chunks of 500 (Kinesis PutRecords limit)
        for i in range(0, len(events), 500):
            batch = events[i:i+500]
            
            # Convert to Kinesis record format
            records = [
                {
                    'Data': json.dumps(event),
                    'PartitionKey': str(event.get(self.partition_key_field, event.get('event_type_id', '0')))
                }
                for event in batch
            ]
            
            try:
                response = self.client.put_records(
                    StreamName=self.stream_name,
                    Records=records
                )
                
                # Count failures from response
                failed = response.get('FailedRecordCount', 0)
                total_sent += len(batch) - failed
                total_failed += failed
                
                # Collect error details if any
                if failed > 0:
                    for idx, record in enumerate(response.get('Records', [])):
                        if 'ErrorCode' in record:
                            all_errors.append(
                                f"Record {idx}: {record.get('ErrorCode')} - "
                                f"{record.get('ErrorMessage', 'Unknown error')}"
                            )
            
            except (ClientError, BotoCoreError) as e:
                error_msg = f"Error publishing batch: {e}"
                print(error_msg)
                total_failed += len(batch)
                all_errors.append(error_msg)
        
        return PublishResult(
            records_sent=total_sent,
            failed_records=total_failed,
            errors=all_errors if all_errors else None
        )
    
    def close(self) -> None:
        """Close the publisher and cleanup resources.
        
        boto3 client handles cleanup automatically, so this is a no-op.
        Provided for consistency with publisher interface.
        """
        pass  # boto3 handles cleanup automatically

