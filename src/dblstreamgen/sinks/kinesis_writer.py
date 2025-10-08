"""Custom PySpark DataSource for AWS Kinesis streaming."""

import logging
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage
from pyspark import TaskContext
from typing import Tuple

logger = logging.getLogger(__name__)


class KinesisDataSource(DataSource):
    """
    PySpark DataSource for AWS Kinesis with parallel batching.
    
    Kinesis limits: 1000 records/sec per shard, 500 records per PutRecords request.
    Recommended shards: total_rows_per_second / 1000.
    """
    
    @classmethod
    def name(cls):
        """Return the data source name."""
        return "dblstreamgen_kinesis"
    
    def streamWriter(self, schema, overwrite: bool):
        """Create a stream writer instance."""
        return KinesisStreamWriter(self.options)


class KinesisStreamWriter(DataSourceStreamWriter):
    """
    Kinesis stream writer with 500-record batching and 50 parallel requests per partition.
    """
    
    def __init__(self, options):
        """
        Initialize Kinesis stream writer.
        
        Args:
            options: Configuration options dict with:
                - stream_name (required): Kinesis stream name
                - region (default: us-east-1): AWS region
                - partition_key_field (default: partition_key): Field for partitioning
                
                Authentication (choose one, or omit for instance profile):
                - service_credential_name: Unity Catalog service credential (single-user clusters only)
                - aws_access_key_id + aws_secret_access_key: Direct IAM credentials
                - (none): Use cluster instance profile
        """
        self.stream_name = options.get("stream_name")
        self.region = options.get("region", "us-east-1")
        self.partition_key_field = options.get("partition_key_field", "partition_key")
        
        if not self.stream_name:
            raise ValueError("stream_name is required for Kinesis writer")
        
        # Authentication options
        self.service_credential_name = options.get("service_credential_name")
        self.aws_access_key = options.get("aws_access_key_id")
        self.aws_secret_key = options.get("aws_secret_access_key")
        self.aws_session_token = options.get("aws_session_token")
        
        # Validate credentials if provided
        if self.aws_access_key or self.aws_secret_key:
            if not (self.aws_access_key and self.aws_secret_key):
                raise ValueError(
                    "Both 'aws_access_key_id' and 'aws_secret_access_key' are required"
                )
            logger.info(f"Using direct AWS credentials")
        elif self.service_credential_name:
            logger.info(f"Using Unity Catalog service credential: {self.service_credential_name}")
        else:
            logger.info("Using cluster instance profile")
    
    
    def write(self, iterator):
        """
        Write partition data to Kinesis in 500-record chunks with 50 parallel threads.
        
        Args:
            iterator: Iterator of Row objects from partition
            
        Returns:
            KinesisCommitMessage with sent/failed counts
        """
        # Import here to ensure serializability
        import boto3
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # Initialize boto3 client with appropriate credentials
        client = self._create_kinesis_client()
        
        # Collect all records from iterator
        records = list(iterator)
        partition_id = TaskContext.get().partitionId()
        
        if not records:
            return KinesisCommitMessage(partition_id, 0, 0)
        
        # Split into chunks of 500 (Kinesis PutRecords API limit)
        chunks = [records[i:i+500] for i in range(0, len(records), 500)]
        
        # Send chunks in parallel (50 threads for maximum throughput)
        total_sent = 0
        total_failed = 0
        
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [
                executor.submit(self._send_chunk, client, chunk)
                for chunk in chunks
            ]
            
            for future in as_completed(futures):
                try:
                    sent, failed = future.result()
                    total_sent += sent
                    total_failed += failed
                except Exception as e:
                    logger.error(f"Error sending chunk: {e}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    # Can't determine exact chunk size here, will be counted in failed records
                    pass
        
        return KinesisCommitMessage(partition_id, total_sent, total_failed)
    
    def _send_chunk(self, client, records) -> Tuple[int, int]:
        """
        Send one chunk (≤500 records) to Kinesis.
        
        Args:
            client: boto3 Kinesis client
            records: List of Row objects (≤500)
            
        Returns:
            Tuple of (sent_count, failed_count)
        """
        kinesis_records = []
        
        for row in records:
            # Convert Row to dict for easier access
            row_dict = row.asDict() if hasattr(row, 'asDict') else row
            
            kinesis_records.append({
                'Data': str(row_dict.get('serialized_payload', '')).encode('utf-8'),
                'PartitionKey': str(row_dict.get('partition_key', row_dict.get('event_type_id', '0')))
            })
        
        try:
            response = client.put_records(
                StreamName=self.stream_name,
                Records=kinesis_records
            )
            
            failed = response.get('FailedRecordCount', 0)
            sent = len(records) - failed
            
            # Log specific errors for failed records
            if failed > 0 and 'Records' in response:
                error_codes = {}
                for i, record_response in enumerate(response['Records']):
                    if 'ErrorCode' in record_response:
                        error_code = record_response['ErrorCode']
                        error_msg = record_response.get('ErrorMessage', 'No message')
                        error_codes[error_code] = error_codes.get(error_code, 0) + 1
                        
                        # Log first occurrence of each error type
                        if error_codes[error_code] == 1:
                            logger.error(f"Kinesis error: {error_code} - {error_msg}")
                
                logger.error(f"Failed {failed}/{len(records)} records. Error summary: {error_codes}")
            
            return (sent, failed)
        
        except Exception as e:
            logger.error(f"Failed to send chunk to Kinesis: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return (0, len(records))
    
    def commit(self, messages, batchId):
        """
        Handle successful batch commit.
        
        Args:
            messages: List of KinesisCommitMessage from all partitions
            batchId: Batch ID
        """
        total_sent = sum(m.records_sent for m in messages)
        total_failed = sum(m.records_failed for m in messages)
        
        if total_failed > 0:
            logger.warning(f"Batch {batchId}: {total_sent:,} sent, {total_failed:,} failed")
        else:
            logger.info(f"Batch {batchId}: {total_sent:,} records sent to Kinesis")
    
    def abort(self, messages, batchId):
        """
        Handle batch failure.
        
        Args:
            messages: List of commit messages from successful partitions
            batchId: Batch ID
        """
        logger.error(f"Batch {batchId} aborted")
    
    def _create_kinesis_client(self):
        """
        Create boto3 Kinesis client with automatic credential refresh.
        
        Runs on executor nodes. Supports three authentication methods:
        1. Unity Catalog service credential (auto-refresh, single-user clusters only)
        2. Direct IAM credentials (access key + secret key)
        3. Cluster instance profile (automatic)
        
        Returns:
            boto3 Kinesis client
        """
        import boto3
        
        # Option 1: Unity Catalog service credential with auto-refresh
        if self.service_credential_name:
            try:
                from databricks.service_credentials import getServiceCredentialsProvider
                
                # Get credential provider (works in executor/UDF context on single-user clusters)
                credential_provider = getServiceCredentialsProvider(self.service_credential_name)
                
                # Create boto3 session with the provider for automatic token refresh
                session = boto3.Session(botocore_session=credential_provider)
                client = session.client('kinesis', region_name=self.region)
                
                # Verify connection
                client.describe_stream(StreamName=self.stream_name, Limit=1)
                logger.info(f"Connected to Kinesis with service credential (auto-refresh enabled)")
                return client
                    
            except Exception as e:
                logger.error(f"Failed to use service credential '{self.service_credential_name}': {e}")
                raise RuntimeError(
                    f"Service credential '{self.service_credential_name}' failed. "
                    f"Ensure: 1) Single-user cluster, 2) Credential exists, 3) You have USE privilege."
                )
        
        # Option 2: Direct IAM credentials
        elif self.aws_access_key and self.aws_secret_key:
            client_kwargs = {
                'region_name': self.region,
                'aws_access_key_id': self.aws_access_key,
                'aws_secret_access_key': self.aws_secret_key
            }
            if self.aws_session_token:
                client_kwargs['aws_session_token'] = self.aws_session_token
            
            client = boto3.client('kinesis', **client_kwargs)
            client.describe_stream(StreamName=self.stream_name, Limit=1)
            logger.info(f"Connected to Kinesis with direct credentials")
            return client
        
        # Option 3: Cluster instance profile
        else:
            client = boto3.client('kinesis', region_name=self.region)
            client.describe_stream(StreamName=self.stream_name, Limit=1)
            logger.info(f"Connected to Kinesis with instance profile")
            return client


class KinesisCommitMessage(WriterCommitMessage):
    """
    Commit message from Kinesis writer containing send statistics.
    
    Inherits from WriterCommitMessage for PySpark DataSource API compatibility.
    
    Attributes:
        partition_id: Spark partition ID
        records_sent: Number of records successfully sent
        records_failed: Number of records that failed to send
    """
    
    def __init__(self, partition_id: int, records_sent: int, records_failed: int):
        """
        Initialize commit message.
        
        Args:
            partition_id: Spark partition ID
            records_sent: Number of records successfully sent
            records_failed: Number of records that failed to send
        """
        self.partition_id = partition_id
        self.records_sent = records_sent
        self.records_failed = records_failed
