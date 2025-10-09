"""Custom PySpark DataSource for AWS Kinesis streaming."""

import logging
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage
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
                - service_credential: Unity Catalog service credential (single-user clusters only)
                - aws_access_key_id + aws_secret_access_key: Direct IAM credentials
                - (none): Use cluster instance profile
        """
        self.stream_name = options.get("stream_name")
        self.region = options.get("region", "us-east-1")
        self.partition_key_field = options.get("partition_key_field", "partition_key")
        
        if not self.stream_name:
            raise ValueError("stream_name is required for Kinesis writer")
        
        # Authentication options
        self.service_credential = options.get("service_credential")
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
        elif self.service_credential:
            logger.info(f"Using Unity Catalog service credential: {self.service_credential}")
        else:
            logger.info("Using cluster instance profile")
    
    
    def write(self, iterator):
        """
        Write partition data to Kinesis in 500-record chunks with parallel streaming.
        
        Optimized for throughput:
        - True streaming: submits chunks as they're ready (no materialization)
        - Pre-converts rows to Kinesis format
        - Fixed 50-thread pool for optimal throughput
        - Parallel processing overlaps with network I/O
        
        Args:
            iterator: Iterator of Row objects from partition
            
        Returns:
            KinesisCommitMessage with sent/failed counts
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        try:
            # Initialize boto3 client once per partition
            print(f"🔧 Initializing Kinesis client for partition...")
            client = self._create_kinesis_client()
            print(f"✅ Kinesis client initialized successfully")
        except Exception as e:
            print(f"❌ FAILED to create Kinesis client: {e}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            raise
        
        # Fixed 50-thread pool for optimal throughput
        # 50 threads × 8 partitions = 400 threads per executor (safe for most executors)
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = []
            current_chunk = []
            
            # Stream rows and submit chunks immediately when ready
            row_count = 0
            for row in iterator:
                row_count += 1
                # Fast path: direct attribute access
                try:
                    payload = row.serialized_payload
                    partition_key = row.partition_key
                except AttributeError:
                    # Fallback to dict access
                    row_dict = row.asDict()
                    payload = row_dict.get('serialized_payload', '')
                    partition_key = row_dict.get('partition_key', row_dict.get('event_type_id', '0'))
                
                # Validate data before encoding
                if payload is None or partition_key is None:
                    logger.error(f"Invalid row data: payload={payload}, partition_key={partition_key}")
                    print(f"❌ Invalid row {row_count}: payload={payload}, partition_key={partition_key}")
                    continue
                
                # Pre-convert to Kinesis format
                current_chunk.append({
                    'Data': payload.encode('utf-8') if isinstance(payload, str) else payload,
                    'PartitionKey': str(partition_key)
                })
                
                # Submit chunk immediately when full (start sending while still processing)
                if len(current_chunk) >= 500:
                    # Pass reference directly (safe - we reset current_chunk immediately after)
                    futures.append(executor.submit(self._send_prepared_chunk, client, current_chunk))
                    current_chunk = []
            
            print(f"📥 Processed {row_count} rows, submitting {len(futures)} chunks ({len(current_chunk)} remaining)")
            
            # Submit remaining records
            if current_chunk:
                futures.append(executor.submit(self._send_prepared_chunk, client, current_chunk))
            
            # No data to send
            if not futures:
                return KinesisCommitMessage(0, 0)
            
            # Collect results as they complete
            total_sent = 0
            total_failed = 0
            
            for future in as_completed(futures):
                try:
                    sent, failed = future.result()
                    total_sent += sent
                    total_failed += failed
                except Exception as e:
                    logger.error(f"Error sending chunk: {e}")
                    if logger.isEnabledFor(logging.DEBUG):
                        import traceback
                        logger.debug(f"Traceback: {traceback.format_exc()}")
        
        print(f"📊 Partition complete: {total_sent} sent, {total_failed} failed")
        return KinesisCommitMessage(total_sent, total_failed)
    
    def _send_prepared_chunk(self, client, kinesis_records) -> Tuple[int, int]:
        """
        Send pre-formatted Kinesis records (optimized - no conversion needed).
        
        Args:
            client: boto3 Kinesis client
            kinesis_records: List of dicts with 'Data' and 'PartitionKey' keys (≤500)
            
        Returns:
            Tuple of (sent_count, failed_count)
        """
        try:
            response = client.put_records(
                StreamName=self.stream_name,
                Records=kinesis_records
            )
            
            failed = response.get('FailedRecordCount', 0)
            sent = len(kinesis_records) - failed
            
            # Only log errors if we have failures
            if failed > 0 and 'Records' in response:
                error_codes = {}
                for record_response in response['Records']:
                    if 'ErrorCode' in record_response:
                        error_code = record_response['ErrorCode']
                        error_codes[error_code] = error_codes.get(error_code, 0) + 1
                
                # Log summary only (avoid per-record logging for performance)
                if error_codes:
                    logger.error(f"Kinesis errors in chunk: {error_codes}")
            
            return (sent, failed)
        
        except Exception as e:
            import traceback
            full_trace = traceback.format_exc()
            print(f"❌ EXCEPTION in put_records: {type(e).__name__}: {e}")
            print(f"   Stream: {self.stream_name}, Region: {self.region}, Chunk size: {len(kinesis_records)}")
            logger.error(f"EXCEPTION in put_records: {type(e).__name__}: {e}")
            logger.error(f"Stream: {self.stream_name}, Region: {self.region}")
            logger.error(f"Chunk size: {len(kinesis_records)}")
            logger.error(f"Full traceback:\n{full_trace}")
            return (0, len(kinesis_records))
    
    def commit(self, messages, batchId):
        """
        Handle successful batch commit.
        
        Args:
            messages: List of KinesisCommitMessage from all partitions
            batchId: Batch ID
        """
        total_sent = sum(m.sent_count for m in messages)
        total_failed = sum(m.failed_count for m in messages)
        
        if total_failed > 0:
            print(f"⚠️  Batch {batchId}: {total_sent:,} sent, {total_failed:,} failed")
            logger.warning(f"Batch {batchId}: {total_sent:,} sent, {total_failed:,} failed")
        else:
            print(f"✅ Batch {batchId}: {total_sent:,} records sent to Kinesis")
            logger.info(f"Batch {batchId}: {total_sent:,} records sent to Kinesis")
    
    def abort(self, messages, batchId):
        """
        Handle batch failure.
        
        Args:
            messages: List of commit messages from successful partitions
            batchId: Batch ID
        """
        print(f"❌ Batch {batchId} aborted - check executor logs for errors")
        logger.error(f"Batch {batchId} aborted")
    
    def _create_kinesis_client(self):
        """
        Create boto3 Kinesis client with automatic credential refresh and optimized connection pooling.
        
        Runs on executor nodes. Supports three authentication methods:
        1. Unity Catalog service credential (auto-refresh, single-user clusters only)
        2. Direct IAM credentials (access key + secret key)
        3. Cluster instance profile (automatic)
        
        Returns:
            boto3 Kinesis client
        """
        import boto3
        from botocore.config import Config
        
        # Optimize client for parallel requests
        client_config = Config(
            max_pool_connections=50,  # Match thread pool size exactly
            tcp_keepalive=True  # Keep connections alive for reuse
        )
        
        # Option 1: Unity Catalog service credential with auto-refresh
        if self.service_credential:
            try:
                from databricks.service_credentials import getServiceCredentialsProvider
                
                # Get credential provider (works in executor/UDF context on single-user clusters)
                credential_provider = getServiceCredentialsProvider(self.service_credential)
                
                # Create boto3 session with the provider for automatic token refresh
                session = boto3.Session(botocore_session=credential_provider)
                client = session.client('kinesis', region_name=self.region, config=client_config)
                
                logger.info(f"Connected to Kinesis with service credential (auto-refresh enabled)")
                return client
                    
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                logger.error(f"Failed to use service credential '{self.service_credential}': {e}")
                logger.error(f"Full traceback: {error_details}")
                raise RuntimeError(
                    f"Service credential '{self.service_credential}' failed: {str(e)}\n\n"
                    f"Ensure: 1) Single-user cluster, 2) Credential exists, 3) You have USE privilege.\n\n"
                    f"Full error: {error_details}"
                )
        
        # Option 2: Direct IAM credentials
        elif self.aws_access_key and self.aws_secret_key:
            client_kwargs = {
                'region_name': self.region,
                'aws_access_key_id': self.aws_access_key,
                'aws_secret_access_key': self.aws_secret_key,
                'config': client_config
            }
            if self.aws_session_token:
                client_kwargs['aws_session_token'] = self.aws_session_token
            
            client = boto3.client('kinesis', **client_kwargs)
            logger.info(f"Connected to Kinesis with direct credentials")
            return client
        
        # Option 3: Cluster instance profile
        else:
            client = boto3.client('kinesis', region_name=self.region, config=client_config)
            logger.info(f"Connected to Kinesis with instance profile")
            return client


class KinesisCommitMessage(WriterCommitMessage):
    """
    Commit message from Kinesis writer containing send statistics.
    
    Inherits from WriterCommitMessage for PySpark DataSource API compatibility.
    
    Attributes:
        sent_count: Number of records successfully sent
        failed_count: Number of records that failed to send
    """
    
    def __init__(self, sent_count: int, failed_count: int):
        """
        Initialize commit message.
        
        Args:
            sent_count: Number of records successfully sent
            failed_count: Number of records that failed to send
        """
        self.sent_count = sent_count
        self.failed_count = failed_count
