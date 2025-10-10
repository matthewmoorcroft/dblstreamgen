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
    """Kinesis stream writer with 500-record batching and 50 parallel requests per partition."""
    
    def __init__(self, options):
        """Initialize Kinesis stream writer with authentication options."""
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
        
        if self.aws_access_key or self.aws_secret_key:
            if not (self.aws_access_key and self.aws_secret_key):
                raise ValueError("Both 'aws_access_key_id' and 'aws_secret_access_key' are required")
    
    
    def write(self, iterator):
        """Write partition data to Kinesis in 500-record chunks with 50 parallel requests."""
        try:
            client = self._create_kinesis_client()
        except Exception as e:
            logger.error(f"Failed to create Kinesis client: {e}")
            raise
        
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures, current_chunk = [], []
            
            for row in iterator:
                try:
                    payload, partition_key = row.serialized_payload, row.partition_key
                except AttributeError:
                    row_dict = row.asDict()
                    payload = row_dict.get('serialized_payload', '')
                    partition_key = row_dict.get('partition_key', row_dict.get('event_type_id', '0'))
                
                if payload is None or partition_key is None:
                    continue
                
                current_chunk.append({
                    'Data': payload.encode('utf-8') if isinstance(payload, str) else payload,
                    'PartitionKey': str(partition_key)
                })
                
                if len(current_chunk) >= 500:
                    futures.append(executor.submit(self._send_prepared_chunk, client, current_chunk))
                    current_chunk = []
            
            if current_chunk:
                futures.append(executor.submit(self._send_prepared_chunk, client, current_chunk))
            
            if not futures:
                return KinesisCommitMessage(0, 0)
            
            total_sent, total_failed = 0, 0
            for future in as_completed(futures):
                try:
                    sent, failed = future.result()
                    total_sent += sent
                    total_failed += failed
                except Exception as e:
                    logger.error(f"Error sending chunk: {e}")
        
        return KinesisCommitMessage(total_sent, total_failed)
    
    def _send_prepared_chunk(self, client, kinesis_records) -> Tuple[int, int]:
        """Send pre-formatted Kinesis records (≤500 records)."""
        try:
            response = client.put_records(StreamName=self.stream_name, Records=kinesis_records)
            failed = response.get('FailedRecordCount', 0)
            sent = len(kinesis_records) - failed
            
            if failed > 0 and 'Records' in response:
                error_codes = {}
                for rec in response['Records']:
                    if 'ErrorCode' in rec:
                        error_codes[rec['ErrorCode']] = error_codes.get(rec['ErrorCode'], 0) + 1
                if error_codes:
                    logger.error(f"Kinesis errors: {error_codes}")
            
            return (sent, failed)
        except Exception as e:
            logger.error(f"Exception in put_records: {type(e).__name__}: {e}")
            return (0, len(kinesis_records))
    
    def commit(self, messages, batchId):
        """Handle successful batch commit."""
        total_sent = sum(m.sent_count for m in messages)
        total_failed = sum(m.failed_count for m in messages)
        
        if total_failed > 0:
            logger.warning(f"Batch {batchId}: {total_sent:,} sent, {total_failed:,} failed")
        else:
            logger.info(f"Batch {batchId}: {total_sent:,} sent")
    
    def abort(self, messages, batchId):
        """Handle batch failure."""
        logger.error(f"Batch {batchId} aborted")
    
    def _create_kinesis_client(self):
        """Create boto3 Kinesis client with connection pooling and credential handling."""
        import boto3
        from botocore.config import Config
        
        client_config = Config(max_pool_connections=50, tcp_keepalive=True)
        
        if self.service_credential:
            try:
                from databricks.service_credentials import getServiceCredentialsProvider
                credential_provider = getServiceCredentialsProvider(self.service_credential)
                session = boto3.Session(botocore_session=credential_provider)
                return session.client('kinesis', region_name=self.region, config=client_config)
            except Exception as e:
                raise RuntimeError(f"Service credential '{self.service_credential}' failed: {e}")
        
        elif self.aws_access_key and self.aws_secret_key:
            client_kwargs = {
                'region_name': self.region,
                'aws_access_key_id': self.aws_access_key,
                'aws_secret_access_key': self.aws_secret_key,
                'config': client_config
            }
            if self.aws_session_token:
                client_kwargs['aws_session_token'] = self.aws_session_token
            return boto3.client('kinesis', **client_kwargs)
        
        else:
            return boto3.client('kinesis', region_name=self.region, config=client_config)


class KinesisCommitMessage(WriterCommitMessage):
    """Commit message containing send statistics."""
    
    def __init__(self, sent_count: int, failed_count: int):
        self.sent_count = sent_count
        self.failed_count = failed_count
