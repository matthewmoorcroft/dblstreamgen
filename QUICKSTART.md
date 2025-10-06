# dblstreamgen - Quick Start Guide

Get up and running with `dblstreamgen` in minutes!

## Installation

### 1. Install the library

```bash
# From the project root
cd /path/to/dblstreamgen

# Install in development mode
pip install -e .

# Or install with dependencies
pip install -e ".[all]"
```

### 2. Install dependencies

```bash
# Required
pip install pyyaml boto3

# For future features
pip install pyspark dbldatagen confluent-kafka fastavro
```

## Quick Test (5 minutes)

### Step 1: Configure AWS Kinesis

Edit `examples/config_source_kinesis.yaml`:

```yaml
kinesis_config:
  stream_name: "your-stream-name"  # ← Change this
  region: "us-east-1"
  aws_access_key_id: "YOUR_KEY"    # ← Add your credentials
  aws_secret_access_key: "YOUR_SECRET"
```

**Alternative**: Use AWS credentials from environment variables or IAM role by removing the `aws_access_key_id` and `aws_secret_access_key` lines.

### Step 2: Run the test

```bash
python examples/simple_test.py
```

You should see:
```
======================================================================
dblstreamgen - Simple Test
======================================================================

Loading configuration...
✓ Configuration loaded
  - Event types: 3
  - Target rate: 100 events/sec
  - Kinesis stream: your-stream-name

Creating generator...
✓ StreamGenerator created

Creating publisher...
✓ KinesisPublisher created

Starting generation (duration: 60s)...
----------------------------------------------------------------------
  Progress: 1,000 events sent (100 events/sec)
  Progress: 2,000 events sent (100 events/sec)
  ...
```

### Step 3: Verify events in Kinesis

Check AWS Console or use CLI:

```bash
aws kinesis get-records \
  --shard-iterator $(aws kinesis get-shard-iterator \
    --stream-name your-stream-name \
    --shard-id shardId-000000000000 \
    --shard-iterator-type LATEST \
    --query 'ShardIterator' --output text) \
  --limit 10
```

## Usage in Your Code

### Basic Example

```python
import dblstreamgen
import time

# Load configuration
config = dblstreamgen.load_config(
    'examples/config_generation.yaml',
    'examples/config_source_kinesis.yaml'
)

# Create generator and publisher
generator = dblstreamgen.StreamGenerator(config)
publisher = dblstreamgen.KinesisPublisher(config['kinesis_config'])

# Generate for 5 minutes
start_time = time.time()
batch_buffer = []

for event in generator.generate():
    batch_buffer.append(event)
    
    # Publish in batches of 100
    if len(batch_buffer) >= 100:
        result = publisher.publish_batch(batch_buffer)
        print(f"Sent {result.records_sent} events")
        batch_buffer = []
    
    # Stop after 5 minutes
    if time.time() - start_time > 300:
        generator.stop()
        break

# Cleanup
publisher.close()
```

### Custom Configuration

Create your own config files:

```yaml
# my_config_generation.yaml
event_types:
  - event_type_id: "user.signup"
    weight: 0.10  # 10% of events
    avg_payload_kb: 2
    fields:
      email:
        type: "string"
        values: ["user1@test.com", "user2@test.com"]
      source:
        type: "string"
        values: ["web", "mobile", "api"]
        weights: [0.5, 0.3, 0.2]
  
  - event_type_id: "user.login"
    weight: 0.90  # 90% of events
    avg_payload_kb: 1
    fields:
      session_id:
        type: "uuid"
      ip_address:
        type: "string"
        values: ["192.168.1.1", "10.0.0.1"]

test_execution:
  base_throughput_per_sec: 500  # 500 events/sec
```

Then use it:

```python
config = dblstreamgen.load_config('my_config_generation.yaml', 'config_source_kinesis.yaml')
generator = dblstreamgen.StreamGenerator(config)
```

## Customization

### Adjust Throughput

Edit `config_generation.yaml`:

```yaml
test_execution:
  base_throughput_per_sec: 1000  # Increase to 1000 events/sec
```

### Add Event Types

Add more event types to `event_types` list:

```yaml
event_types:
  - event_type_id: "player.achievement.unlock"
    weight: 0.02  # 2% of events
    avg_payload_kb: 4
    fields:
      achievement_id:
        type: "int"
        range: [1, 100]
      timestamp:
        type: "timestamp"
```

### Field Types Supported

- `uuid`: Random UUID string
- `string`: Pick from values list (with optional weights)
- `int`: Random integer in range
- `float`: Random float in range
- `timestamp`: ISO timestamp string

## Troubleshooting

### Import Error: No module named 'dblstreamgen'

```bash
# Make sure you installed the package
pip install -e .

# Or add to PYTHONPATH
export PYTHONPATH="/path/to/dblstreamgen/src:$PYTHONPATH"
```

### Import Error: No module named 'boto3'

```bash
pip install boto3
```

### AWS Credentials Error

```bash
# Set environment variables
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"

# Or use AWS CLI to configure
aws configure
```

### Kinesis Stream Not Found

Make sure the stream exists:

```bash
aws kinesis describe-stream --stream-name your-stream-name
```

Or create it:

```bash
aws kinesis create-stream \
  --stream-name test-game-events-stream \
  --shard-count 1
```

## Next Steps

- **Add more event types**: Edit `config_generation.yaml`
- **Increase throughput**: Adjust `base_throughput_per_sec`
- **Run longer tests**: Modify `duration_seconds` in the example script
- **Add BatchGenerator**: See `docs/agent_context/EXTENSION_GUIDE.md`
- **Add Kafka support**: See `docs/agent_context/EXTENSION_GUIDE.md`
- **Add rate variance**: See `docs/agent_context/EXTENSION_GUIDE.md`

## Need Help?

- Check `README.md` for full documentation
- See `docs/agent_context/TECHNICAL_SPECIFICATION.md` for architecture details
- See `examples/simple_test.py` for a complete working example

Happy testing! 🚀

