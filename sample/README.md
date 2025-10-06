# Sample Databricks Setup

This folder contains example configurations and notebooks for running `dblstreamgen` in Databricks.

## 📁 Folder Structure

```
sample/
├── README.md                    # This file
├── configs/
│   ├── config_generation.yaml   # Event schemas
│   └── config_source_kinesis.yaml  # Kinesis config (with Databricks secrets)
└── notebooks/
    ├── 01_install_and_test.py   # Installation & quick test
    ├── 02_stream_generator.py   # StreamGenerator example
    └── 03_batch_generator.py    # BatchGenerator example (future)
```

## 🚀 Quick Start in Databricks

### Step 1: Create Unity Catalog Volume (Recommended)

```sql
-- Create a volume for libraries
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.libraries
COMMENT 'Shared libraries and wheels';

-- Create a volume for configs
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.config
COMMENT 'Configuration files';
```

### Step 2: Upload the Wheel

1. Build the wheel (if not already built):
   ```bash
   cd /path/to/dblstreamgen
   python -m build
   ```

2. Upload to Unity Catalog Volume:
   - Option A: Use Databricks CLI
     ```bash
     databricks fs cp dist/dblstreamgen-0.1.0-py3-none-any.whl \
       /Volumes/<catalog>/<schema>/libraries/dblstreamgen-0.1.0-py3-none-any.whl
     ```
   - Option B: Use Workspace Repos (for development)
   - Option C: Upload via UI → Catalog → Volumes → libraries → Upload

### Step 3: Upload Configs

Upload the `configs/` folder to Unity Catalog Volume:
```bash
databricks fs cp configs/config_generation.yaml \
  /Volumes/<catalog>/<schema>/config/config_generation.yaml
databricks fs cp configs/config_source_kinesis.yaml \
  /Volumes/<catalog>/<schema>/config/config_source_kinesis.yaml
```

**Important**: Edit `config_source_kinesis.yaml` and add your AWS credentials using Databricks secrets!

### Step 4: Create Databricks Secrets (Recommended)

```bash
# Create secret scope
databricks secrets create-scope --scope dblstreamgen

# Add AWS credentials
databricks secrets put --scope dblstreamgen --key aws-access-key
databricks secrets put --scope dblstreamgen --key aws-secret-key
```

Then in `config_source_kinesis.yaml`:
```yaml
kinesis_config:
  aws_access_key_id: "{{secrets/dblstreamgen/aws-access-key}}"
  aws_secret_access_key: "{{secrets/dblstreamgen/aws-secret-key}}"
```

### Step 5: Run Notebooks

1. Import notebooks from `notebooks/` folder
2. Update paths to use your Unity Catalog volume paths
3. Run `01_install_and_test.py` first to verify installation
4. Then run `02_stream_generator.py` to generate events

### Step 4: Run Notebooks

1. Import notebooks from `notebooks/` folder
2. Run `01_install_and_test.py` first to verify installation
3. Then run `02_stream_generator.py` to generate events

## 📝 Notes

- This `sample/` folder is in `.gitignore` - customize freely for your environment
- Update AWS credentials and Kinesis stream names in configs
- StreamGenerator works in any Databricks notebook
- BatchGenerator requires a Spark cluster (coming soon)

## 🔐 Security Best Practices

**Never commit credentials!**

Use Databricks secrets:
```python
# In notebooks
dbutils.secrets.get(scope="dblstreamgen", key="aws-access-key")
```

Or update config to reference secrets:
```yaml
aws_access_key_id: "{{secrets/dblstreamgen/aws-access-key}}"
```

The library will automatically resolve `{{secrets/...}}` references.

