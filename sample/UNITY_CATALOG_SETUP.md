# Unity Catalog Volumes Setup for dblstreamgen

Quick reference for using Unity Catalog volumes with dblstreamgen.

## Why Unity Catalog Volumes?

Unity Catalog volumes are the modern, recommended way to store files in Databricks:

✅ **Governance**: Full Unity Catalog permissions and lineage  
✅ **Security**: Catalog-level access controls  
✅ **Organization**: Structured by catalog/schema  
✅ **Future-proof**: Recommended over DBFS  

## Quick Setup

### Step 1: Create Volumes

```sql
-- In a Databricks SQL notebook
USE CATALOG <your_catalog>;
USE SCHEMA <your_schema>;

-- Create volumes
CREATE VOLUME IF NOT EXISTS libraries
COMMENT 'Python wheels and shared libraries';

CREATE VOLUME IF NOT EXISTS config
COMMENT 'Configuration files';

-- Verify
SHOW VOLUMES;
```

Expected output:
```
volume_catalog | volume_schema | volume_name | volume_type | owner
---------------|---------------|-------------|-------------|-------
your_catalog   | your_schema   | libraries   | MANAGED     | you
your_catalog   | your_schema   | config      | MANAGED     | you
```

### Step 2: Upload Files

**Using Databricks CLI:**

```bash
# Set your catalog and schema
CATALOG="your_catalog"
SCHEMA="your_schema"

# Upload wheel
databricks fs cp dist/dblstreamgen-0.1.0-py3-none-any.whl \
  /Volumes/$CATALOG/$SCHEMA/libraries/dblstreamgen-0.1.0-py3-none-any.whl

# Upload configs
databricks fs cp sample/configs/config_generation.yaml \
  /Volumes/$CATALOG/$SCHEMA/config/config_generation.yaml
  
databricks fs cp sample/configs/config_source_kinesis.yaml \
  /Volumes/$CATALOG/$SCHEMA/config/config_source_kinesis.yaml

# Verify uploads
databricks fs ls /Volumes/$CATALOG/$SCHEMA/libraries/
databricks fs ls /Volumes/$CATALOG/$SCHEMA/config/
```

**Using Databricks UI:**

1. Go to **Catalog** → Your Catalog → Your Schema
2. Click on **Volumes** tab
3. Click on `libraries` volume
4. Click **Upload** button
5. Select `dblstreamgen-0.1.0-py3-none-any.whl`
6. Repeat for `config` volume with YAML files

### Step 3: Use in Notebooks

```python
# Install from Unity Catalog volume
%pip install /Volumes/<catalog>/<schema>/libraries/dblstreamgen-0.1.0-py3-none-any.whl

# Load config from Unity Catalog volume
import dblstreamgen

config = dblstreamgen.load_config(
    '/Volumes/<catalog>/<schema>/config/config_generation.yaml',
    '/Volumes/<catalog>/<schema>/config/config_source_kinesis.yaml'
)
```

## Path Format

Unity Catalog volume paths follow this pattern:

```
/Volumes/<catalog_name>/<schema_name>/<volume_name>/<path>
```

Examples:
```python
# Wheel
/Volumes/main/dblstreamgen/libraries/dblstreamgen-0.1.0-py3-none-any.whl

# Config
/Volumes/main/dblstreamgen/config/config_generation.yaml

# Any file
/Volumes/main/dblstreamgen/config/custom/my_config.yaml
```

## Permissions

Grant read access to users/groups:

```sql
-- Grant USAGE on catalog and schema
GRANT USAGE ON CATALOG <catalog> TO `user@company.com`;
GRANT USAGE ON SCHEMA <catalog>.<schema> TO `user@company.com`;

-- Grant READ on volumes
GRANT READ VOLUME ON VOLUME <catalog>.<schema>.libraries TO `user@company.com`;
GRANT READ VOLUME ON VOLUME <catalog>.<schema>.config TO `user@company.com`;

-- For groups
GRANT READ VOLUME ON VOLUME <catalog>.<schema>.libraries TO `data-engineers`;
```

## Managing Files

### List files in volume

```bash
# Using CLI
databricks fs ls /Volumes/<catalog>/<schema>/libraries/

# Using Python in notebook
dbutils.fs.ls("/Volumes/<catalog>/<schema>/libraries/")
```

### Download file from volume

```bash
databricks fs cp /Volumes/<catalog>/<schema>/config/config_generation.yaml ./config_generation.yaml
```

### Delete file from volume

```bash
databricks fs rm /Volumes/<catalog>/<schema>/libraries/old_wheel.whl
```

### Update config file

```bash
# Download
databricks fs cp /Volumes/<catalog>/<schema>/config/config_generation.yaml ./config_generation.yaml

# Edit locally
nano config_generation.yaml

# Upload back (overwrites)
databricks fs cp ./config_generation.yaml /Volumes/<catalog>/<schema>/config/config_generation.yaml
```

## Example Notebook Setup

```python
# Databricks notebook source

# COMMAND ----------
# DBTITLE 1,Configuration
CATALOG = "main"              # Your catalog
SCHEMA = "dblstreamgen"       # Your schema
VOLUME_LIBS = "libraries"     # Volume for wheels
VOLUME_CONFIG = "config"      # Volume for configs

# Build paths
WHEEL_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_LIBS}/dblstreamgen-0.1.0-py3-none-any.whl"
GEN_CONFIG = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_CONFIG}/config_generation.yaml"
SRC_CONFIG = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_CONFIG}/config_source_kinesis.yaml"

print(f"Wheel: {WHEEL_PATH}")
print(f"Generation config: {GEN_CONFIG}")
print(f"Source config: {SRC_CONFIG}")

# COMMAND ----------
# DBTITLE 1,Install
%pip install {WHEEL_PATH}

# COMMAND ----------
# DBTITLE 1,Load and Run
import dblstreamgen

config = dblstreamgen.load_config(GEN_CONFIG, SRC_CONFIG)
generator = dblstreamgen.StreamGenerator(config)
publisher = dblstreamgen.KinesisPublisher(config['kinesis_config'])

# ... your generation logic ...
```

## Comparing Paths

| Location | Unity Catalog Volume | DBFS (Legacy) | Workspace Repos |
|----------|---------------------|---------------|-----------------|
| Wheel | `/Volumes/main/dblstreamgen/libraries/dblstreamgen-0.1.0-py3-none-any.whl` | `/dbfs/libraries/dblstreamgen-0.1.0-py3-none-any.whl` | `/Workspace/Repos/<user>/dblstreamgen/dist/dblstreamgen-0.1.0-py3-none-any.whl` |
| Config | `/Volumes/main/dblstreamgen/config/config_generation.yaml` | `/dbfs/configs/dblstreamgen/config_generation.yaml` | `/Workspace/Repos/<user>/dblstreamgen/sample/configs/config_generation.yaml` |

**Recommendation**: Use Unity Catalog volumes for production, Workspace Repos for development.

## Troubleshooting

### "Volume not found"

```sql
-- Check if volume exists
SHOW VOLUMES IN <catalog>.<schema>;

-- Create if missing
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.libraries;
```

### "Permission denied"

```sql
-- Check your permissions
SHOW GRANT ON VOLUME <catalog>.<schema>.libraries;

-- Request access from admin
```

### "File not found"

```bash
# List files to verify path
databricks fs ls /Volumes/<catalog>/<schema>/libraries/

# Check full path matches
databricks fs cat /Volumes/<catalog>/<schema>/config/config_generation.yaml
```

## Best Practices

1. **Organize by purpose**: Use separate volumes for libraries, configs, data, etc.
2. **Version your wheels**: Include version in filename
3. **Use consistent naming**: Follow your org's naming conventions
4. **Document structure**: Add VOLUME comments explaining purpose
5. **Grant minimal permissions**: Only give READ access where needed
6. **Keep configs in version control**: Edit locally, then upload

## Migration from DBFS

If you're currently using DBFS:

```bash
# Copy from DBFS to Unity Catalog volume
databricks fs cp -r \
  dbfs:/libraries/ \
  /Volumes/<catalog>/<schema>/libraries/

# Verify
databricks fs ls /Volumes/<catalog>/<schema>/libraries/

# Update notebook paths
# Change: /dbfs/libraries/file.whl
# To: /Volumes/<catalog>/<schema>/libraries/file.whl
```

---

**Summary**: Unity Catalog volumes provide better governance and are the modern standard for file storage in Databricks. Use them instead of DBFS for new projects!


