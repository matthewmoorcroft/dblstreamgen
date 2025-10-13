# Scale Testing Framework for Streaming Pipelines

**A strategic guide for validating streaming pipeline capacity and cluster sizing using dblstreamgen**

---

## Table of Contents

1. [Introduction & Purpose](#introduction--purpose)
2. [Test Design Framework](#test-design-framework)
3. [Key Metrics to Monitor](#key-metrics-to-monitor)
4. [Decision Framework](#decision-framework)
5. [Testing Methodology with dblstreamgen](#testing-methodology-with-dblstreamgen)
6. [Cluster Sizing Guidelines](#cluster-sizing-guidelines)
7. [Red Flags and Warning Signs](#red-flags-and-warning-signs)
8. [Best Practices](#best-practices)
9. [References](#references)

---

## Introduction & Purpose

### Why Scale Testing Matters

Streaming pipelines in production face unpredictable load patterns, data skew, and resource constraints. Without proper scale testing, you risk:

- **Pipeline failures** during traffic spikes
- **Growing backlogs** that never recover
- **Cost overruns** from over-provisioned clusters
- **Latency violations** impacting downstream systems
- **Data loss** when buffers overflow

Scale testing provides the empirical data needed to make informed decisions about cluster sizing, pipeline architecture, and operational thresholds.

### The Role of dblstreamgen

`dblstreamgen` serves as a **controlled synthetic load generator** that enables you to:

- Generate realistic event distributions matching your production schema
- Simulate various load patterns (baseline, peak, burst)
- Test with configurable throughput rates
- Validate pipeline behavior before production deployment
- Establish capacity baselines for cluster sizing decisions

**Key advantage**: Unlike production traffic, dblstreamgen provides repeatable, controllable load patterns that isolate pipeline performance from upstream system variability.

### Target Audience

This guide is designed for:

- **Data Engineers** designing and sizing streaming pipelines
- **Platform Engineers** establishing cluster standards and cost models
- **DevOps Teams** setting up monitoring and alerting thresholds
- **Architects** making decisions about pipeline splitting and scaling strategies

---

## Test Design Framework

Effective scale testing requires multiple test types, each revealing different aspects of pipeline behavior. Design your testing strategy to include all five categories:

### 1. Baseline Testing

**Objective**: Establish normal operating characteristics

**Load Pattern**: 50-75% of expected production throughput

**Duration**: 2-4 hours

**What to Measure**:
- Steady-state resource utilization (CPU, memory, network)
- Processing rate stability over time
- Batch/trigger duration consistency
- Cost per million events processed

**Success Criteria**:
- Processing rate consistently exceeds input rate
- No scheduling delay accumulation
- Resource utilization stable (no gradual increase)
- No memory leaks or GC pressure over time

**Purpose**: This establishes your baseline metrics. Any deviation from these values during peak or burst testing indicates capacity issues.

---

### 2. Peak Load Testing

**Objective**: Validate sustained maximum throughput capacity

**Load Pattern**: 100-150% of expected peak production load

**Duration**: 4-8 hours

**What to Measure**:
- Maximum sustainable processing rate
- Point where scheduling delay begins accumulating
- Resource saturation patterns (CPU, memory, I/O)
- State size growth for stateful operations

**Success Criteria**:
- Pipeline maintains processing rate ≥ input rate for entire duration
- Scheduling delay remains bounded (<30 seconds)
- Resource utilization <80% to allow headroom
- No backlog accumulation

**Purpose**: Identifies your pipeline's true capacity ceiling and validates that your cluster can handle peak production loads with headroom for bursts.

---

### 3. Burst Testing

**Objective**: Validate recovery from traffic spikes

**Load Pattern**: 
- Baseline load for 30 minutes
- 2-5x spike for 10-15 minutes
- Return to baseline
- Observe recovery

**Duration**: 2-3 hours with multiple burst cycles

**What to Measure**:
- Backlog accumulation during burst
- Recovery time to clear backlog
- Maximum scheduling delay during burst
- Resource behavior during spike (OOM risks)

**Success Criteria**:
- Pipeline survives burst without failure
- Backlog clears within acceptable time (2-3x burst duration)
- No OOM or executor failures
- Returns to baseline performance after recovery

**Purpose**: Real-world traffic is bursty. This validates that temporary spikes don't cause cascading failures and that your pipeline can recover.

---

### 4. Soak Testing

**Objective**: Validate long-term stability under sustained load

**Load Pattern**: 80-100% of peak capacity

**Duration**: 12-24 hours minimum (ideally 48-72 hours)

**What to Measure**:
- Memory leak detection (gradual increase in heap usage)
- State size growth patterns
- GC frequency and duration over time
- Checkpoint performance degradation
- Disk space consumption trends

**Success Criteria**:
- Memory usage stabilizes (no unbounded growth)
- GC time remains <10% of execution time
- Checkpoint durations remain stable
- No resource exhaustion over time

**Purpose**: Many issues only emerge after hours of continuous operation (memory leaks, state bloat, checkpoint corruption). Soak testing catches these before production.

---

### 5. Degradation Testing

**Objective**: Understand behavior under resource constraints

**Load Pattern**: Baseline load with induced constraints:
- Reduced executor count
- Smaller instance types
- Network throttling
- Deliberately limited memory

**Duration**: 1-2 hours per configuration

**What to Measure**:
- Graceful degradation vs catastrophic failure
- Impact on processing rate and latency
- Recovery behavior when constraints removed
- Error rates and data loss risks

**Success Criteria**:
- Pipeline degrades gracefully (slower processing, not failures)
- No data loss or corruption
- Recovers when resources restored
- Monitoring clearly indicates degraded state

**Purpose**: Production environments experience resource contention, spot instance reclamation, and infrastructure failures. Understanding degradation behavior helps you design resilient pipelines and appropriate alerting.

---

## Key Metrics to Monitor

Effective scale testing requires monitoring both **streaming pipeline metrics** (application-level) and **cluster metrics** (infrastructure-level). The combination reveals where bottlenecks exist and guides optimization decisions.

### Streaming Pipeline Metrics

#### For Spark Structured Streaming

**Where to Find**: Query progress events, Spark UI Streaming tab, streaming query metrics

| Metric | What It Measures | Healthy Range | Red Flag |
|--------|------------------|---------------|----------|
| **Input Rate** | Records/sec arriving from source | Match expected load | Suddenly drops to zero |
| **Processing Rate** | Records/sec actually processed | ≥ Input Rate | < Input Rate sustained |
| **Batch Duration** | Time to process one trigger | <80% of trigger interval | > trigger interval |

**Processing Rate vs Input Rate**:
   ```
   Healthy:     Input=1000/s, Processing=1200/s (20% headroom)
   Unhealthy:   Input=1000/s, Processing=800/s (backlog accumulates)
   ```

---

#### For Spark Declarative Pipelines (SDP)

**Where to Find**: SDP Event Log, SDP Pipeline UI, system tables

| Metric | What It Measures | Healthy Range | Red Flag |
|--------|------------------|---------------|----------|
| **Flow Backlog** | Records waiting to be processed | Stable or decreasing | Continuously increasing |
| **Backlog Bytes** | Data volume in backlog | <1GB for streaming | Growing >10GB |



**SDP-Specific Considerations**:

- **Continuous vs Triggered**: Continuous mode behaves like Structured Streaming; Triggered mode processes available data then stops
- **Auto-scaling**: SDP can auto-scale clusters, but monitor to ensure it scales appropriately
- **Expectations**: SDP data quality expectations add processing overhead; factor into capacity planning


---

### Cluster Metrics

**Where to Find**: Databricks Metrics UI, Ganglia (legacy), Spark UI, system tables

#### CPU Utilization

| Metric | What It Measures | Healthy Range | Red Flag |
|--------|------------------|---------------|----------|
| **Driver CPU** | CPU usage on driver node | 20-60% | >80% sustained |
| **Executor CPU (avg)** | Average across all executors | 50-80% | >90% sustained or <20% |
| **CPU Skew** | Variance across executors | Low variance | Some executors 90%+, others <20% |

**Interpretation**:
- **High CPU across all executors**: Scale out (add workers) or optimize code
- **High CPU on driver only**: Driver bottleneck, scale up driver or reduce coordination overhead
- **High CPU skew**: Data skew issue, repartition or adjust partition keys

---

#### Memory Usage

| Metric | What It Measures | Healthy Range | Red Flag |
|--------|------------------|---------------|----------|
| **GC Time %** | Time spent in garbage collection | <5% | >10% |
| **Memory per Task** | Memory usage per Spark task | Stable | High variance or OOM |
| **State Store Size** | Memory for stateful streaming | Bounded growth | Unbounded growth |

**Interpretation**:
- **High memory + high GC**: Scale up (larger instances) or reduce batch size
- **OOM on executors**: Increase executor memory or reduce parallelism per executor
- **OOM on driver**: Scale up driver or reduce shuffle partition count
- **State store growth**: Review stateful operation windows or add TTL

**GC Pressure Example**:
```
Good:  GC time = 0.5s out of 10s batch = 5% GC time
Bad:   GC time = 3.2s out of 10s batch = 32% GC time (too high)
```


---

### Accessing Cluster Metrics Via Databricks

1. Cluster page → Metrics tab
2. Spark UI → Executors tab (memory, CPU per executor)
3. Spark UI → Streaming tab (streaming metrics)
4. Spark UI → Stages tab (task-level metrics)


---

## Decision Framework

The metrics above reveal *what* is happening. This section provides the decision framework for *what to do about it*.

### The Three Scaling Strategies

```
┌─────────────────────────────────────────────────────────────┐
│                    Scaling Strategy                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1. Scale OUT     → Add more workers                        │
│                     (Horizontal scaling)                    │
│                                                              │
│  2. Scale UP      → Use larger instance types               │
│                     (Vertical scaling)                      │
│                                                              │
│  3. Split PIPELINE → Separate into multiple pipelines       │
│                     (Architectural change)                  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

### When to Scale OUT (Add Workers)

**Indicators**:
- High CPU utilization across **all** executors (>80%)
- Good task parallelism (many small tasks)
- Processing rate scales linearly with executor count
- Memory per executor is stable (<70% utilized)

**Pattern Recognition**:
```
Executor Metrics (all similar):
  Executor 1: CPU 85%, Memory 65%, Tasks: 120
  Executor 2: CPU 87%, Memory 62%, Tasks: 118
  Executor 3: CPU 84%, Memory 68%, Tasks: 122
  Executor 4: CPU 86%, Memory 64%, Tasks: 119

Diagnosis: CPU-bound, well-distributed → Scale OUT
```

---

### When to Scale UP (Larger Instance Types)

**Indicators**:
- Driver memory pressure or OOM errors
- High GC time (>10%) on executors or driver
- Single-task performance bottlenecks (tasks not parallelizable)
- Memory-intensive stateful operations (large windows, aggregations)
- Shuffle spill to disk

**Pattern Recognition**:
```
Driver Metrics:
  Memory: 28GB / 32GB (87% utilized)
  GC Time: 18% of execution time
  OOM errors in logs
  
Executor Metrics:
  CPU: 45-55% (underutilized)
  Memory: High variance across tasks
  Frequent shuffle spill to disk

Diagnosis: Memory-bound → Scale UP or Throttle Ingestion
```

---

### When to SPLIT the Pipeline

**Indicators**:
- Mixed workload characteristics (some stages CPU-bound, others memory-bound)
- Different latency requirements per table/stage
- Backlog isolation needs (hot path vs cold path)
- Need for different scaling profiles per stage
- Cost optimization opportunity (heterogeneous instance types)
- Cannot resolve with scaling out/up alone
- **Fan-out table explosion** (writing to 50+ tables in one pipeline overwhelming driver)
- Driver CPU/memory saturation due to managing too many concurrent streaming queries

**Pattern Recognition**:
```
Pipeline Stages:
  Stage 1 (Ingest + Parse):     CPU 85%, Memory 45% → Wants scale OUT
  Stage 2 (Enrich):             CPU 60%, Memory 80% → Wants scale UP
  Stage 3 (Aggregate 1-hour):   CPU 40%, Memory 90% → Wants large memory
  Stage 4 (Write to warehouse): CPU 70%, Memory 50% → Different profile

Diagnosis: Cannot optimize single cluster for all stages → SPLIT

Fan-Out Pattern:
  Single pipeline writing to 100+ tables
  Driver CPU: 95% (managing streaming queries)
  Driver Memory: 88% (query coordination overhead)
  Executors: Underutilized at 40-50% CPU
  Symptoms: Slow checkpointing, query coordination delays

Diagnosis: Driver overwhelmed by managing too many streams → SPLIT by table groups
```

**Typical Scenarios**:
- **Hot/Cold Path Pattern**: Real-time alerts (low latency) + historical aggregations (high latency OK)
- **Different SLAs**: Critical data vs best-effort analytics
- **Compute vs Memory Stages**: Parsing (CPU) → Enrichment (memory) → Aggregation (memory)
- **Cost Optimization**: Use spot instances for non-critical paths, on-demand for critical
- **Blast Radius Reduction**: Isolate risky transformations from stable pipelines
- **Fan-Out Table Explosion**: Single source fanning out to 50-200+ tables overwhelming driver coordination and checkpoint management

**Architecture Patterns**:

**Pattern 1: Hot/Cold Split**
```
Source → [Pipeline 1: Real-time Alerts] → Alert Destination
      → [Pipeline 2: Historical Agg]   → Data Warehouse
```

**Pattern 2: Stage-based Split**
```
Source → [Pipeline 1: Ingest + Parse]  → Bronze (Delta)
Bronze → [Pipeline 2: Enrich]          → Silver (Delta)
Silver → [Pipeline 3: Aggregate]       → Gold (Delta)
```

**Pattern 3: Event Type Split**
```
Source → [Pipeline 1: High-volume events] → Destination A
      → [Pipeline 2: Low-volume events]  → Destination B
```

**Pattern 4: Fan-Out Split (Table Explosion Prevention)**
```
Single Source with 150 event types → Originally: One pipeline → 150 tables (driver overwhelmed)

Split Approach:
Source → [Pipeline 1: Event types 1-50]   → Tables 1-50
      → [Pipeline 2: Event types 51-100]  → Tables 51-100
      → [Pipeline 3: Event types 101-150] → Tables 101-150

Each pipeline manages ~50 tables, keeping driver coordination manageable
```


### Decision Tree

```
Start: Pipeline not keeping up with load
  │
  ├─→ Check: Backlog growing?
  │     │
  │     NO → Pipeline is healthy, no action needed
  │     │
  │     YES → Continue to next check
  │
  ├─→ Check: What resource is saturated?
  │     │
  │     ├─→ CPU >80% across all executors?
  │     │     │
  │     │     YES → SCALE OUT (add workers)
  │     │     │
  │     │     Test: Does throughput increase linearly?
  │     │       │
  │     │       YES → Continue scaling out
  │     │       NO  → Check for data skew or other bottleneck
  │     │
  │     ├─→ Memory >85% or High GC (>10%)?
  │     │     │
  │     │     YES → SCALE UP (larger instances)
  │     │     │
  │     │     Test: Does GC time drop and throughput increase?
  │     │       │
  │     │       YES → Continue with larger instances
  │     │       NO  → Review code for memory leaks or inefficient operations
  │     │
  │     ├─→ Network saturated?
  │     │     │
  │     │     YES → Optimize shuffle (repartitioning, broadcast joins)
  │     │           Or SCALE OUT with better network instances
  │     │
  │     ├─→ Mixed symptoms (some stages CPU, some memory)?
  │     │     │
  │     │     YES → Consider SPLIT PIPELINE
  │     │
  │     └─→ No clear resource bottleneck?
  │           │
  │           Check for:
  │           - Data skew (straggler tasks)
  │           - Source throughput limits
  │           - Downstream sink bottlenecks
  │           - Code inefficiencies (expensive UDFs)
```

---

### Real-World Decision Examples

**Example 1: E-commerce Clickstream**

**Symptoms**:
- Input rate: 15,000 events/sec
- Processing rate: 8,000 events/sec
- CPU: 90% across all executors
- Memory: 50% across all executors
- GC: 4%

**Decision**: **SCALE OUT**
- CPU-bound, well-distributed load
- Added 4 workers (4 → 8)
- Result: Processing rate increased to 14,000 events/sec
- CPU dropped to 75%

---

**Example 2: IoT Sensor Data with 24-hour Windows**

**Symptoms**:
- Input rate: 5,000 events/sec
- Processing rate: 3,000 events/sec
- Memory: 85% across executors
- GC time: 15%
- Frequent OOM errors
- CPU: 55%
- State store size: Growing continuously

**Decision**: **SCALE UP**
- Memory-bound due to large stateful windows
- Changed from m5.2xlarge (32GB) to r5.4xlarge (128GB memory-optimized)
- Result: Processing rate increased to 5,500 events/sec
- GC time dropped to 3%, no OOM errors
- State store size stabilized

---

**Example 3: Multi-Stage Analytics Pipeline**

**Symptoms**:
- Stage 1 (Ingest): CPU 90%, Memory 40%
- Stage 2 (Enrich): CPU 50%, Memory 85%
- Stage 3 (Aggregate): CPU 30%, Memory 95%, large windows
- Overall throughput suboptimal
- Cannot tune cluster for all stages

**Decision**: **SPLIT PIPELINE**
- Created 3 separate SDP pipelines
- Pipeline 1 (Ingest): 8 x m5.2xlarge (CPU-optimized, scale out)
- Pipeline 2 (Enrich): 4 x m5.4xlarge (balanced)
- Pipeline 3 (Aggregate): 2 x r5.4xlarge (memory-optimized)
- Result: 30% cost reduction, 40% throughput improvement
- Each pipeline independently optimized and monitored

---

**Example 4: Fan-Out to 180 Event Type Tables**

**Symptoms**:
- Single SDP pipeline reading from Kinesis
- Fan-out to 180 Delta tables (one per event type)
- Input rate: 3,000 events/sec (modest)
- Driver CPU: 92% (managing 180 streaming queries)
- Driver Memory: 87% (checkpoint coordination overhead)
- Executor CPU: Only 35-40% (underutilized)
- Checkpoint operations taking 2-3 minutes
- Frequent "Cannot allocate memory" driver errors
- Processing lag growing despite low throughput

**Decision**: **SPLIT PIPELINE BY TABLE GROUPS**
- Split into 4 separate SDP pipelines
- Pipeline 1: Event types 1-45 (45 tables)
- Pipeline 2: Event types 46-90 (45 tables)
- Pipeline 3: Event types 91-135 (45 tables)
- Pipeline 4: Event types 136-180 (45 tables)
- Each pipeline: 1 x m5.xlarge driver + 4 x m5.xlarge workers
- Result:
  - Driver CPU per pipeline: 60-65% (healthy)
  - Checkpoint time: 15-20 seconds (down from 2-3 minutes)
  - No more driver memory errors
  - Processing rate: 3,500 events/sec (increased despite same total compute)
  - Total cost: Similar (4 smaller pipelines vs 1 large driver cluster)

**Key Lesson**: Driver coordination overhead for managing 50+ concurrent streaming queries can become the bottleneck even when executors are underutilized. Split fan-out pipelines by table groups to keep each driver managing ≤50 tables.

---

## Testing Methodology with dblstreamgen

This section outlines a systematic approach to scale testing using dblstreamgen as your load generator, integrated with your actual streaming pipeline.

### Overview of Testing Flow

```
┌──────────────────────────────────────────────────────────────┐
│                      Testing Workflow                         │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  1. dblstreamgen (Load Generator)                            │
│          ↓                                                    │
│     Kinesis/Kafka/Event Hubs (Your actual streaming source)  │
│          ↓                                                    │
│  2. Your Streaming Pipeline (Under Test)                     │
│     - Structured Streaming or SDP                            │
│     - Your transformations and business logic                │
│          ↓                                                    │
│  3. Monitoring & Metrics Collection                          │
│     - Streaming metrics (backlog, processing rate)           │
│     - Cluster metrics (CPU, memory, GC)                      │
│          ↓                                                    │
│  4. Analysis & Decision                                      │
│     - Compare against success criteria                       │
│     - Apply decision framework                               │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### Phase 1: Establish Baseline

**Goal**: Understand healthy pipeline behavior under normal load

**Steps**:

1. **Configure dblstreamgen to match production schema**
   - Use same event types and field distributions
   - Match expected event size (JSON payload size)
   - Set `total_rows_per_second` to 100% of expected production load

2. **Run baseline test**
   - Duration: 2-4 hours minimum
   - Start dblstreamgen → feeds into your source (Kinesis/Kafka)
   - Your pipeline consumes and processes
   - Monitor all metrics continuously

3. **Collect baseline metrics**
   - Streaming: Processing rate, batch duration, scheduling delay
   - Cluster: CPU, memory, GC time at steady state
   - Cost: Calculate $/million events
   - Performance: End-to-end latency from generation to sink

4. **Validate stability**
   - Confirm processing rate > input rate
   - No scheduling delay accumulation
   - Resource usage stable (no upward trends)
   - No errors or failures

**Success Criteria**:
```
Target Metrics (Example):
- Input Rate: 1,000 events/sec
- Processing Rate: 1,200 events/sec (20% headroom)
- Batch Duration: 8 sec (for 10 sec trigger)
- CPU: 60-70% average
- Memory: 50-60% average
- GC Time: <5%
```

**Configuration Example**:
```yaml
# dblstreamgen config for baseline test
streaming_config:
  total_rows_per_second: 1000  # 50% of expected 2000/sec peak

event_types:
  - event_type_id: "user.page_view"
    weight: 0.60  # Match production distribution
  - event_type_id: "user.click"
    weight: 0.30
  - event_type_id: "user.purchase"
    weight: 0.10
```

---

### Phase 2: Find Breaking Point

**Goal**: Determine maximum pipeline capacity

**Steps**:

1. **Incremental load increase**
   - Start at baseline 
   - Increase by 25% every 30-60 minutes
   - Continue until pipeline cannot keep up

2. **Load progression example**:
   ```
   Step 1:  1,000 events/sec (baseline) → 30 min → Monitor
   Step 2:  1,500 events/sec (+50%)    → 30 min → Monitor
   Step 3:  2,000 events/sec (+100%)   → 30 min → Monitor
   Step 4:  2,500 events/sec (+150%)   → 30 min → Monitor
   Step 5:  3,000 events/sec (+200%)   → 30 min → Breaking point?
   ```

3. **Watch for breaking point indicators**:
   - Processing rate plateaus despite higher input rate
   - Backlog starts growing continuously
   - Resource exhaustion (CPU 95%+, memory OOM)

4. **Document saturation patterns**:
   - What resource saturated first? (CPU, memory, network)
   - At what throughput did it happen?
   - Was saturation uniform or skewed?
   - Any errors or failures?

**Example Results**:
```
Breaking Point Analysis:
- Baseline (1000/s): Healthy, 20% headroom
- 1500/s: Healthy, 10% headroom
- 2000/s: Marginal, 5% headroom
- 2500/s: Breaking point
  * Processing rate: 2200/s (cannot keep up)
  * Scheduling delay: Growing unbounded
  * CPU: 95% across all executors
  * Diagnosis: CPU saturation

Capacity: 2000 events/sec with headroom
           2200 events/sec absolute max
```

**Success Criteria**:
- Identified maximum sustainable throughput
- Documented which resource saturated first
- Confirmed headroom between expected load and breaking point

---

### Phase 3: Optimize Configuration

**Goal**: Apply decision framework to improve capacity

**Steps**:

1. **Apply decision framework**
   - Review Phase 2 results
   - Determine root cause (CPU, memory, skew, etc.)
   - Decide: Scale OUT, Scale UP, or Split Pipeline

2. **Implement changes**
   - Adjust cluster configuration
   - Or split into multiple pipelines
   - Or optimize code (if bottleneck is algorithmic)

3. **Re-test from Phase 1**
   - Run baseline test with new configuration
   - Confirm improvement
   - Run breaking point test again
   - Validate higher capacity

4. **Iterate if needed**
   - May need multiple rounds of optimization
   - Different bottlenecks emerge at different scales

**Example Optimization Cycle**:
```
Iteration 1:
  Config: 4 x m5.2xlarge (8 vCPU, 32GB)
  Capacity: 2,000 events/sec
  Bottleneck: CPU saturation
  
  Decision: SCALE OUT
  
Iteration 2:
  Config: 8 x m5.2xlarge
  Capacity: 3,800 events/sec
  Bottleneck: Still CPU but better
  Cost: 2x iteration 1
  
  Decision: Continue with 8 workers, meets 3,000/sec target with headroom
```

**Success Criteria**:
- Pipeline meets target throughput with 20-30% headroom
- Cost-effective (not over-provisioned)
- All resources balanced (no single bottleneck)

---

### Phase 4: Validate Resilience

**Goal**: Ensure pipeline handles real-world conditions

**Steps**:

1. **Burst Testing**
   - Run baseline load for 30 minutes
   - Increase dblstreamgen to 3-5x for 10-15 minutes
   - Drop back to baseline
   - Measure recovery time

2. **Expected Behavior**:
   ```
   Time:     0 min    30 min   45 min   60 min   90 min
   Load:     1000/s   1000/s   5000/s   1000/s   1000/s
   Backlog:  0        0        12000    0        0
   Status:   Normal   Normal   Burst    Recovery Normal
   ```

3. **Soak Testing**
   - Run at 80-100% of max capacity
   - Duration: 12-24 hours minimum
   - Watch for gradual degradation

4. **Degradation Testing**
   - Reduce cluster size deliberately
   - Simulate executor loss
   - Validate graceful degradation and recovery

**Example Burst Test Results**:
```
Burst Test (3x normal load):
  Baseline: 1,000 events/sec
  Burst: 3,000 events/sec for 15 minutes
  Total burst events: 2,700,000
  
  Results:
  - Backlog peaked at 30,000 records
  - Recovery time: 25 minutes
  - No failures or errors
  - Returned to baseline performance
  
  Evaluation: PASS
  - Recovery time acceptable (< 2x burst duration)
  - No stability issues
```

**Example Soak Test Results**:
```
Soak Test (24 hours at 90% capacity):
  Load: 1,800 events/sec
  Duration: 24 hours
  
  Results:
  - Memory usage stable (60-65% throughout)
  - GC time consistent (<5%)
  - No checkpoint degradation
  - State size grew to 2.5GB then stabilized
  - No OOM or failures
  
  Evaluation: PASS
  - Production-ready for 2,000 events/sec target
```

**Success Criteria**:
- Burst: Pipeline recovers within acceptable time
- Soak: No degradation over 24+ hours
- Degradation: Graceful behavior, no data loss

---

## Cluster Sizing Guidelines

Use these guidelines as starting points. Always validate with actual testing using the methodology above.

### Small Pipelines (<1K events/sec)

**Characteristics**:
- Startup/proof-of-concept
- Low-volume analytics
- Single-table ingestion
- Simple transformations

**Recommended Starting Point**:
```
Cluster Configuration:
  Driver: 1 x m5.large (2 vCPU, 8GB)
  Workers: 2-4 x m5.large (2 vCPU, 8GB each)
  
Expected Capacity: 500-1,000 events/sec
```

**When to Adjust**:
- If CPU <30%: Over-provisioned, reduce to single-node or smaller
- If CPU >80%: Scale out to 4 workers
- If latency not critical: Consider triggered mode (batch) instead of continuous

---

### Medium Pipelines (1K-10K events/sec)

**Characteristics**:
- Production streaming analytics
- Multiple event types
- Moderate transformations (joins, aggregations)
- Some stateful operations

**Recommended Starting Point**:
```
Cluster Configuration:
  Driver: 1 x m5.xlarge (4 vCPU, 16GB)
  Workers: 4-8 x m5.xlarge (4 vCPU, 16GB each)
  
Expected Capacity: 5,000-8,000 events/sec
```

**Optimization Notes**:
- For stateful operations (windows), consider memory-optimized: 4-8 x r5.xlarge
- For high join cardinality, increase executor memory
- Monitor shuffle spill; if present, increase executor memory

**When to Adjust**:
- **CPU-bound**: Scale out to 8-12 workers
- **Memory-bound**: Switch to m5.2xlarge (8 vCPU, 32GB)
- **Cost optimization**: Test with spot instances for non-critical paths

---

### Large Pipelines (10K-100K events/sec)

**Characteristics**:
- High-throughput production systems
- Complex transformations
- Large stateful windows
- Multiple stages

**Recommended Starting Point**:
```
Cluster Configuration (Option 1: Balanced):
  Driver: 1 x m5.2xlarge (8 vCPU, 32GB)
  Workers: 8-16 x m5.2xlarge (8 vCPU, 32GB each)
  
Expected Capacity: 30,000-60,000 events/sec
Cost: $30-60/hour

Cluster Configuration (Option 2: Memory-Optimized):
  Driver: 1 x r5.2xlarge (8 vCPU, 64GB)
  Workers: 8-12 x r5.2xlarge (8 vCPU, 64GB each)
  
Expected Capacity: 25,000-50,000 events/sec (with large state)
```

**Architecture Considerations**:
- Strongly consider **splitting pipeline** into stages
- Use separate clusters for different stages (ingest vs analytics)
- Implement backpressure and rate limiting
- Monitor state store size carefully
- Use autoscaling if load varies significantly

**When to Adjust**:
- At this scale, always test first with dblstreamgen
- Small changes have large cost impact
- Consider serverless SQL for non-streaming parts

---

### XL Pipelines (>100K events/sec)

**Characteristics**:
- Mission-critical high-throughput systems
- Massive scale
- Complex multi-stage pipelines
- Advanced requirements

**Architectural Approach**:

```
SPLIT PIPELINES Example:

Pipeline 1: Ingest & Parse
  Cluster: 12-20 x m5.2xlarge (CPU-optimized)
  Focus: High throughput, minimal transformation
  Output: Bronze Delta table
  
Pipeline 2: Enrichment & Joins
  Cluster: 8-12 x r5.2xlarge (Memory-optimized)
  Focus: Lookups, enrichment, complex logic
  Output: Silver Delta table
  
Pipeline 3: Aggregations & Analytics
  Cluster: 4-8 x r5.4xlarge (Large memory for state)
  Focus: Windowed aggregations, ML inference
  Output: Gold Delta tables
```

**Expected Capacity**: 100,000-500,000+ events/sec (across all pipelines)

**Critical Success Factors**:
- **Partitioning strategy**: Ensure proper data distribution
- **State management**: Use RocksDB state store for large state
- **Checkpointing**: Use reliable storage (S3, DBFS)
- **Monitoring**: Comprehensive observability (detailed metrics, alerting)
- **Testing**: Extensive testing with dblstreamgen before production
- **Autoscaling**: Leverage Databricks autoscaling for variable load
- **Cost optimization**: Mix of spot and on-demand instances

**When to Consider Alternative Architectures**:
- If latency <1 second required: Consider Real Time Mode
- If cost prohibitive: Re-evaluate if all data needs streaming (batch some parts)

---

### Instance Type Selection Guide

| Instance Family | Best For | CPU:RAM Ratio | Network | Cost |
|-----------------|----------|---------------|---------|------|
| **m5** | General purpose, CPU-bound | 1:4 | Moderate | $ |
| **r5** | Memory-intensive, large state | 1:8 | Moderate | $$ |
| **i3** | Shuffle-heavy, disk I/O | 1:8 | High + NVMe | $$$ |

**Decision Guide**:
- **Start with m5** for most pipelines (balanced)
- **Switch to r5** if GC >10% or large stateful operations
- **Use i3** if excessive shuffle spill (rare, usually optimize first)

---

### Autoscaling Considerations

**When to Enable Autoscaling**:
- Load varies significantly by time of day
- Traffic spikes are common but unpredictable
- Cost optimization is priority
- Can tolerate brief scaling delays (1-2 minutes)

**When NOT to Use Autoscaling**:
- Ultra-low latency requirements (<5 seconds)
- Consistently high load (no scale-down opportunity)
- Stateful streaming with large state (scale-up/down is expensive)
- Testing (introduces variability in metrics)

**Autoscaling Configuration**:
```
Min Workers: Sufficient for baseline load (e.g., 4)
Max Workers: 2-3x baseline for burst capacity (e.g., 12)=
```

---

### Setting Up Alerts

**Critical Alerts** (Page on-call):
```
Alert: Backlog growing >100K records
Alert: OOM errors
Alert: Query failure/stopped
```

**Warning Alerts** (Notify team):
```
Alert: CPU >90% for >5 minutes
Alert: Memory >85% for >5 minutes
Alert: Backlog >50K records
Alert: Driver CPU >80% (for fan-out pipelines with 50+ tables)
```

---

### Troubleshooting Checklist

When pipeline performance degrades:

1. **Check Streaming Metrics**:
   - [ ] Is processing rate < input rate?
   - [ ] Is backlog accumulating?

2. **Check Resource Metrics**:
   - [ ] CPU saturation on any node?
   - [ ] Memory pressure (high usage or GC)?
   - [ ] Network saturation?
   - [ ] Disk I/O issues?

3. **Check for Data Issues**:
   - [ ] Data skew (straggler tasks)?
   - [ ] Sudden increase in event size?
   - [ ] Unexpected event type distribution?
   - [ ] Source throttling or delays?

4. **Check for Code Issues**:
   - [ ] Recent deployment changes?
   - [ ] Expensive UDFs?
   - [ ] Inefficient joins or aggregations?
   - [ ] Memory leaks?

5. **Check External Dependencies**:
   - [ ] Downstream sink throttling?
   - [ ] External API rate limits?
   - [ ] Network connectivity issues?
   - [ ] S3/DBFS performance degraded?

---

## Best Practices

### 1. Always Test with Realistic Load

**Why**: Unrealistic testing misses critical issues

**How**:
- Use dblstreamgen to match production event distributions exactly
- Include rare events (don't just test happy path)
- Match production event size (JSON payload bytes)
- Test with production-like data complexity (nested structures, nulls)

**Example**:
```yaml
# Production distribution
event_types:
  - event_type_id: "common_event"
    weight: 0.80  # 80% of traffic
  - event_type_id: "important_event"
    weight: 0.15  # 15%
  - event_type_id: "rare_event"
    weight: 0.05  # 5% but critical

# Don't just test common_event at 100%
```

---

### 2. Include Stateful Operations in Tests

**Why**: Stateful operations (windows, aggregations, joins) have different scaling characteristics

**How**:
- Test with same window sizes as production (e.g., 1-hour tumbling windows)
- Include high-cardinality aggregations
- Test stream-stream joins if used
- Monitor state store size during tests

**Stateful vs Stateless Scaling**:
```
Stateless (map, filter):
  - Scales linearly with workers
  - Memory stays constant
  - Easy to optimize

Stateful (windows, aggregations):
  - State size grows with window duration
  - Memory requirements increase with cardinality
  - Checkpoint size matters
  - Test carefully
```

---

### 3. Monitor for 3-5x Expected Peak Load

**Why**: Production spikes exceed expectations; headroom prevents failures

**How**:
- Run breaking point tests to 3-5x expected peak
- Understand degradation behavior at extreme load
- Plan for "Black Friday" scenarios

**Headroom Strategy**:
```
Expected Average: 1,000 events/sec
Expected Peak: 2,000 events/sec (2x)
Test Breaking Point: 6,000-10,000 events/sec (3-5x peak)

Sizing Decision:
  Cluster should handle: 3,000 events/sec comfortably (1.5x peak)
  Breaking point: 6,000 events/sec (3x peak)
  Headroom: 2x above expected peak
```

---

### 4. Document Cost per Million Events

**Why**: Enables cost-performance trade-offs and budget planning

**How**:
```
Test Results:
  Configuration: 8 x m5.xlarge
  Cost: $12/hour
  Throughput: 5,000 events/sec = 18M events/hour
  Cost per Million Events: $12 / 18 = $0.67

Alternative Configuration:
  Configuration: 4 x m5.2xlarge
  Cost: $13/hour
  Throughput: 5,200 events/sec = 18.7M events/hour
  Cost per Million Events: $13 / 18.7 = $0.70

Decision: First option slightly more cost-efficient
```

**Cost Optimization Matrix**:
- Prioritize cost: Test spot instances, triggered mode, batch processing
- Prioritize latency: Over-provision, use continuous mode, on-demand instances
- Balanced: Right-size with 20-30% headroom, mix of spot/on-demand


### 5. Test End-to-End Latency

**Why**: Individual stage latency compounds; end-to-end matters

**How**:
- Add timestamps at generation (dblstreamgen includes `event_timestamp`)
- Measure at each pipeline stage
- Calculate end-to-end latency

**Example Measurement**:
```python
# In pipeline
df_with_latency = df.withColumn(
    "processing_latency_ms",
    (unix_timestamp() - unix_timestamp("event_timestamp")) * 1000
)

# Monitor P50, P95, P99 latencies
df_with_latency.selectExpr(
    "percentile_approx(processing_latency_ms, 0.5) as p50",
    "percentile_approx(processing_latency_ms, 0.95) as p95",
    "percentile_approx(processing_latency_ms, 0.99) as p99"
).show()
```

---

### 6. Version Control Test Configurations

**Why**: Reproducible testing and change tracking

**How**:
```
/test-configs/
  baseline-1k-eps.yaml
  peak-3k-eps.yaml
  burst-10k-eps.yaml
  soak-2k-eps-24h.yaml

Include:
  - dblstreamgen configuration
  - Cluster configuration
  - Expected results
  - Test date and outcomes
```

---

### 7. Automate Testing Where Possible

**Why**: Consistent, repeatable testing; catch regressions early

**How**:
- Script test execution
- Automated metrics collection
- Automated analysis and alerting
- Integration with CI/CD

**Example Automation**:
```python
# test_pipeline.py
def run_scale_test(config_path, duration_minutes):
    # Start dblstreamgen
    # Monitor metrics
    # Collect results
    # Analyze against thresholds
    # Return pass/fail
    pass

# Run as part of deployment pipeline
results = run_scale_test("baseline-test.yaml", 60)
if not results['success']:
    raise Exception("Scale test failed, blocking deployment")
```

---

### 8. Test Failure Scenarios

**Why**: Production will experience failures; validate recovery

**How**:
- Kill executor nodes during test
- Simulate source downtime (stop dblstreamgen)
- Test checkpoint recovery
- Validate exactly-once semantics preserved

---

### 9. Document Everything

**Why**: Institutional knowledge, onboarding, troubleshooting

**What to Document**:
- Test methodology and configurations
- Results for each test run (metrics, screenshots)
- Decisions made (why this cluster size?)
- Optimization attempts and outcomes
- Production incidents and root causes
- Lessons learned

**Template**:
```markdown
## Test Run: 2025-10-13 - Baseline Test

### Configuration
- dblstreamgen: baseline-1k-eps.yaml
- Cluster: 4 x m5.xlarge
- Duration: 2 hours

### Results
- Throughput: 1,200 events/sec achieved (target: 1,000)
- Scheduling delay: 1-2 seconds (healthy)
- CPU: 65% average
- Memory: 55% average
- Cost: $8/hour = $0.67 per million events

### Conclusion
- Configuration adequate for baseline load
- 20% headroom available
- Ready for peak load testing
```

---

### 10. Limit Fan-Out to ≤50 Tables per Pipeline

**Why**: Driver coordination overhead scales non-linearly with number of concurrent streaming queries

**How**:
- When fanning out to many tables (one per event type), split into multiple pipelines
- Target: ≤50 tables per pipeline for optimal driver performance
- Monitor driver CPU and checkpoint duration as key indicators

**Fan-Out Scaling Guidelines**:
```
10-30 tables:   Single pipeline OK
                Driver overhead: Minimal
                
30-50 tables:   Single pipeline acceptable
                Monitor driver CPU closely
                Watch checkpoint duration
                
50-100 tables:  Consider splitting
                Driver CPU likely >70%
                Checkpoint duration growing
                
100+ tables:    MUST split
                Driver will become bottleneck
                Split into groups of 30-50 tables each
```

**Example Split Strategy**:
```yaml
# For 150 event types, split into 3 pipelines:

Pipeline 1 Config (event_types_001-050.yaml):
  event_type_filter: ["event_type_001" through "event_type_050"]
  
Pipeline 2 Config (event_types_051-100.yaml):
  event_type_filter: ["event_type_051" through "event_type_100"]
  
Pipeline 3 Config (event_types_101-150.yaml):
  event_type_filter: ["event_type_101" through "event_type_150"]
```

**Warning Signs You Need to Split**:
- Driver CPU >80% while executor CPU <60%
- Checkpoint operations taking >30 seconds
- Scheduling delays despite adequate executor resources
- Driver memory pressure or OOM errors

---

## References

### Databricks Documentation

**Structured Streaming**:
- [Structured Streaming Programming Guide](https://docs.databricks.com/structured-streaming/index.html)
- [Monitoring Streaming Queries](https://docs.databricks.com/structured-streaming/stream-monitoring.html)
- [Structured Streaming Production](https://docs.databricks.com/structured-streaming/production.html)

**Spark Declarative Pipelines**:
- [SDP Overview](https://docs.databricks.com/delta-live-tables/index.html)
- [SDP Monitoring](https://docs.databricks.com/delta-live-tables/observability.html)
- [SDP Event Log](https://docs.databricks.com/delta-live-tables/event-log.html)

**Cluster Configuration**:
- [Cluster Sizing Guide](https://docs.databricks.com/clusters/cluster-config-best-practices.html)
- [Autoscaling](https://docs.databricks.com/clusters/configure.html#autoscaling)
- [Instance Types](https://docs.databricks.com/clusters/instance-types.html)

**Monitoring**:
- [Cluster Metrics](https://docs.databricks.com/clusters/metrics.html)
- [Spark UI Guide](https://docs.databricks.com/spark/latest/sparkui.html)
- [System Tables](https://docs.databricks.com/administration-guide/system-tables/index.html)

---

### dblstreamgen Resources

**Configuration Examples**:
- [Simple Config](../sample/configs/simple_config.yaml) - Basic 3 event types
- [1500 Events Config](../sample/configs/1500_events_config.yaml) - Stress testing
- [Example Notebook](../sample/notebooks/01_simple_example.py) - Complete workflow

**Library Documentation**:
- [README](../README.md) - Installation and quickstart
- [Project Status](agent_context/PROJECT_STATUS.md) - Architecture and roadmap

---

### Additional Reading

**Apache Spark**:
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)

**Streaming Best Practices**:
- Tyler Akidau et al. - "Streaming Systems" (O'Reilly)
- Martin Kleppmann - "Designing Data-Intensive Applications" (O'Reilly)

---

## Summary

Effective scale testing requires:

1. **Systematic testing**: Baseline → Peak → Burst → Soak → Degradation
2. **Comprehensive monitoring**: Both streaming pipeline and cluster metrics
3. **Data-driven decisions**: Scale OUT vs UP vs Split Pipeline based on metrics
4. **Realistic load**: Use dblstreamgen to match production distributions
5. **Continuous validation**: Test before production, monitor in production

**Key Takeaway**: Scale testing is not a one-time activity. As your pipeline evolves (new event types, transformations, requirements), re-test to validate continued performance and optimize costs.

---

**dblstreamgen** enables this entire testing workflow by providing controllable, realistic synthetic load that mirrors production traffic patterns. Use it to validate your streaming pipeline design before committing to production infrastructure.

For questions or contributions, see the main [README](../README.md) or project repository.

---

*Last Updated: October 2025 - v0.1.0*

