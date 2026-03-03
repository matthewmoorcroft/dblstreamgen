"""
Characterization tests for dblstreamgen output correctness.

These tests validate that generated DataFrames match the config specification:
schema, event type distribution, conditional null correctness, value constraints,
and weighted value distributions.

Written against v0.3 to establish a baseline. After v0.4 migration, only the
`wide_df` fixture changes (Config/Scenario API) — all assertions stay identical.
This makes the suite a migration acceptance gate.

Run:  pytest tests/characterization/ -v
"""

import re
import uuid

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dblstreamgen.config import Config

# ---------------------------------------------------------------------------
# Test config — v0.4 schema: float weights summing to 1.0, scenario section.
# ---------------------------------------------------------------------------

ROW_COUNT = 50_000

TEST_CONFIG = {
    "generation_mode": "batch",
    "scenario": {
        "total_rows": ROW_COUNT,
        "partitions": 8,
    },
    "common_fields": {
        "event_name": {"event_type_id": True},
        "event_key": {
            "type": "string",
            "values": ["user_1", "user_2", "user_3", "user_4", "user_5"],
        },
        "event_timestamp": {"type": "timestamp"},
        "event_id": {"type": "uuid"},
        "status": {
            "type": "string",
            "values": ["active", "inactive", "pending"],
            "weights": [6, 3, 1],
        },
    },
    "event_types": [
        {
            "event_type_id": "user.page_view",
            "weight": 0.60,
            "fields": {
                "page_url": {
                    "type": "string",
                    "values": ["/home", "/products", "/cart"],
                    "weights": [5, 3, 2],
                },
                "referrer": {
                    "type": "string",
                    "values": ["direct", "google", "facebook"],
                    "weights": [3, 5, 2],
                },
                "user_id": {"type": "int", "range": [1, 1_000_000]},
            },
        },
        {
            "event_type_id": "user.click",
            "weight": 0.30,
            "fields": {
                "element_id": {
                    "type": "string",
                    "values": ["btn_buy", "btn_add_cart", "link_product"],
                    "weights": [4, 4, 2],
                },
                "user_id": {"type": "int", "range": [1, 1_000_000]},
            },
        },
        {
            "event_type_id": "user.purchase",
            "weight": 0.10,
            "fields": {
                "amount": {"type": "float", "range": [10.0, 500.0]},
                "product_count": {"type": "int", "range": [1, 10]},
                "user_id": {"type": "int", "range": [1, 1_000_000]},
            },
        },
    ],
}

EXPECTED_EVENT_WEIGHTS = {
    "user.page_view": 6,
    "user.click": 3,
    "user.purchase": 1,
}
TOTAL_WEIGHT = sum(EXPECTED_EVENT_WEIGHTS.values())
EXPECTED_EVENT_PROPORTIONS = {
    k: v / TOTAL_WEIGHT for k, v in EXPECTED_EVENT_WEIGHTS.items()
}

EXPECTED_COMMON_FIELDS = {"event_name", "event_key", "event_timestamp", "event_id", "status"}

CONDITIONAL_FIELD_OWNERSHIP = {
    "page_url": {"user.page_view"},
    "referrer": {"user.page_view"},
    "element_id": {"user.click"},
    "amount": {"user.purchase"},
    "product_count": {"user.purchase"},
    "user_id": {"user.page_view", "user.click", "user.purchase"},
}

EXPECTED_ALL_COLUMNS = EXPECTED_COMMON_FIELDS | set(CONDITIONAL_FIELD_OWNERSHIP.keys())


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def wide_df(spark) -> DataFrame:
    """Build the wide-schema DataFrame.

    v0.4: Scenario(spark, Config.from_dict(config_dict)).build(serialize=False)
    """
    from dblstreamgen.scenario.scenario import Scenario

    config = Config.from_dict(TEST_CONFIG)
    return Scenario(spark, config).build(serialize=False)


@pytest.fixture(scope="module")
def collected(wide_df) -> list[dict]:
    """Collect all rows as dicts for assertion. Cached per module."""
    return [row.asDict() for row in wide_df.collect()]


@pytest.fixture(scope="module")
def event_counts(wide_df) -> dict[str, int]:
    """Row count per event type."""
    rows = (
        wide_df.groupBy("event_name")
        .count()
        .collect()
    )
    return {row["event_name"]: row["count"] for row in rows}


# ===================================================================
# 1. SCHEMA CORRECTNESS
# ===================================================================

class TestSchema:
    """Validate the output DataFrame schema matches the config."""

    def test_has_all_expected_columns(self, wide_df):
        actual = set(wide_df.columns)
        missing = EXPECTED_ALL_COLUMNS - actual
        assert not missing, f"Missing columns: {missing}"

    def test_no_unexpected_columns(self, wide_df):
        actual = set(wide_df.columns)
        extra = actual - EXPECTED_ALL_COLUMNS
        assert not extra, f"Unexpected columns: {extra}"

    def test_internal_columns_omitted(self, wide_df):
        internal = [c for c in wide_df.columns if c.startswith("__dsg_")]
        assert internal == [], f"Internal columns leaked to output: {internal}"

    def test_column_types(self, wide_df):
        type_map = {f.name: f.dataType.simpleString() for f in wide_df.schema.fields}

        assert type_map["event_name"] == "string"
        assert type_map["event_key"] == "string"
        assert type_map["event_timestamp"] == "timestamp"
        assert type_map["event_id"] == "string"  # uuid → string
        assert type_map["status"] == "string"
        assert type_map["page_url"] == "string"
        assert type_map["referrer"] == "string"
        assert type_map["element_id"] == "string"
        assert type_map["user_id"] == "int"
        assert type_map["amount"] == "float"
        assert type_map["product_count"] == "int"

    def test_row_count(self, wide_df):
        count = wide_df.count()
        assert count == ROW_COUNT, f"Expected {ROW_COUNT} rows, got {count}"


# ===================================================================
# 2. EVENT TYPE DISTRIBUTION
# ===================================================================

class TestEventTypeDistribution:
    """Validate event type proportions match configured weights."""

    TOLERANCE = 0.03  # ±3% absolute at 50K rows

    def test_all_event_types_present(self, event_counts):
        expected_types = set(EXPECTED_EVENT_PROPORTIONS.keys())
        actual_types = set(event_counts.keys())
        assert actual_types == expected_types, (
            f"Expected event types {expected_types}, got {actual_types}"
        )

    @pytest.mark.parametrize(
        "event_type_id",
        list(EXPECTED_EVENT_PROPORTIONS.keys()),
    )
    def test_event_type_proportion(self, event_type_id, event_counts):
        total = sum(event_counts.values())
        actual_proportion = event_counts[event_type_id] / total
        expected_proportion = EXPECTED_EVENT_PROPORTIONS[event_type_id]

        assert abs(actual_proportion - expected_proportion) < self.TOLERANCE, (
            f"{event_type_id}: expected ~{expected_proportion:.2%}, "
            f"got {actual_proportion:.2%} "
            f"({event_counts[event_type_id]}/{total} rows)"
        )


# ===================================================================
# 3. CONDITIONAL FIELD NULL CORRECTNESS
# ===================================================================

class TestConditionalNulls:
    """Validate conditional fields are NULL for non-owning event types
    and populated for owning event types."""

    @pytest.mark.parametrize("field_name", [
        "page_url", "referrer", "element_id", "amount", "product_count",
    ])
    def test_field_null_for_non_owning_types(self, wide_df, field_name):
        owners = CONDITIONAL_FIELD_OWNERSHIP[field_name]
        all_types = set(EXPECTED_EVENT_PROPORTIONS.keys())
        non_owners = all_types - owners

        for event_type in non_owners:
            non_null_count = (
                wide_df.filter(
                    (F.col("event_name") == event_type)
                    & F.col(field_name).isNotNull()
                )
                .count()
            )
            assert non_null_count == 0, (
                f"'{field_name}' should be NULL for {event_type}, "
                f"but found {non_null_count} non-null rows"
            )

    @pytest.mark.parametrize("field_name", [
        "page_url", "referrer", "element_id", "amount", "product_count",
    ])
    def test_field_populated_for_owning_types(self, wide_df, field_name):
        owners = CONDITIONAL_FIELD_OWNERSHIP[field_name]

        for event_type in owners:
            type_count = wide_df.filter(F.col("event_name") == event_type).count()
            non_null_count = (
                wide_df.filter(
                    (F.col("event_name") == event_type)
                    & F.col(field_name).isNotNull()
                )
                .count()
            )
            null_rate = 1 - (non_null_count / type_count) if type_count > 0 else 0

            assert non_null_count > 0, (
                f"'{field_name}' should be populated for {event_type}, "
                f"but all {type_count} rows are NULL"
            )
            assert null_rate < 0.01, (
                f"'{field_name}' for {event_type}: {null_rate:.1%} nulls "
                f"(expected ~0% since no percent_nulls configured)"
            )

    def test_user_id_populated_for_all_types(self, wide_df):
        """user_id is defined in all 3 event types — should never be null."""
        null_count = wide_df.filter(F.col("user_id").isNull()).count()
        assert null_count == 0, f"user_id has {null_count} unexpected NULLs"


# ===================================================================
# 4. COMMON FIELD CORRECTNESS
# ===================================================================

class TestCommonFields:
    """Validate common fields are populated for all event types."""

    def test_event_name_never_null(self, wide_df):
        null_count = wide_df.filter(F.col("event_name").isNull()).count()
        assert null_count == 0, f"event_name has {null_count} NULLs"

    def test_event_name_matches_event_type_ids(self, collected):
        valid_types = set(EXPECTED_EVENT_PROPORTIONS.keys())
        actual_types = {row["event_name"] for row in collected}
        assert actual_types == valid_types, (
            f"event_name values {actual_types} don't match "
            f"event_type_ids {valid_types}"
        )

    def test_event_key_from_configured_values(self, collected):
        expected_keys = {"user_1", "user_2", "user_3", "user_4", "user_5"}
        actual_keys = {row["event_key"] for row in collected if row["event_key"]}
        unexpected = actual_keys - expected_keys
        assert not unexpected, f"Unexpected event_key values: {unexpected}"

    def test_event_key_never_null(self, wide_df):
        null_count = wide_df.filter(F.col("event_key").isNull()).count()
        assert null_count == 0, f"event_key has {null_count} NULLs"

    def test_event_timestamp_never_null(self, wide_df):
        null_count = wide_df.filter(F.col("event_timestamp").isNull()).count()
        assert null_count == 0, f"event_timestamp has {null_count} NULLs"

    def test_event_id_never_null(self, wide_df):
        null_count = wide_df.filter(F.col("event_id").isNull()).count()
        assert null_count == 0, f"event_id has {null_count} NULLs"

    def test_event_id_looks_like_uuid(self, collected):
        """Spot-check that event_id values are valid UUIDs."""
        sample = [row["event_id"] for row in collected[:100] if row["event_id"]]
        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            re.IGNORECASE,
        )
        for val in sample:
            assert uuid_pattern.match(val), f"event_id '{val}' is not a valid UUID"

    def test_status_from_configured_values(self, collected):
        expected = {"active", "inactive", "pending"}
        actual = {row["status"] for row in collected if row["status"]}
        unexpected = actual - expected
        assert not unexpected, f"Unexpected status values: {unexpected}"


# ===================================================================
# 5. VALUE RANGE CONSTRAINTS
# ===================================================================

class TestValueRanges:
    """Validate numeric fields respect configured range bounds."""

    def test_amount_within_range(self, wide_df):
        stats = (
            wide_df.filter(F.col("amount").isNotNull())
            .agg(
                F.min("amount").alias("min_val"),
                F.max("amount").alias("max_val"),
            )
            .collect()[0]
        )
        assert stats["min_val"] >= 10.0, f"amount min {stats['min_val']} < 10.0"
        assert stats["max_val"] <= 500.0, f"amount max {stats['max_val']} > 500.0"

    def test_product_count_within_range(self, wide_df):
        stats = (
            wide_df.filter(F.col("product_count").isNotNull())
            .agg(
                F.min("product_count").alias("min_val"),
                F.max("product_count").alias("max_val"),
            )
            .collect()[0]
        )
        assert stats["min_val"] >= 1, f"product_count min {stats['min_val']} < 1"
        assert stats["max_val"] <= 10, f"product_count max {stats['max_val']} > 10"

    def test_user_id_within_range(self, wide_df):
        stats = (
            wide_df.filter(F.col("user_id").isNotNull())
            .agg(
                F.min("user_id").alias("min_val"),
                F.max("user_id").alias("max_val"),
            )
            .collect()[0]
        )
        assert stats["min_val"] >= 1, f"user_id min {stats['min_val']} < 1"
        assert stats["max_val"] <= 1_000_000, f"user_id max {stats['max_val']} > 1M"


# ===================================================================
# 6. STRING VALUE SET CORRECTNESS
# ===================================================================

class TestStringValueSets:
    """Validate string fields only contain configured values."""

    def test_page_url_values(self, wide_df):
        expected = {"/home", "/products", "/cart"}
        actual = {
            row["page_url"]
            for row in wide_df.filter(F.col("page_url").isNotNull())
            .select("page_url")
            .distinct()
            .collect()
        }
        unexpected = actual - expected
        assert not unexpected, f"Unexpected page_url values: {unexpected}"

    def test_referrer_values(self, wide_df):
        expected = {"direct", "google", "facebook"}
        actual = {
            row["referrer"]
            for row in wide_df.filter(F.col("referrer").isNotNull())
            .select("referrer")
            .distinct()
            .collect()
        }
        unexpected = actual - expected
        assert not unexpected, f"Unexpected referrer values: {unexpected}"

    def test_element_id_values(self, wide_df):
        expected = {"btn_buy", "btn_add_cart", "link_product"}
        actual = {
            row["element_id"]
            for row in wide_df.filter(F.col("element_id").isNotNull())
            .select("element_id")
            .distinct()
            .collect()
        }
        unexpected = actual - expected
        assert not unexpected, f"Unexpected element_id values: {unexpected}"


# ===================================================================
# 7. WEIGHTED VALUE DISTRIBUTIONS
# ===================================================================

class TestWeightedDistributions:
    """Validate that weighted values produce expected proportions.

    Uses ±5% absolute tolerance — loose enough for 50K rows but tight
    enough to catch broken generation (e.g., uniform instead of weighted).
    """

    TOLERANCE = 0.05

    def _get_distribution(self, wide_df, event_type, field_name):
        """Get {value: proportion} for a field within one event type."""
        df_filtered = wide_df.filter(
            (F.col("event_name") == event_type)
            & F.col(field_name).isNotNull()
        )
        total = df_filtered.count()
        if total == 0:
            return {}
        rows = (
            df_filtered.groupBy(field_name)
            .count()
            .collect()
        )
        return {row[field_name]: row["count"] / total for row in rows}

    def test_page_url_distribution(self, wide_df):
        """page_url weights: /home=5, /products=3, /cart=2 → 50%, 30%, 20%"""
        dist = self._get_distribution(wide_df, "user.page_view", "page_url")
        expected = {"/home": 0.50, "/products": 0.30, "/cart": 0.20}

        for value, expected_pct in expected.items():
            actual_pct = dist.get(value, 0)
            assert abs(actual_pct - expected_pct) < self.TOLERANCE, (
                f"page_url '{value}': expected ~{expected_pct:.0%}, "
                f"got {actual_pct:.1%}"
            )

    def test_referrer_distribution(self, wide_df):
        """referrer weights: direct=3, google=5, facebook=2 → 30%, 50%, 20%"""
        dist = self._get_distribution(wide_df, "user.page_view", "referrer")
        expected = {"direct": 0.30, "google": 0.50, "facebook": 0.20}

        for value, expected_pct in expected.items():
            actual_pct = dist.get(value, 0)
            assert abs(actual_pct - expected_pct) < self.TOLERANCE, (
                f"referrer '{value}': expected ~{expected_pct:.0%}, "
                f"got {actual_pct:.1%}"
            )

    def test_element_id_distribution(self, wide_df):
        """element_id weights: btn_buy=4, btn_add_cart=4, link_product=2 → 40%, 40%, 20%"""
        dist = self._get_distribution(wide_df, "user.click", "element_id")
        expected = {"btn_buy": 0.40, "btn_add_cart": 0.40, "link_product": 0.20}

        for value, expected_pct in expected.items():
            actual_pct = dist.get(value, 0)
            assert abs(actual_pct - expected_pct) < self.TOLERANCE, (
                f"element_id '{value}': expected ~{expected_pct:.0%}, "
                f"got {actual_pct:.1%}"
            )

    def test_status_distribution(self, wide_df):
        """status weights: active=6, inactive=3, pending=1 → 60%, 30%, 10%"""
        total = wide_df.filter(F.col("status").isNotNull()).count()
        rows = (
            wide_df.filter(F.col("status").isNotNull())
            .groupBy("status")
            .count()
            .collect()
        )
        dist = {row["status"]: row["count"] / total for row in rows}
        expected = {"active": 0.60, "inactive": 0.30, "pending": 0.10}

        for value, expected_pct in expected.items():
            actual_pct = dist.get(value, 0)
            assert abs(actual_pct - expected_pct) < self.TOLERANCE, (
                f"status '{value}': expected ~{expected_pct:.0%}, "
                f"got {actual_pct:.1%}"
            )
