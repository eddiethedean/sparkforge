"""
Full pipeline integration tests for sql_pipeline_builder.

These tests verify complete pipeline execution from Bronze → Silver → Gold
with real SQLAlchemy operations.
"""

from typing import Any, cast

import pytest
from abstracts.reports.run import Report
from pipeline_builder_base.models import ExecutionMode, ExecutionResult
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from sql_pipeline_builder import SqlPipelineBuilder

Base: Any = declarative_base()


class UserEvent(Base):
    """Sample bronze table model."""

    __tablename__ = "user_events"
    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    event_type = Column(String)
    value = Column(Integer)
    timestamp = Column(String)  # Using string for simplicity in SQLite


class CleanEvent(Base):
    """Sample silver table model."""

    __tablename__ = "clean_events"
    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    event_type = Column(String)
    value = Column(Integer)
    event_date = Column(String)


class DailyMetric(Base):
    """Sample gold table model."""

    __tablename__ = "daily_metrics"
    event_date = Column(String, primary_key=True)
    total_events = Column(Integer)
    unique_users = Column(Integer)


class EventTypeMetric(Base):
    """Alternate gold table model."""

    __tablename__ = "event_type_metrics"
    event_type = Column(String, primary_key=True)
    count = Column(Integer)


class DependencyMetric(Base):
    """Simple gold model for dependency ordering test."""

    __tablename__ = "dependency_metrics"
    id = Column(Integer, primary_key=True)
    value = Column(Integer)


def create_builder_with_events(
    session: Session, schema: str = "analytics", incremental: bool = True
) -> SqlPipelineBuilder:
    """Create a SqlPipelineBuilder with a default bronze 'events' step."""
    builder = SqlPipelineBuilder(session=session, schema=schema)
    builder.with_bronze_rules(
        name="events",
        rules={"user_id": [UserEvent.user_id.is_not(None)]},
        incremental_col="timestamp" if incremental else None,
        model_class=UserEvent,
    )
    return builder


@pytest.fixture
def sqlite_session():
    """Create a SQLite in-memory session with sample data."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    session = Session(engine)

    # Insert sample bronze data
    events = [
        UserEvent(
            user_id="user1", event_type="click", value=100, timestamp="2024-01-01"
        ),
        UserEvent(
            user_id="user2", event_type="purchase", value=200, timestamp="2024-01-01"
        ),
        UserEvent(
            user_id="user1", event_type="click", value=150, timestamp="2024-01-02"
        ),
        UserEvent(
            user_id="user3", event_type="purchase", value=300, timestamp="2024-01-02"
        ),
    ]
    session.add_all(events)
    session.commit()

    yield session
    session.close()


def test_full_pipeline_initial_load(sqlite_session):
    """Test complete pipeline execution with initial load."""
    # Build pipeline
    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")

    # Bronze: Validate raw events
    builder.with_bronze_rules(
        name="events",
        rules={
            "user_id": [UserEvent.user_id.is_not(None)],
            "value": [UserEvent.value > 0],
        },
        incremental_col="timestamp",
        model_class=UserEvent,
    )

    # Silver: Clean and transform events
    def clean_events(session, bronze_query, silvers):
        from sqlalchemy import func

        return bronze_query.filter(
            UserEvent.value > 50
        ).with_entities(  # Filter low-value events
            UserEvent.id,
            UserEvent.user_id,
            UserEvent.event_type,
            UserEvent.value,
            # SQLite doesn't have date() function, use substring for simplicity
            func.substr(UserEvent.timestamp, 1, 10).label("event_date"),
        )

    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",
        transform=clean_events,
        rules={
            "user_id": [UserEvent.user_id.is_not(None)],
            "value": [UserEvent.value > 50],
        },
        table_name="clean_events",
        model_class=CleanEvent,
    )

    # Gold: Aggregate daily metrics
    def daily_metrics(session, silvers):
        from sqlalchemy import func

        # Read from the clean_events table (silver step output)
        clean_events_query = silvers["clean_events"]
        # Query the table and aggregate
        return clean_events_query.with_entities(
            CleanEvent.event_date,
            func.count(CleanEvent.id).label("total_events"),
            func.count(func.distinct(CleanEvent.user_id)).label("unique_users"),
        ).group_by(CleanEvent.event_date)

    builder.add_gold_transform(
        name="daily_metrics",
        transform=daily_metrics,
        rules={"event_date": []},  # Empty list means no validation
        table_name="daily_metrics",
        source_silvers=["clean_events"],
        model_class=DailyMetric,
    )

    # Execute pipeline
    pipeline = builder.to_pipeline()
    bronze_source = sqlite_session.query(UserEvent)

    result: Report = pipeline.run_initial_load(bronze_sources={"events": bronze_source})
    exec_result = cast(ExecutionResult, result)

    # Verify execution succeeded
    assert exec_result.success, f"Pipeline failed: {exec_result.step_results}"
    assert len(exec_result.step_results) == 3  # Bronze, Silver, Gold

    # Verify bronze step
    bronze_result = next(r for r in exec_result.step_results if r.step_name == "events")
    assert bronze_result.success
    assert bronze_result.rows_processed > 0

    # Verify silver step
    silver_result = next(
        r for r in exec_result.step_results if r.step_name == "clean_events"
    )
    assert silver_result.success
    assert silver_result.rows_written > 0
    assert silver_result.table_fqn == "analytics.clean_events"

    # Verify gold step
    gold_result = next(
        r for r in exec_result.step_results if r.step_name == "daily_metrics"
    )
    assert gold_result.success
    assert gold_result.rows_written > 0
    assert gold_result.table_fqn == "analytics.daily_metrics"

    # Verify data was written to tables
    clean_events_count = sqlite_session.query(CleanEvent).count()
    assert clean_events_count > 0

    daily_metrics_count = sqlite_session.query(DailyMetric).count()
    assert daily_metrics_count > 0


def test_pipeline_with_validation_failure(sqlite_session):
    """Test pipeline behavior when validation fails."""
    # Insert data that will fail validation
    bad_event = UserEvent(
        user_id=None, event_type="click", value=0, timestamp="2024-01-01"
    )
    sqlite_session.add(bad_event)
    sqlite_session.commit()

    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")

    # Bronze with strict validation
    builder.with_bronze_rules(
        name="events",
        rules={
            "user_id": [UserEvent.user_id.is_not(None)],
            "value": [UserEvent.value > 0],
        },
        model_class=UserEvent,
    )

    # Simple silver transform
    def clean_events(session, bronze_query, silvers):
        return bronze_query.filter(UserEvent.user_id.is_not(None))

    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",
        transform=clean_events,
        rules={"user_id": [UserEvent.user_id.is_not(None)]},
        table_name="clean_events",
        model_class=CleanEvent,
    )

    pipeline = builder.to_pipeline()
    bronze_source = sqlite_session.query(UserEvent)

    result: Report = pipeline.run_initial_load(bronze_sources={"events": bronze_source})
    exec_result = cast(ExecutionResult, result)

    # Pipeline should still succeed, but validation should filter out invalid rows
    assert exec_result.success

    # Check that invalid rows were filtered
    bronze_result = next(r for r in exec_result.step_results if r.step_name == "events")
    # Validation rate should be less than 100% due to invalid row
    assert bronze_result.validation_rate < 100.0


def test_pipeline_with_multiple_silver_steps(sqlite_session):
    """Test pipeline with multiple silver steps feeding into gold."""
    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")

    # Bronze
    builder.with_bronze_rules(
        name="events",
        rules={"user_id": [UserEvent.user_id.is_not(None)]},
        model_class=UserEvent,
    )

    # Silver 1: Click events
    def click_events(session, bronze_query, silvers):
        return bronze_query.filter(UserEvent.event_type == "click")

    builder.add_silver_transform(
        name="click_events",
        source_bronze="events",
        transform=click_events,
        rules={"event_type": [UserEvent.event_type == "click"]},
        table_name="click_events",
        model_class=CleanEvent,
    )

    # Silver 2: Purchase events
    def purchase_events(session, bronze_query, silvers):
        return bronze_query.filter(UserEvent.event_type == "purchase")

    builder.add_silver_transform(
        name="purchase_events",
        source_bronze="events",
        transform=purchase_events,
        rules={"event_type": [UserEvent.event_type == "purchase"]},
        table_name="purchase_events",
        model_class=CleanEvent,
    )

    # Gold: Combine both silver sources
    def combined_metrics(session, silvers):
        from sqlalchemy import literal, select

        clicks = silvers["click_events"]
        purchases = silvers["purchase_events"]

        # Count events by type
        click_count = clicks.count()
        purchase_count = purchases.count()

        # Return a simple query with both counts using proper select syntax
        # SQLAlchemy select() requires columns as separate arguments, not a list
        return select(
            literal("click").label("event_type"),
            literal(click_count).label("count"),
        ).union_all(
            select(
                literal("purchase").label("event_type"),
                literal(purchase_count).label("count"),
            )
        )

    builder.add_gold_transform(
        name="event_type_metrics",
        transform=combined_metrics,
        rules={"event_type": []},
        table_name="event_type_metrics",
        source_silvers=["click_events", "purchase_events"],
        model_class=EventTypeMetric,
    )

    # Execute
    pipeline = builder.to_pipeline()
    bronze_source = sqlite_session.query(UserEvent)

    result: Report = pipeline.run_initial_load(bronze_sources={"events": bronze_source})
    exec_result = cast(ExecutionResult, result)

    assert exec_result.success
    assert len(exec_result.step_results) == 4  # 1 bronze + 2 silver + 1 gold

    # Verify both silver steps executed
    click_result = next(
        r for r in exec_result.step_results if r.step_name == "click_events"
    )
    purchase_result = next(
        r for r in exec_result.step_results if r.step_name == "purchase_events"
    )
    assert click_result.success
    assert purchase_result.success


def test_pipeline_creates_missing_tables(sqlite_session):
    """Ensure silver/gold tables are created automatically when missing."""
    from sqlalchemy import func, text

    # Drop tables if they already exist to simulate first-run creation
    sqlite_session.execute(text("DROP TABLE IF EXISTS clean_events"))
    sqlite_session.execute(text("DROP TABLE IF EXISTS daily_metrics"))
    sqlite_session.commit()

    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")

    # Bronze configuration
    builder.with_bronze_rules(
        name="events",
        rules={
            "user_id": [UserEvent.user_id.is_not(None)],
            "value": [UserEvent.value > 0],
        },
        incremental_col="timestamp",
        model_class=UserEvent,
    )

    # Silver transform
    def clean_events(session, bronze_query, silvers):
        return bronze_query.with_entities(
            UserEvent.id,
            UserEvent.user_id,
            UserEvent.event_type,
            UserEvent.value,
            UserEvent.timestamp.label("event_date"),
        )

    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",
        transform=clean_events,
        rules={"user_id": [UserEvent.user_id.is_not(None)]},
        table_name="clean_events",
        model_class=CleanEvent,
    )

    # Gold transform
    def daily_metrics(session, silvers):
        clean_events_query = silvers["clean_events"]
        return clean_events_query.with_entities(
            CleanEvent.event_date,
            func.count(CleanEvent.id).label("total_events"),
            func.count(func.distinct(CleanEvent.user_id)).label("unique_users"),
        ).group_by(CleanEvent.event_date)

    builder.add_gold_transform(
        name="daily_metrics",
        transform=daily_metrics,
        rules={"event_date": []},
        table_name="daily_metrics",
        source_silvers=["clean_events"],
        model_class=DailyMetric,
    )

    pipeline = builder.to_pipeline()
    bronze_source = sqlite_session.query(UserEvent)
    result = pipeline.run_initial_load(bronze_sources={"events": bronze_source})

    assert result.success

    # Verify tables now exist by querying them
    clean_events_count = sqlite_session.execute(
        text("SELECT COUNT(*) FROM clean_events")
    ).scalar_one()
    daily_metrics_count = sqlite_session.execute(
        text("SELECT COUNT(*) FROM daily_metrics")
    ).scalar_one()

    assert clean_events_count > 0
    assert daily_metrics_count >= 0


def test_silver_table_recreated_on_initial(sqlite_session):
    """Silver tables are dropped/recreated on initial run when model provided."""
    metadata = MetaData()
    # Ensure existing table is removed to simulate legacy schema
    sqlite_session.execute(text("DROP TABLE IF EXISTS clean_events"))
    sqlite_session.commit()

    legacy_table = Table(
        "clean_events",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("legacy_col", String),
    )
    legacy_table.create(sqlite_session.bind, checkfirst=False)
    sqlite_session.execute(
        text("INSERT INTO clean_events (id, legacy_col) VALUES (1, 'legacy')")
    )
    sqlite_session.commit()

    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")

    builder.with_bronze_rules(
        name="events",
        rules={
            "user_id": [UserEvent.user_id.is_not(None)],
            "value": [UserEvent.value > 0],
        },
        incremental_col="timestamp",
        model_class=UserEvent,
    )

    def clean_events(session, bronze_query, silvers):
        return bronze_query.with_entities(
            UserEvent.id,
            UserEvent.user_id,
            UserEvent.event_type,
            UserEvent.value,
            UserEvent.timestamp.label("event_date"),
        )

    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",
        transform=clean_events,
        rules={"user_id": [UserEvent.user_id.is_not(None)]},
        table_name="clean_events",
        model_class=CleanEvent,
    )

    pipeline = builder.to_pipeline()
    bronze_source = sqlite_session.query(UserEvent)
    result = pipeline.run_initial_load(bronze_sources={"events": bronze_source})

    assert result.success

    inspector = inspect(sqlite_session.bind)
    columns = {col["name"] for col in inspector.get_columns("clean_events")}
    assert columns == {"id", "user_id", "event_type", "value", "event_date"}


def test_gold_table_recreated_every_run(sqlite_session):
    """Gold tables are dropped and recreated on every run."""
    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")

    builder.with_bronze_rules(
        name="events",
        rules={"user_id": [UserEvent.user_id.is_not(None)]},
        incremental_col="timestamp",
        model_class=UserEvent,
    )

    def clean_events(session, bronze_query, silvers):
        from sqlalchemy import func

        return bronze_query.filter(UserEvent.value > 0).with_entities(
            UserEvent.id,
            UserEvent.user_id,
            UserEvent.event_type,
            UserEvent.value,
            func.substr(UserEvent.timestamp, 1, 10).label("event_date"),
        )

    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",
        transform=clean_events,
        rules={"value": [UserEvent.value > 0]},
        table_name="clean_events",
        model_class=CleanEvent,
    )

    def daily_metrics(session, silvers):
        clean_events_query = silvers["clean_events"]
        from sqlalchemy import func

        return clean_events_query.with_entities(
            CleanEvent.event_date,
            func.count(CleanEvent.id).label("total_events"),
            func.count(func.distinct(CleanEvent.user_id)).label("unique_users"),
        ).group_by(CleanEvent.event_date)

    builder.add_gold_transform(
        name="daily_metrics",
        transform=daily_metrics,
        rules={"event_date": []},
        table_name="daily_metrics",
        source_silvers=["clean_events"],
        model_class=DailyMetric,
    )

    pipeline = builder.to_pipeline()
    bronze_source = sqlite_session.query(UserEvent)
    result1 = pipeline.run_initial_load(bronze_sources={"events": bronze_source})
    assert result1.success

    # Mutate the gold table schema to simulate drift
    sqlite_session.execute(text("ALTER TABLE daily_metrics ADD COLUMN legacy_col TEXT"))
    sqlite_session.commit()

    # Add new event and run incremental load
    new_event = UserEvent(
        user_id="user5",
        event_type="click",
        value=500,
        timestamp="2024-01-04",
    )
    sqlite_session.add(new_event)
    sqlite_session.commit()

    new_query = sqlite_session.query(UserEvent).filter(
        UserEvent.timestamp == "2024-01-04"
    )
    result2 = pipeline.run_incremental(bronze_sources={"events": new_query})
    assert result2.success

    inspector = inspect(sqlite_session.bind)
    columns = {col["name"] for col in inspector.get_columns("daily_metrics")}
    # legacy_col should be gone after the gold table was dropped/recreated
    assert columns == {"event_date", "total_events", "unique_users"}


def test_pipeline_incremental_mode(sqlite_session):
    """Test pipeline execution in incremental mode."""
    builder = create_builder_with_events(sqlite_session)

    def clean_events(session, bronze_query, silvers):
        from sqlalchemy import func

        return bronze_query.filter(UserEvent.value > 0).with_entities(
            UserEvent.id,
            UserEvent.user_id,
            UserEvent.event_type,
            UserEvent.value,
            func.substr(UserEvent.timestamp, 1, 10).label("event_date"),
        )

    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",
        transform=clean_events,
        rules={"value": [UserEvent.value > 0]},
        table_name="clean_events",
        watermark_col="timestamp",
        model_class=CleanEvent,
    )

    pipeline = builder.to_pipeline()
    bronze_source = sqlite_session.query(UserEvent)

    # Run initial load
    result1 = pipeline.run_initial_load(bronze_sources={"events": bronze_source})
    assert result1.success

    # Add new data
    new_event = UserEvent(
        user_id="user4", event_type="click", value=250, timestamp="2024-01-03"
    )
    sqlite_session.add(new_event)
    sqlite_session.commit()

    # Run incremental load - filter to only new events
    # In a real scenario, we'd use the incremental_col to filter, but for this test
    # we'll just query the new event
    new_event_query = sqlite_session.query(UserEvent).filter(
        UserEvent.timestamp == "2024-01-03"
    )
    result2 = pipeline.run_incremental(bronze_sources={"events": new_event_query})
    assert result2.success

    # Verify incremental execution
    assert result2.context.mode == ExecutionMode.INCREMENTAL


def test_silver_append_mode_writes_new_rows(sqlite_session):
    """Silver steps with watermark append only new records."""
    builder = create_builder_with_events(sqlite_session)

    def clean_events(session, bronze_query, silvers):
        return bronze_query.filter(UserEvent.value > 0)

    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",
        transform=clean_events,
        rules={"value": [UserEvent.value > 0]},
        table_name="clean_events",
        watermark_col="timestamp",
        model_class=CleanEvent,
    )

    pipeline = builder.to_pipeline()
    bronze_source = sqlite_session.query(UserEvent)

    result_initial = pipeline.run_initial_load(bronze_sources={"events": bronze_source})
    assert result_initial.success
    initial_count = sqlite_session.query(CleanEvent).count()
    assert initial_count > 0

    # add incremental rows
    new_event = UserEvent(
        user_id="user5", event_type="purchase", value=500, timestamp="2024-01-03"
    )
    sqlite_session.add(new_event)
    sqlite_session.commit()

    new_query = sqlite_session.query(UserEvent).filter(
        UserEvent.timestamp == "2024-01-03"
    )
    result_incremental = pipeline.run_incremental(bronze_sources={"events": new_query})
    assert result_incremental.success

    final_count = sqlite_session.query(CleanEvent).count()
    assert final_count == initial_count + 1

    silver_step = next(
        r for r in result_incremental.step_results if r.step_name == "clean_events"
    )
    assert silver_step.write_mode == "append"


def test_pipeline_dependency_order(sqlite_session):
    """Step execution follows dependency order (bronze -> silver -> gold)."""
    builder = create_builder_with_events(sqlite_session, incremental=False)

    def clean_events(session, bronze_query, silvers):
        return bronze_query.filter(UserEvent.value > 0)

    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",
        transform=clean_events,
        rules={"value": [UserEvent.value > 0]},
        table_name="clean_events",
        model_class=CleanEvent,
    )

    def daily_metrics(session, silvers):
        clean_events_subquery = silvers["clean_events"].subquery()
        return session.query(
            clean_events_subquery.c.id.label("id"),
            clean_events_subquery.c.value.label("value"),
        )

    builder.add_gold_transform(
        name="daily_metrics",
        transform=daily_metrics,
        rules={"value": []},
        table_name="dependency_metrics",
        source_silvers=["clean_events"],
        model_class=DependencyMetric,
    )

    pipeline = builder.to_pipeline()
    result: Report = pipeline.run_initial_load(
        bronze_sources={"events": sqlite_session.query(UserEvent)}
    )
    exec_result = cast(ExecutionResult, result)
    assert exec_result.success

    step_names = [step.step_name for step in exec_result.step_results]
    assert step_names == ["events", "clean_events", "daily_metrics"]


def test_pipeline_error_handling(sqlite_session):
    """Test pipeline error handling when transform fails."""
    builder = SqlPipelineBuilder(session=sqlite_session, schema="analytics")

    builder.with_bronze_rules(
        name="events",
        rules={"user_id": [UserEvent.user_id.is_not(None)]},
        model_class=UserEvent,
    )

    # Transform that will fail
    def failing_transform(session, bronze_query, silvers):
        # This will fail because we're trying to access a non-existent column
        return bronze_query.with_entities(bronze_query.c.non_existent_column)

    builder.add_silver_transform(
        name="failing_step",
        source_bronze="events",
        transform=failing_transform,
        rules={"user_id": []},
        table_name="failing_step",
        model_class=CleanEvent,
    )

    pipeline = builder.to_pipeline()
    bronze_source = sqlite_session.query(UserEvent)

    result: Report = pipeline.run_initial_load(bronze_sources={"events": bronze_source})
    exec_result = cast(ExecutionResult, result)

    # Pipeline should report failure
    assert not exec_result.success

    # Find the failing step
    failing_result = next(
        r for r in exec_result.step_results if r.step_name == "failing_step"
    )
    assert not failing_result.success
    assert failing_result.error_message is not None
