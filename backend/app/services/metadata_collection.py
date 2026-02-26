from sqlalchemy.orm import Session
from app.models.pipeline import Pipeline
from app.models.execution import Execution, ExecutionStage
from app.schemas.execution import ExecutionCreate
from datetime import datetime
import uuid


class MetadataCollectionService:
    """
    Metadata Collection Service:
    - Receives execution metadata from data pipelines
    - Validates required fields
    - Forwards validated data for storage
    """

    @staticmethod
    def validate_execution_metadata(execution_data: ExecutionCreate) -> bool:
        """
        Validates that all required fields are present
        """
        required_fields = [
            "execution_id",
            "pipeline_id",
            "status",
            "start_time",
        ]
        return all(hasattr(execution_data, field) for field in required_fields)

    @staticmethod
    def collect_and_store_metadata(
        db: Session, execution_data: ExecutionCreate
    ) -> Execution:
        """
        Collects metadata and stores it in the database
        """
        # Validate metadata
        if not MetadataCollectionService.validate_execution_metadata(execution_data):
            raise ValueError("Invalid metadata: missing required fields")

        # Get or create pipeline
        pipeline = db.query(Pipeline).filter(
            Pipeline.pipeline_id == execution_data.pipeline_id
        ).first()

        if not pipeline:
            pipeline = Pipeline(
                pipeline_id=execution_data.pipeline_id,
                name=execution_data.pipeline_id,
                description=f"Pipeline {execution_data.pipeline_id}",
            )
            db.add(pipeline)
            db.flush()

        # Create execution record
        execution = Execution(
            execution_id=execution_data.execution_id,
            pipeline_id=pipeline.id,
            status=execution_data.status,
            start_time=execution_data.start_time,
            end_time=execution_data.end_time,
            total_duration=execution_data.total_duration,
            total_rows_processed=execution_data.total_rows_processed,
            schema_info=execution_data.schema_info,
        )
        db.add(execution)
        db.flush()

        # Store execution stages
        for stage_data in execution_data.stages:
            stage = ExecutionStage(
                execution_id=execution.id,
                stage_name=stage_data.stage_name,
                stage_order=stage_data.stage_order,
                status=stage_data.status,
                start_time=stage_data.start_time,
                end_time=stage_data.end_time,
                duration=stage_data.duration,
                rows_input=stage_data.rows_input,
                rows_output=stage_data.rows_output,
                schema_input=stage_data.schema_input,
                schema_output=stage_data.schema_output,
            )
            db.add(stage)

        db.commit()
        db.refresh(execution)
        return execution