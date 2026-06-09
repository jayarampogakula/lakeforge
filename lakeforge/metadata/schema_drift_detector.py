"""
LakeForge Schema Drift Detection Module
Detects schema changes between source and target datasets.
"""
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DataType
import json


class SchemaDriftDetector:
    """
    Detect schema drift between source and target schemas.
    
    Detects:
    - Datatype changes
    - New columns
    - Deleted columns
    - Nullable changes
    - Column order changes
    """
    
    def __init__(self, spark: SparkSession):
        """Initialize Schema Drift Detector."""
        self.spark = spark
    
    def compare_schemas(
        self,
        source_schema: StructType,
        target_schema: StructType,
        column_mapping: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Compare two schemas and detect differences.
        
        Args:
            source_schema: Source DataFrame schema
            target_schema: Target DataFrame schema
            column_mapping: Optional column name mapping (source -> target)
            
        Returns:
            Dictionary containing drift detection results
        """
        column_mapping = column_mapping or {}
        
        # Convert schemas to dicts for easier comparison
        source_cols = {f.name: f for f in source_schema.fields}
        target_cols = {f.name: f for f in target_schema.fields}
        
        # Detect changes
        datatype_changes = []
        nullable_changes = []
        new_columns = []
        deleted_columns = []
        
        # Check for datatype and nullable changes
        for col_name, source_field in source_cols.items():
            target_col_name = column_mapping.get(col_name, col_name)
            
            if target_col_name in target_cols:
                target_field = target_cols[target_col_name]
                
                # Check datatype change
                if source_field.dataType != target_field.dataType:
                    datatype_changes.append({
                        "column": col_name,
                        "source_type": str(source_field.dataType),
                        "target_type": str(target_field.dataType)
                    })
                
                # Check nullable change
                if source_field.nullable != target_field.nullable:
                    nullable_changes.append({
                        "column": col_name,
                        "source_nullable": source_field.nullable,
                        "target_nullable": target_field.nullable
                    })
        
        # Detect new columns (in source but not in target)
        source_col_names = set(source_cols.keys())
        target_col_names = set(target_cols.keys())
        
        # Account for column mapping
        mapped_target_names = set()
        for source_col in source_col_names:
            mapped_name = column_mapping.get(source_col, source_col)
            mapped_target_names.add(mapped_name)
        
        new_columns = [
            {"column": col, "datatype": str(source_cols[col].dataType)}
            for col in source_col_names
            if column_mapping.get(col, col) not in target_col_names
        ]
        
        deleted_columns = [
            {"column": col, "datatype": str(target_cols[col].dataType)}
            for col in target_col_names
            if col not in mapped_target_names
        ]
        
        # Calculate drift score
        total_checks = len(source_cols) + len(target_cols)
        total_drifts = (
            len(datatype_changes) + 
            len(nullable_changes) + 
            len(new_columns) + 
            len(deleted_columns)
        )
        
        drift_score = 1.0 - (total_drifts / total_checks) if total_checks > 0 else 1.0
        has_drift = total_drifts > 0
        
        return {
            "timestamp": datetime.now().isoformat(),
            "has_drift": has_drift,
            "drift_score": round(drift_score, 4),
            "total_drifts": total_drifts,
            "datatype_changes": datatype_changes,
            "nullable_changes": nullable_changes,
            "new_columns": new_columns,
            "deleted_columns": deleted_columns,
            "source_column_count": len(source_cols),
            "target_column_count": len(target_cols)
        }
    
    def detect_drift(
        self,
        source_df: DataFrame,
        target_table: str,
        catalog: str,
        schema: str,
        column_mapping: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Detect schema drift between source DataFrame and target table.
        
        Args:
            source_df: Source DataFrame
            target_table: Target table name
            catalog: Catalog name
            schema: Schema name
            column_mapping: Optional column mapping
            
        Returns:
            Drift detection results
        """
        table_path = f"{catalog}.{schema}.{target_table}"
        
        # Check if target table exists
        try:
            target_df = self.spark.table(table_path)
            target_schema = target_df.schema
        except Exception as e:
            return {
                "timestamp": datetime.now().isoformat(),
                "has_drift": False,
                "drift_score": 1.0,
                "error": f"Target table does not exist: {table_path}",
                "is_new_table": True
            }
        
        return self.compare_schemas(source_df.schema, target_schema, column_mapping)
    
    def log_drift(
        self,
        drift_results: Dict[str, Any],
        catalog: str,
        schema: str,
        log_table: str = "schema_drift_log"
    ):
        """
        Log schema drift results to Delta table.
        
        Args:
            drift_results: Drift detection results
            catalog: Catalog name
            schema: Schema name
            log_table: Log table name
        """
        table_path = f"{catalog}.{schema}.{log_table}"
        
        # Flatten results for table storage
        log_record = {
            "timestamp": drift_results["timestamp"],
            "has_drift": drift_results["has_drift"],
            "drift_score": drift_results["drift_score"],
            "total_drifts": drift_results.get("total_drifts", 0),
            "datatype_changes_count": len(drift_results.get("datatype_changes", [])),
            "nullable_changes_count": len(drift_results.get("nullable_changes", [])),
            "new_columns_count": len(drift_results.get("new_columns", [])),
            "deleted_columns_count": len(drift_results.get("deleted_columns", [])),
            "details_json": json.dumps(drift_results)
        }
        
        # Create DataFrame and append
        log_df = self.spark.createDataFrame([log_record])
        
        try:
            log_df.write.format("delta").mode("append").saveAsTable(table_path)
        except Exception:
            # Create table if it doesn't exist
            log_df.write.format("delta").mode("overwrite").saveAsTable(table_path)
    
    def get_schema_evolution_strategy(
        self,
        drift_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Recommend schema evolution strategy based on drift.
        
        Args:
            drift_results: Drift detection results
            
        Returns:
            Strategy recommendations
        """
        recommendations = []
        
        # Handle new columns
        if drift_results.get("new_columns"):
            recommendations.append({
                "type": "new_columns",
                "action": "ADD_COLUMNS",
                "description": "Use ALTER TABLE ADD COLUMNS or mergeSchema option",
                "columns": drift_results["new_columns"]
            })
        
        # Handle deleted columns
        if drift_results.get("deleted_columns"):
            recommendations.append({
                "type": "deleted_columns",
                "action": "HANDLE_DELETED",
                "description": "Consider dropping columns or keeping for historical data",
                "columns": drift_results["deleted_columns"]
            })
        
        # Handle datatype changes
        if drift_results.get("datatype_changes"):
            recommendations.append({
                "type": "datatype_changes",
                "action": "CAST_OR_RECREATE",
                "description": "Cast compatible types or recreate table",
                "changes": drift_results["datatype_changes"]
            })
        
        # Handle nullable changes
        if drift_results.get("nullable_changes"):
            recommendations.append({
                "type": "nullable_changes",
                "action": "UPDATE_CONSTRAINTS",
                "description": "Update NOT NULL constraints if needed",
                "changes": drift_results["nullable_changes"]
            })
        
        return {
            "has_recommendations": len(recommendations) > 0,
            "recommendation_count": len(recommendations),
            "recommendations": recommendations
        }
    
    def auto_evolve_schema(
        self,
        source_df: DataFrame,
        target_table: str,
        catalog: str,
        schema: str,
        allow_datatype_changes: bool = False
    ) -> Dict[str, Any]:
        """
        Automatically evolve target schema to match source.
        
        Args:
            source_df: Source DataFrame
            target_table: Target table name
            catalog: Catalog name
            schema: Schema name
            allow_datatype_changes: Allow automatic datatype evolution
            
        Returns:
            Evolution results
        """
        drift_results = self.detect_drift(source_df, target_table, catalog, schema)
        
        if not drift_results.get("has_drift"):
            return {
                "evolved": False,
                "message": "No schema drift detected"
            }
        
        table_path = f"{catalog}.{schema}.{target_table}"
        actions_taken = []
        
        # Handle new columns (safe operation)
        if drift_results.get("new_columns"):
            try:
                # Write with mergeSchema option to add new columns
                source_df.write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(table_path)
                
                actions_taken.append("Added new columns via mergeSchema")
            except Exception as e:
                actions_taken.append(f"Failed to add columns: {str(e)}")
        
        # Handle datatype changes (potentially unsafe)
        if drift_results.get("datatype_changes") and allow_datatype_changes:
            actions_taken.append("Datatype changes detected but auto-evolution not safe")
        
        return {
            "evolved": len(actions_taken) > 0,
            "actions_taken": actions_taken,
            "drift_results": drift_results
        }


def create_schema_drift_detector(spark: SparkSession) -> SchemaDriftDetector:
    """
    Factory function to create SchemaDriftDetector instance.
    
    Args:
        spark: SparkSession
        
    Returns:
        SchemaDriftDetector instance
    """
    return SchemaDriftDetector(spark)
