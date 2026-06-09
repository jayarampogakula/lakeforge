"""
LakeForge Trust Engine Module
Validates data integrity, join relationships, and transformation anomalies.
"""
from typing import Dict, List, Any, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, isnan, when, lit
import json


class TrustEngine:
    """
    Trust validation engine for ETL pipelines.
    
    Validations:
    - Row count validation (source vs target)
    - Join integrity validation
    - Duplicate explosion detection
    - Anti-join mismatch detection
    - Null spike detection
    """
    
    def __init__(self, spark: SparkSession):
        """Initialize Trust Engine."""
        self.spark = spark
    
    def validate_row_count(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        tolerance_percent: float = 5.0,
        expected_ratio: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Validate row counts between source and target.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            tolerance_percent: Acceptable deviation percentage
            expected_ratio: Expected target/source ratio (None for 1:1)
            
        Returns:
            Validation results
        """
        source_count = source_df.count()
        target_count = target_df.count()
        
        if expected_ratio is None:
            expected_count = source_count
            actual_ratio = target_count / source_count if source_count > 0 else 0
        else:
            expected_count = source_count * expected_ratio
            actual_ratio = target_count / source_count if source_count > 0 else 0
        
        deviation = abs(target_count - expected_count)
        deviation_percent = (deviation / expected_count * 100) if expected_count > 0 else 0
        
        passed = deviation_percent <= tolerance_percent
        
        return {
            "validation": "row_count",
            "passed": passed,
            "source_count": source_count,
            "target_count": target_count,
            "expected_count": int(expected_count),
            "deviation": int(deviation),
            "deviation_percent": round(deviation_percent, 2),
            "tolerance_percent": tolerance_percent,
            "actual_ratio": round(actual_ratio, 4),
            "message": "Row count validation passed" if passed else 
                      f"Row count deviation {deviation_percent:.2f}% exceeds tolerance {tolerance_percent}%"
        }
    
    def validate_join_integrity(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        join_keys: List[str],
        join_type: str = "inner",
        expected_match_percent: float = 95.0
    ) -> Dict[str, Any]:
        """
        Validate join integrity between two DataFrames.
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            join_keys: List of join key columns
            join_type: Join type (inner, left, right, full)
            expected_match_percent: Expected percentage of matches
            
        Returns:
            Validation results
        """
        left_count = left_df.count()
        right_count = right_df.count()
        
        # Perform join
        joined_df = left_df.join(right_df, on=join_keys, how=join_type)
        joined_count = joined_df.count()
        
        # Calculate match rate
        if join_type == "inner":
            match_rate = (joined_count / min(left_count, right_count) * 100) if min(left_count, right_count) > 0 else 0
        elif join_type == "left":
            match_rate = (joined_count / left_count * 100) if left_count > 0 else 0
        elif join_type == "right":
            match_rate = (joined_count / right_count * 100) if right_count > 0 else 0
        else:
            match_rate = 100.0  # Full outer join
        
        passed = match_rate >= expected_match_percent
        
        # Calculate unmatched counts
        if join_type == "left":
            unmatched_left = left_count - joined_count
            unmatched_right = 0
        elif join_type == "right":
            unmatched_left = 0
            unmatched_right = right_count - joined_count
        else:
            unmatched_left = left_count - joined_count
            unmatched_right = right_count - joined_count
        
        return {
            "validation": "join_integrity",
            "passed": passed,
            "join_type": join_type,
            "join_keys": join_keys,
            "left_count": left_count,
            "right_count": right_count,
            "joined_count": joined_count,
            "match_rate_percent": round(match_rate, 2),
            "expected_match_percent": expected_match_percent,
            "unmatched_left": unmatched_left,
            "unmatched_right": unmatched_right,
            "message": "Join integrity validation passed" if passed else 
                      f"Join match rate {match_rate:.2f}% below expected {expected_match_percent}%"
        }
    
    def detect_duplicate_explosion(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        key_columns: List[str],
        max_explosion_ratio: float = 1.5
    ) -> Dict[str, Any]:
        """
        Detect duplicate explosion after transformation.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            key_columns: Business key columns
            max_explosion_ratio: Maximum acceptable explosion ratio
            
        Returns:
            Validation results
        """
        source_count = source_df.count()
        target_count = target_df.count()
        
        # Get distinct key counts
        source_distinct = source_df.select(key_columns).distinct().count()
        target_distinct = target_df.select(key_columns).distinct().count()
        
        # Calculate explosion ratios
        overall_ratio = target_count / source_count if source_count > 0 else 0
        key_ratio = target_distinct / source_distinct if source_distinct > 0 else 0
        
        # Check for explosion
        has_explosion = overall_ratio > max_explosion_ratio
        
        # Calculate average duplicates per key
        avg_source_dups = source_count / source_distinct if source_distinct > 0 else 0
        avg_target_dups = target_count / target_distinct if target_distinct > 0 else 0
        
        passed = not has_explosion
        
        return {
            "validation": "duplicate_explosion",
            "passed": passed,
            "has_explosion": has_explosion,
            "source_count": source_count,
            "target_count": target_count,
            "source_distinct_keys": source_distinct,
            "target_distinct_keys": target_distinct,
            "overall_explosion_ratio": round(overall_ratio, 4),
            "key_explosion_ratio": round(key_ratio, 4),
            "max_explosion_ratio": max_explosion_ratio,
            "avg_source_duplicates": round(avg_source_dups, 2),
            "avg_target_duplicates": round(avg_target_dups, 2),
            "message": "No duplicate explosion detected" if passed else 
                      f"Duplicate explosion detected: ratio {overall_ratio:.2f} exceeds max {max_explosion_ratio}"
        }
    
    def validate_anti_join(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        join_keys: List[str],
        max_mismatch_percent: float = 5.0
    ) -> Dict[str, Any]:
        """
        Detect records in left that don't match right (anti-join).
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            join_keys: Join key columns
            max_mismatch_percent: Maximum acceptable mismatch percentage
            
        Returns:
            Validation results
        """
        left_count = left_df.count()
        
        # Anti-join: records in left not in right
        anti_join_df = left_df.join(right_df, on=join_keys, how="left_anti")
        mismatch_count = anti_join_df.count()
        
        mismatch_percent = (mismatch_count / left_count * 100) if left_count > 0 else 0
        
        passed = mismatch_percent <= max_mismatch_percent
        
        return {
            "validation": "anti_join_mismatch",
            "passed": passed,
            "left_count": left_count,
            "mismatch_count": mismatch_count,
            "mismatch_percent": round(mismatch_percent, 2),
            "max_mismatch_percent": max_mismatch_percent,
            "join_keys": join_keys,
            "message": "Anti-join validation passed" if passed else 
                      f"Anti-join mismatch {mismatch_percent:.2f}% exceeds max {max_mismatch_percent}%"
        }
    
    def detect_null_spike(
        self,
        df: DataFrame,
        column: str,
        historical_null_rate: Optional[float] = None,
        max_spike_percent: float = 10.0
    ) -> Dict[str, Any]:
        """
        Detect spike in null values for a column.
        
        Args:
            df: DataFrame to check
            column: Column name
            historical_null_rate: Historical null rate (0-100)
            max_spike_percent: Maximum acceptable spike in null rate
            
        Returns:
            Validation results
        """
        total_count = df.count()
        
        # Count nulls
        null_count = df.filter(col(column).isNull()).count()
        current_null_rate = (null_count / total_count * 100) if total_count > 0 else 0
        
        if historical_null_rate is None:
            # No baseline - just report current rate
            has_spike = False
            spike_percent = 0.0
        else:
            spike_percent = current_null_rate - historical_null_rate
            has_spike = spike_percent > max_spike_percent
        
        passed = not has_spike
        
        return {
            "validation": "null_spike",
            "passed": passed,
            "column": column,
            "total_count": total_count,
            "null_count": null_count,
            "current_null_rate": round(current_null_rate, 2),
            "historical_null_rate": historical_null_rate,
            "spike_percent": round(spike_percent, 2),
            "max_spike_percent": max_spike_percent,
            "has_spike": has_spike,
            "message": "No null spike detected" if passed else 
                      f"Null spike detected: {spike_percent:.2f}% increase exceeds max {max_spike_percent}%"
        }
    
    def run_trust_validations(
        self,
        validations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Run multiple trust validations.
        
        Args:
            validations: List of validation configurations
            
        Returns:
            Combined validation results with trust score
        """
        results = []
        passed_count = 0
        failed_count = 0
        
        for validation_config in validations:
            validation_type = validation_config.get("type")
            
            try:
                if validation_type == "row_count":
                    result = self.validate_row_count(**validation_config.get("params", {}))
                elif validation_type == "join_integrity":
                    result = self.validate_join_integrity(**validation_config.get("params", {}))
                elif validation_type == "duplicate_explosion":
                    result = self.detect_duplicate_explosion(**validation_config.get("params", {}))
                elif validation_type == "anti_join":
                    result = self.validate_anti_join(**validation_config.get("params", {}))
                elif validation_type == "null_spike":
                    result = self.detect_null_spike(**validation_config.get("params", {}))
                else:
                    result = {
                        "validation": validation_type,
                        "passed": False,
                        "error": f"Unknown validation type: {validation_type}"
                    }
                
                results.append(result)
                
                if result.get("passed"):
                    passed_count += 1
                else:
                    failed_count += 1
            
            except Exception as e:
                results.append({
                    "validation": validation_type,
                    "passed": False,
                    "error": str(e)
                })
                failed_count += 1
        
        total_validations = len(validations)
        trust_score = (passed_count / total_validations * 100) if total_validations > 0 else 0
        
        return {
            "timestamp": datetime.now().isoformat(),
            "total_validations": total_validations,
            "passed_count": passed_count,
            "failed_count": failed_count,
            "trust_score": round(trust_score, 2),
            "all_passed": failed_count == 0,
            "validation_results": results
        }
    
    def log_trust_results(
        self,
        trust_results: Dict[str, Any],
        catalog: str,
        schema: str,
        log_table: str = "trust_validation_log"
    ):
        """
        Log trust validation results to Delta table.
        
        Args:
            trust_results: Trust validation results
            catalog: Catalog name
            schema: Schema name
            log_table: Log table name
        """
        table_path = f"{catalog}.{schema}.{log_table}"
        
        log_record = {
            "timestamp": trust_results["timestamp"],
            "total_validations": trust_results["total_validations"],
            "passed_count": trust_results["passed_count"],
            "failed_count": trust_results["failed_count"],
            "trust_score": trust_results["trust_score"],
            "all_passed": trust_results["all_passed"],
            "details_json": json.dumps(trust_results)
        }
        
        log_df = self.spark.createDataFrame([log_record])
        
        try:
            log_df.write.format("delta").mode("append").saveAsTable(table_path)
        except Exception:
            log_df.write.format("delta").mode("overwrite").saveAsTable(table_path)


def create_trust_engine(spark: SparkSession) -> TrustEngine:
    """
    Factory function to create TrustEngine instance.
    
    Args:
        spark: SparkSession
        
    Returns:
        TrustEngine instance
    """
    return TrustEngine(spark)
