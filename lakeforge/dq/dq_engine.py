"""
LakeForge Data Quality Engine
Executes data quality rules and generates scorecards.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, lit, current_timestamp, sum as _sum
from typing import List, Dict, Any, Optional
from datetime import datetime


class DQEngine:
    """
    Data Quality engine for validating data against defined rules.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize DQ Engine.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self.validation_results = []
    
    def run_null_check(
        self,
        df: DataFrame,
        column: str,
        threshold: float = 0.0
    ) -> Dict[str, Any]:
        """
        Check for null values in a column.
        
        Args:
            df: DataFrame to validate
            column: Column name to check
            threshold: Maximum allowed null percentage (0-1)
            
        Returns:
            Validation result dictionary
        """
        total_count = df.count()
        null_count = df.filter(col(column).isNull()).count()
        null_percentage = null_count / total_count if total_count > 0 else 0
        
        passed = null_percentage <= threshold
        
        return {
            "rule_type": "null_check",
            "column": column,
            "total_records": total_count,
            "null_count": null_count,
            "null_percentage": null_percentage,
            "threshold": threshold,
            "passed": passed,
            "failed_records": null_count if not passed else 0
        }
    
    def run_duplicate_check(
        self,
        df: DataFrame,
        columns: List[str],
        allow_duplicates: bool = False
    ) -> Dict[str, Any]:
        """
        Check for duplicate records based on specified columns.
        
        Args:
            df: DataFrame to validate
            columns: Columns to check for duplicates
            allow_duplicates: Whether duplicates are allowed
            
        Returns:
            Validation result dictionary
        """
        total_count = df.count()
        unique_count = df.select(columns).distinct().count()
        duplicate_count = total_count - unique_count
        
        passed = duplicate_count == 0 if not allow_duplicates else True
        
        return {
            "rule_type": "duplicate_check",
            "columns": columns,
            "total_records": total_count,
            "unique_records": unique_count,
            "duplicate_count": duplicate_count,
            "passed": passed,
            "failed_records": duplicate_count if not passed else 0
        }
    
    def run_regex_check(
        self,
        df: DataFrame,
        column: str,
        pattern: str,
        threshold: float = 1.0
    ) -> Dict[str, Any]:
        """
        Validate column values against regex pattern.
        
        Args:
            df: DataFrame to validate
            column: Column name to check
            pattern: Regex pattern to match
            threshold: Minimum match percentage required (0-1)
            
        Returns:
            Validation result dictionary
        """
        from pyspark.sql.functions import regexp_extract
        
        total_count = df.count()
        
        # Count matching records
        match_count = df.filter(
            regexp_extract(col(column).cast("string"), pattern, 0) != ""
        ).count()
        
        match_percentage = match_count / total_count if total_count > 0 else 0
        failed_count = total_count - match_count
        
        passed = match_percentage >= threshold
        
        return {
            "rule_type": "regex_check",
            "column": column,
            "pattern": pattern,
            "total_records": total_count,
            "matching_records": match_count,
            "match_percentage": match_percentage,
            "threshold": threshold,
            "passed": passed,
            "failed_records": failed_count if not passed else 0
        }
    
    def run_range_check(
        self,
        df: DataFrame,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        threshold: float = 1.0
    ) -> Dict[str, Any]:
        """
        Validate numeric column is within specified range.
        
        Args:
            df: DataFrame to validate
            column: Column name to check
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            threshold: Minimum in-range percentage required (0-1)
            
        Returns:
            Validation result dictionary
        """
        total_count = df.count()
        
        # Build range condition
        condition = col(column).isNotNull()
        if min_value is not None:
            condition = condition & (col(column) >= min_value)
        if max_value is not None:
            condition = condition & (col(column) <= max_value)
        
        in_range_count = df.filter(condition).count()
        in_range_percentage = in_range_count / total_count if total_count > 0 else 0
        failed_count = total_count - in_range_count
        
        passed = in_range_percentage >= threshold
        
        return {
            "rule_type": "range_check",
            "column": column,
            "min_value": min_value,
            "max_value": max_value,
            "total_records": total_count,
            "in_range_records": in_range_count,
            "in_range_percentage": in_range_percentage,
            "threshold": threshold,
            "passed": passed,
            "failed_records": failed_count if not passed else 0
        }
    
    def run_custom_sql_check(
        self,
        df: DataFrame,
        custom_sql: str,
        temp_view_name: str = "dq_check_view"
    ) -> Dict[str, Any]:
        """
        Run custom SQL validation query.
        Expected: SQL should return a single row with 'passed' boolean column.
        
        Args:
            df: DataFrame to validate
            custom_sql: SQL query to execute
            temp_view_name: Temporary view name for the DataFrame
            
        Returns:
            Validation result dictionary
        """
        # Create temp view
        df.createOrReplaceTempView(temp_view_name)
        
        try:
            # Execute custom SQL
            result_df = self.spark.sql(custom_sql)
            result = result_df.collect()[0]
            
            passed = result["passed"] if "passed" in result.asDict() else False
            
            return {
                "rule_type": "custom_sql",
                "custom_sql": custom_sql,
                "passed": passed,
                "result": result.asDict()
            }
        except Exception as e:
            return {
                "rule_type": "custom_sql",
                "custom_sql": custom_sql,
                "passed": False,
                "error": str(e)
            }
    
    def validate_dataframe(
        self,
        df: DataFrame,
        rules: List[Dict[str, Any]],
        quarantine_failures: bool = True
    ) -> Dict[str, Any]:
        """
        Validate DataFrame against multiple DQ rules.
        
        Args:
            df: DataFrame to validate
            rules: List of rule dictionaries
            quarantine_failures: Whether to create quarantine DataFrame
            
        Returns:
            Comprehensive validation results
        """
        results = {
            "validation_timestamp": datetime.now().isoformat(),
            "total_records": df.count(),
            "rules_executed": 0,
            "rules_passed": 0,
            "rules_failed": 0,
            "rule_results": [],
            "quarantine_df": None
        }
        
        quarantine_conditions = []
        
        for rule in rules:
            rule_type = rule.get("rule_type")
            rule_name = rule.get("rule_name", rule_type)
            
            try:
                # Execute appropriate rule
                if rule_type == "null_check":
                    rule_result = self.run_null_check(
                        df,
                        rule["column"],
                        rule.get("threshold", 0.0)
                    )
                    # Add to quarantine condition
                    if quarantine_failures and not rule_result["passed"]:
                        quarantine_conditions.append(col(rule["column"]).isNull())
                
                elif rule_type == "duplicate_check":
                    rule_result = self.run_duplicate_check(
                        df,
                        rule["columns"],
                        rule.get("allow_duplicates", False)
                    )
                
                elif rule_type == "regex_check":
                    rule_result = self.run_regex_check(
                        df,
                        rule["column"],
                        rule["pattern"],
                        rule.get("threshold", 1.0)
                    )
                    # Add to quarantine condition
                    if quarantine_failures and not rule_result["passed"]:
                        from pyspark.sql.functions import regexp_extract
                        quarantine_conditions.append(
                            regexp_extract(col(rule["column"]).cast("string"), rule["pattern"], 0) == ""
                        )
                
                elif rule_type == "range_check":
                    rule_result = self.run_range_check(
                        df,
                        rule["column"],
                        rule.get("min_value"),
                        rule.get("max_value"),
                        rule.get("threshold", 1.0)
                    )
                    # Add to quarantine condition
                    if quarantine_failures and not rule_result["passed"]:
                        condition = col(rule["column"]).isNull()
                        if rule.get("min_value") is not None:
                            condition = condition | (col(rule["column"]) < rule["min_value"])
                        if rule.get("max_value") is not None:
                            condition = condition | (col(rule["column"]) > rule["max_value"])
                        quarantine_conditions.append(condition)
                
                elif rule_type == "custom_sql":
                    rule_result = self.run_custom_sql_check(
                        df,
                        rule["custom_sql"]
                    )
                
                else:
                    rule_result = {
                        "rule_type": rule_type,
                        "passed": False,
                        "error": f"Unknown rule type: {rule_type}"
                    }
                
                rule_result["rule_name"] = rule_name
                rule_result["severity"] = rule.get("severity", "error")
                
                results["rules_executed"] += 1
                if rule_result["passed"]:
                    results["rules_passed"] += 1
                else:
                    results["rules_failed"] += 1
                
                results["rule_results"].append(rule_result)
                
            except Exception as e:
                results["rule_results"].append({
                    "rule_name": rule_name,
                    "rule_type": rule_type,
                    "passed": False,
                    "error": str(e)
                })
                results["rules_executed"] += 1
                results["rules_failed"] += 1
        
        # Create quarantine DataFrame if requested
        if quarantine_failures and quarantine_conditions:
            # Combine all quarantine conditions with OR
            final_condition = quarantine_conditions[0]
            for condition in quarantine_conditions[1:]:
                final_condition = final_condition | condition
            
            results["quarantine_df"] = df.filter(final_condition)
            results["quarantine_count"] = results["quarantine_df"].count()
        
        return results
    
    def write_quarantine_table(
        self,
        quarantine_df: DataFrame,
        catalog: str,
        schema: str,
        table_name: str,
        validation_results: Dict[str, Any]
    ):
        """
        Write quarantined records to Delta table.
        
        Args:
            quarantine_df: DataFrame with failed records
            catalog: Catalog name
            schema: Schema name
            table_name: Quarantine table name
            validation_results: Results from validation
        """
        from pyspark.sql.functions import lit
        
        # Add quarantine metadata
        quarantine_df = quarantine_df.withColumn("_quarantine_timestamp", current_timestamp())
        quarantine_df = quarantine_df.withColumn("_validation_timestamp", lit(validation_results["validation_timestamp"]))
        quarantine_df = quarantine_df.withColumn("_rules_failed", lit(validation_results["rules_failed"]))
        
        # Write to Delta table
        full_table_name = f"{catalog}.{schema}.{table_name}"
        quarantine_df.write.format("delta").mode("append").saveAsTable(full_table_name)
    
    def generate_scorecard(
        self,
        validation_results: Dict[str, Any],
        catalog: str,
        schema: str,
        table_name: str,
        scorecard_table: str = "dq_scorecard"
    ):
        """
        Generate DQ scorecard and save to table.
        
        Args:
            validation_results: Results from validation
            catalog: Catalog name
            schema: Schema name
            table_name: Source table name
            scorecard_table: Scorecard table name
        """
        # Calculate overall score
        total_rules = validation_results["rules_executed"]
        passed_rules = validation_results["rules_passed"]
        score = (passed_rules / total_rules * 100) if total_rules > 0 else 0
        
        scorecard_data = [{
            "table_name": f"{catalog}.{schema}.{table_name}",
            "validation_timestamp": validation_results["validation_timestamp"],
            "total_records": validation_results["total_records"],
            "rules_executed": total_rules,
            "rules_passed": passed_rules,
            "rules_failed": validation_results["rules_failed"],
            "dq_score": score,
            "quarantine_count": validation_results.get("quarantine_count", 0)
        }]
        
        scorecard_df = self.spark.createDataFrame(scorecard_data)
        
        # Write to scorecard table
        full_scorecard_table = f"{catalog}.{schema}.{scorecard_table}"
        scorecard_df.write.format("delta").mode("append").saveAsTable(full_scorecard_table)


def create_dq_engine(spark: Optional[SparkSession] = None) -> DQEngine:
    """
    Factory function to create DQ engine.
    
    Args:
        spark: SparkSession (creates new one if None)
        
    Returns:
        DQEngine instance
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-DQ-Engine").getOrCreate()
    
    return DQEngine(spark)
