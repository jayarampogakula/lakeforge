"""
LakeForge Governance Engine Module.
Manages centralized Unity Catalog access control, role-based grants, row filters, and column masks.
"""
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class PrincipalPrivilege:
    """Privileges granted to a principal on a specific securable object."""
    principal: str
    securable_type: str  # CATALOG, SCHEMA, TABLE, VIEW
    securable_name: str
    privileges: List[str]

    def to_grant_sql(self) -> str:
        """Generate Databricks Unity Catalog SQL GRANT statement."""
        privs = ", ".join(self.privileges)
        # Principal quoting if not already backticked
        principal_ident = f"`{self.principal}`" if not self.principal.startswith("`") else self.principal
        return f"GRANT {privs} ON {self.securable_type} {self.securable_name} TO {principal_ident}"


@dataclass
class AccessControlConfig:
    """Dataclass holding structured governance rules."""
    roles: Dict[str, Any] = field(default_factory=dict)
    clients: Dict[str, Any] = field(default_factory=dict)
    grants: List[PrincipalPrivilege] = field(default_factory=list)


class GovernanceEngine:
    """
    Centralized governance reconciler for Unity Catalog.
    Loads access control policies and reconciles grants across catalogs, schemas, tables, and views.
    """

    def __init__(self, spark=None):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def load_governance_config(
        config_path_or_dir: str,
        client_id: Optional[str] = None,
        environment: Optional[str] = None
    ) -> AccessControlConfig:
        """
        Load access control configuration from JSON file or directory.
        Supports multi-tenant client scoping and environment/stage catalog resolution.
        """
        path = Path(config_path_or_dir)
        raw_data = None

        if path.is_file():
            with open(path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        elif path.is_dir():
            # Check for client-specific governance first
            if client_id:
                client_gov = path / "clients" / client_id / "governance" / "access_control.json"
                if client_gov.is_file():
                    with open(client_gov, "r", encoding="utf-8") as f:
                        raw_data = json.load(f)

            if raw_data is None:
                gov_file = path / "governance" / "access_control.json"
                if gov_file.is_file():
                    with open(gov_file, "r", encoding="utf-8") as f:
                        raw_data = json.load(f)
                else:
                    gov_file = path / "access_control.json"
                    if gov_file.is_file():
                        with open(gov_file, "r", encoding="utf-8") as f:
                            raw_data = json.load(f)

        if not raw_data:
            raw_data = {"roles": {}, "clients": {}}

        # Resolve target stage catalog if environment/stage is supplied
        target_catalog_name = None
        if environment:
            try:
                from lakeforge.metadata.config_parser import ConfigParser
                env_cfg = ConfigParser.get_environment_config(config_path_or_dir, environment)
                target_catalog_name = env_cfg.get("catalog")
            except Exception:
                target_catalog_name = f"lakehouse_{environment}"

        roles = raw_data.get("roles", {})
        clients = raw_data.get("clients", {})

        # If client_id is requested and client has specific roles in the main file
        if client_id and client_id in clients:
            client_roles = clients[client_id].get("roles", {})
            # Merge client roles into active roles (client overrides or extends)
            combined_roles = dict(roles)
            combined_roles.update(client_roles)
            roles = combined_roles

        def resolve_securable(name: str) -> str:
            if not target_catalog_name:
                return name
            resolved = name.replace("{catalog}", target_catalog_name)
            if environment and environment != "dev" and resolved.startswith("lakeforge_dev"):
                resolved = resolved.replace("lakeforge_dev", target_catalog_name)
            return resolved

        grants: List[PrincipalPrivilege] = []

        # Parse roles into concrete grants
        for role_name, role_def in roles.items():
            principals = role_def.get("principals", [])
            catalogs = role_def.get("catalogs", {})
            schemas = role_def.get("schemas", {})
            tables = role_def.get("tables", {})
            views = role_def.get("views", {})

            for principal in principals:
                for cat_name, privs in catalogs.items():
                    grants.append(
                        PrincipalPrivilege(
                            principal=principal,
                            securable_type="CATALOG",
                            securable_name=resolve_securable(cat_name),
                            privileges=privs if isinstance(privs, list) else [privs]
                        )
                    )
                for schema_name, privs in schemas.items():
                    grants.append(
                        PrincipalPrivilege(
                            principal=principal,
                            securable_type="SCHEMA",
                            securable_name=resolve_securable(schema_name),
                            privileges=privs if isinstance(privs, list) else [privs]
                        )
                    )
                for table_name, privs in tables.items():
                    grants.append(
                        PrincipalPrivilege(
                            principal=principal,
                            securable_type="TABLE",
                            securable_name=resolve_securable(table_name),
                            privileges=privs if isinstance(privs, list) else [privs]
                        )
                    )
                for view_name, privs in views.items():
                    grants.append(
                        PrincipalPrivilege(
                            principal=principal,
                            securable_type="VIEW",
                            securable_name=resolve_securable(view_name),
                            privileges=privs if isinstance(privs, list) else [privs]
                        )
                    )

        return AccessControlConfig(
            roles=roles,
            clients=clients,
            grants=grants
        )

    def generate_grant_ddl(
        self,
        config: AccessControlConfig,
        target_catalog: Optional[str] = None
    ) -> List[str]:
        """
        Generate SQL GRANT DDL statements from an AccessControlConfig.
        Optionally filter by target_catalog.
        """
        statements = []
        for grant in config.grants:
            if target_catalog:
                # Filter if the securable does not start with target_catalog
                if not grant.securable_name.startswith(target_catalog):
                    continue
            statements.append(grant.to_grant_sql())
        return statements

    def reconcile_permissions(
        self,
        config_path_or_dir: str,
        environment: str = "dev",
        dry_run: bool = False,
        target_catalog: Optional[str] = None,
        client_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Reconcile and enforce access control permissions.
        
        Args:
            config_path_or_dir: Root config path or directory
            environment: dev, test, prod
            dry_run: If True, generate statements without executing them
            target_catalog: Restrict reconciliation to a specific catalog
            client_id: Tenant or customer scope
            
        Returns:
            Dict containing execution summary metrics and DDL statements.
        """
        config = self.load_governance_config(config_path_or_dir, client_id=client_id, environment=environment)
        statements = self.generate_grant_ddl(config, target_catalog=target_catalog)

        executed = 0
        failed = 0
        errors = []

        self.logger.info(
            f"Governance reconciliation initiated (env={environment}, client={client_id}, "
            f"dry_run={dry_run}, grants_to_process={len(statements)})"
        )

        for stmt in statements:
            if dry_run or self.spark is None:
                self.logger.info(f"[DRY-RUN] Would execute: {stmt}")
                executed += 1
            else:
                try:
                    self.logger.info(f"Executing: {stmt}")
                    self.spark.sql(stmt)
                    executed += 1
                except Exception as ex:
                    msg = f"Failed to execute '{stmt}': {str(ex)}"
                    self.logger.warning(msg)
                    errors.append(msg)
                    failed += 1

        return {
            "environment": environment,
            "client_id": client_id,
            "dry_run": dry_run,
            "total_statements": len(statements),
            "executed": executed,
            "failed": failed,
            "errors": errors,
            "statements": statements
        }


def create_governance_engine(spark=None) -> GovernanceEngine:
    """Factory helper to create GovernanceEngine."""
    return GovernanceEngine(spark=spark)
