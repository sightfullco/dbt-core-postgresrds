from dbt.contracts.util import (
    Mergeable
)
from dbt.dataclass_schema import dbtClassMixin, StrEnum
from dataclasses import dataclass
from typing import Optional

class EntityReference(object):
    def __init__(self, entity_name, package_name=None):
        self.entity_name = entity_name
        self.package_name = package_name

    def __str__(self):
        return f"{self.entity_name}"


class ResolvedEntityReference(EntityReference):
    """
    Simple proxy over an Entity which delegates property
    lookups to the underlying node. Also adds helper functions
    for working with metrics (ie. __str__ and templating functions)
    """

    def __init__(self, node, manifest, Relation):
        super().__init__(node.name, node.package_name)
        self.node = node
        self.manifest = manifest
        self.Relation = Relation

    def __getattr__(self, key):
        return getattr(self.node, key)

    def __str__(self):
        return f"{self.node.name}"


class EntityMutabilityType(StrEnum):
    """How data at the physical layer is expected to behave"""

    UNKNOWN = "UNKNOWN"
    IMMUTABLE = "IMMUTABLE"  # never changes
    APPEND_ONLY = "APPEND_ONLY"  # appends along an orderable column
    DS_APPEND_ONLY = "DS_APPEND_ONLY"  # appends along daily column
    FULL_MUTATION = "FULL_MUTATION"  # no guarantees, everything may change


@dataclass
class EntityMutabilityTypeParams(dbtClassMixin, Mergeable):
    """Type params add additional context to mutability"""

    min: Optional[str] = None
    max: Optional[str] = None
    update_cron: Optional[str] = None
    along: Optional[str] = None


@dataclass
class EntityMutability(dbtClassMixin):
    """Describes the mutability properties of a data source"""

    type: EntityMutabilityType
    type_params: Optional[EntityMutabilityTypeParams] = None