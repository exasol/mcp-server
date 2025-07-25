from dataclasses import dataclass
from typing import Any


@dataclass
class ExaDbObject:
    name: str
    comment: str | None

    @property
    def comment_decl(self) -> str:
        return f" COMMENT IS '{self.comment}'" if self.comment else ""


@dataclass
class ExaConstraint:
    type: str
    reference: str | None = None

    def decl(self, column_name: str) -> str:
        col_ref = f" REFERENCES {self.reference}" if self.reference else ""
        return f'{self.type} ("{column_name}"){col_ref}'


@dataclass
class ExaColumn(ExaDbObject):
    type: str
    constraint: ExaConstraint | None = None

    @property
    def primary_key(self) -> bool:
        return (self.constraint is not None) and (self.constraint.type == "PRIMARY KEY")

    @property
    def foreign_key(self) -> bool:
        return (self.constraint is not None) and (self.constraint.type == "FOREIGN KEY")

    def decl(self) -> str:
        column_decl = f'"{self.name}" {self.type}{self.comment_decl}'
        if self.constraint is not None:
            return ", ".join([column_decl, self.constraint.decl(self.name)])
        return column_decl


@dataclass
class ExaTable(ExaDbObject):
    columns: list[ExaColumn]
    rows: list[tuple[Any, ...]]

    def decl(self, schema_name: str) -> str:
        column_def = ", ".join(col.decl() for col in self.columns)
        return f'"{schema_name}"."{self.name}"({column_def}){self.comment_decl}'


@dataclass
class ExaView(ExaDbObject):
    sql: str

    def decl(self, schema_name: str) -> str:
        return (
            f'"{schema_name}"."{self.name}" AS '
            f"({self.sql.format(schema=schema_name)}){self.comment_decl}"
        )


@dataclass
class ExaParameter:
    type: str
    name: str


@dataclass
class ExaFunction(ExaDbObject):
    body: str
    inputs: list[ExaParameter]
    emits: list[ExaParameter] | None = None
    returns: str | None = None


@dataclass
class ExaSchema(ExaDbObject):
    is_new: bool

    @property
    def schema_name_arg(self) -> str:
        """
        Schema name to use in a call to a tool.
        """
        if self.is_new:
            return self.name
        return ""
