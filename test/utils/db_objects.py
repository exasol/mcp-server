from dataclasses import dataclass
from itertools import chain
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
    columns: list[str]
    ref_table: str | None = None
    ref_columns: list[str] | None = None
    name: str | None = None

    def decl(self, schema_name: str) -> str:
        reference = ""
        col_list = ",".join(f'"{col}"' for col in self.columns)
        if self.type == "FOREIGN KEY" and self.ref_columns:
            ref_col_list = ",".join(f'"{col}"' for col in self.ref_columns)
            reference = (
                f' REFERENCES "{schema_name}"."{self.ref_table}"({ref_col_list})'
            )
        return f'CONSTRAINT {self.name or ""} {self.type} ({col_list}){reference}'


@dataclass
class ExaColumn(ExaDbObject):
    type: str

    def decl(self) -> str:
        return f'"{self.name}" {self.type}{self.comment_decl}'
        if self.constraint is not None:
            return ", ".join(
                [column_decl, self.constraint.decl(schema_name, self.name)]
            )
        return column_decl


@dataclass
class ExaTable(ExaDbObject):
    columns: list[ExaColumn]
    constraints: list[ExaConstraint]
    rows: list[tuple[Any, ...]]

    def decl(self, schema_name: str) -> str:
        column_decl = ", ".join(
            chain(
                (col.decl() for col in self.columns),
                (cons.decl(schema_name) for cons in self.constraints),
            )
        )
        return f'"{schema_name}"."{self.name}"({column_decl}){self.comment_decl}'


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
