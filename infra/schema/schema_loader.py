import json
from pathlib import Path

class SchemaLoader:
    SCHEMA_JSON = Path(__file__).parent / "formula_one.json"

    @classmethod
    def get_table_schema(cls, table_name: str) -> dict:
        with open(cls.SCHEMA_JSON, "r") as f:
            schema = json.load(f)
            return schema.get(table_name)
