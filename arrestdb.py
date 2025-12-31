import sqlite3
from datetime import datetime
import pandas as pd
import re

DB_PATH = "output.sqlite"

# Your CSV columns (exactly as provided)
CSV_COLS = [
    "CB_NO","CASE NUMBER","NAME","AGE","ARREST LOCATION","ARREST DATE","RACE",
    "CHARGE 1 STATUTE","CHARGE 1 DESCRIPTION","CHARGE 1 TYPE","CHARGE 1 CLASS",
    "CHARGE 2 STATUTE","CHARGE 2 DESCRIPTION","CHARGE 2 TYPE","CHARGE 2 CLASS",
    "CHARGE 3 STATUTE","CHARGE 3 DESCRIPTION","CHARGE 3 TYPE","CHARGE 3 CLASS",
    "CHARGE 4 STATUTE","CHARGE 4 DESCRIPTION","CHARGE 4 TYPE","CHARGE 4 CLASS",
    "CHARGES STATUTE","CHARGES DESCRIPTION","CHARGES TYPE","CHARGES CLASS",
]

def to_sql_col(col: str) -> str:
    # Normalize to safe SQLite identifiers
    # e.g. "ARREST LOCATION" -> "arrest_location"
    #      "CHARGE 1 STATUTE" -> "charge_1_statute"
    return re.sub(r"[^a-z0-9_]+", "_", col.strip().lower()).strip("_")

SQL_COLS = [to_sql_col(c) for c in CSV_COLS]

# Add metadata columns for durability + debugging
META_COLS = ["status", "error", "updated_at"]


class ArrestDB:
    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        # All user-data columns stored as TEXT for simplicity/robustness
        # (You can later cast types in queries if you want.)
        cols_def = ",\n  ".join([f"{c} TEXT" for c in SQL_COLS])
        meta_def = ",\n  ".join([
            "status TEXT",
            "error TEXT",
            "updated_at TEXT"
        ])

        self.conn.execute(f"""
        CREATE TABLE IF NOT EXISTS arrests (
          {cols_def},
          {meta_def},
          PRIMARY KEY (cb_no)
        )
        """)
        self.conn.commit()

    def upsert_row(self, row_dict: dict, status="OK", error=None):
        """
        row_dict keys are your original CSV column names.
        We'll map them to normalized SQL column names.
        """
        mapped = {to_sql_col(k): ("" if row_dict.get(k) is None else str(row_dict.get(k)))
                  for k in CSV_COLS}

        mapped["status"] = status
        mapped["error"] = "" if error is None else str(error)
        mapped["updated_at"] = datetime.utcnow().isoformat()

        cols = SQL_COLS + META_COLS
        placeholders = ", ".join(["?"] * len(cols))
        col_list = ", ".join(cols)

        # Update all columns on conflict
        update_set = ", ".join([f"{c}=excluded.{c}" for c in cols if c != "cb_no"])

        self.conn.execute(
            f"""
            INSERT INTO arrests ({col_list})
            VALUES ({placeholders})
            ON CONFLICT(cb_no) DO UPDATE SET
              {update_set}
            """,
            [mapped.get(c, "") for c in cols]
        )
        self.conn.commit()  # ✅ durability point: commit each row

    def close(self):
        self.conn.close()