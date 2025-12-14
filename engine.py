# engine.py
import csv
from typing import List, Dict, Any, Optional

class MiniSQLEngine:
    def __init__(self):
        self.tables: Dict[str, List[Dict[str, Any]]] = {}

    def load_table(self, csv_path: str) -> str:
        table_name = csv_path.split("/")[-1].removesuffix(".csv").lower()
        
        rows = []
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            if not fieldnames:
                raise ValueError("CSV file is empty or has no headers")
                
            for row in reader:
                cleaned_row = {}
                for key, value in row.items():
                    value = value.strip()
                    if value == "":
                        cleaned_row[key] = None
                    elif value.isdigit():
                        cleaned_row[key] = int(value)
                    else:
                        try:
                            cleaned_row[key] = float(value)
                        except ValueError:
                            cleaned_row[key] = value
                rows.append(cleaned_row)

        self.tables[table_name] = rows
        print(f"Loaded table '{table_name}' with {len(rows)} rows.")
        return table_name

    def execute(self, query: str) -> List[Dict[str, Any]]:
        parsed = parse_query(query.strip().rstrip(";"))
        table_name = parsed["from"].lower()  # Make table name case-insensitive
        
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found. Available: {list(self.tables.keys())}")
        
        data = self.tables[table_name]
        
        # Create a lowercase to original key mapping for case-insensitive column access
        sample_row = data[0] if data else {}
        col_map = {k.lower(): k for k in sample_row.keys()}
        
        # Apply WHERE filter
        filtered = data
        if parsed["where"]:
            col, op, val = parsed["where"]
            actual_col = col_map.get(col.lower())
            if actual_col is None:
                raise ValueError(f"Column '{col}' not found in table '{table_name}'")
            
            filtered = []
            for row in data:
                row_val = row[actual_col]
                if apply_condition(row_val, op, val):
                    filtered.append(row)
        
        # Handle aggregation (COUNT)
        if parsed["aggregate"]:
            agg_func, agg_col = parsed["aggregate"]
            if agg_func == "COUNT":
                if agg_col == "*":
                    count = len(filtered)
                else:
                    actual_agg_col = col_map.get(agg_col.lower())
                    if actual_agg_col is None:
                        raise ValueError(f"Column '{agg_col}' not found in table '{table_name}'")
                    count = sum(1 for row in filtered if row.get(actual_agg_col) is not None)
                return [{"COUNT": count}]
        
        # Projection (SELECT)
        result = []
        select_cols = parsed["select"]
        for row in filtered:
            if "*" in select_cols:
                result.append(row.copy())
            else:
                projected = {}
                for col in select_cols:
                    actual_col = col_map.get(col.lower())
                    if actual_col is not None:
                        projected[col] = row[actual_col]  # Keep original column name casing in output
                result.append(projected)
        
        return result


def apply_condition(left: Any, op: str, right: Any) -> bool:
    if op == "=": return left == right
    if op == "!=": return left != right
    if op == ">": return left > right
    if op == "<": return left < right
    if op == ">=": return left >= right
    if op == "<=": return left <= right
    raise ValueError(f"Unsupported operator: {op}")


def parse_query(query: str) -> Dict:
    query = query.upper()
    parts = query.split()
    if not parts or parts[0] != "SELECT":
        raise ValueError("Query must start with SELECT")
    
    try:
        from_idx = parts.index("FROM")
        where_idx = parts.index("WHERE") if "WHERE" in parts else len(parts)
    except ValueError:
        raise ValueError("Query must contain FROM clause")
    
    select_part = " ".join(parts[1:from_idx])
    table_name = parts[from_idx + 1]
    where_part = " ".join(parts[where_idx + 1:]) if where_idx < len(parts) else ""
    
    # Parse SELECT for COUNT or regular columns
    if select_part.startswith("COUNT("):
        if not select_part.endswith(")"):
            raise ValueError("Invalid COUNT syntax")
        inside = select_part[6:-1].strip()
        agg_col = "*" if inside == "*" else inside
        return {
            "select": [],
            "from": table_name,
            "where": parse_where(where_part) if where_part else None,
            "aggregate": ("COUNT", agg_col)
        }
    else:
        cols = [col.strip() for col in select_part.split(",")]
        return {
            "select": cols,
            "from": table_name,
            "where": parse_where(where_part) if where_part else None,
            "aggregate": None
        }


def parse_where(where_part: str) -> Optional[tuple]:
    if not where_part:
        return None
    
    for op in ["!=", ">=", "<=", "=", ">", "<"]:
        if op in where_part:
            col_val = where_part.split(op, 1)
            if len(col_val) != 2:
                continue
            col = col_val[0].strip()
            val_str = col_val[1].strip().strip("'\"")
            if val_str.isdigit():
                val = int(val_str)
            else:
                try:
                    val = float(val_str)
                except ValueError:
                    val = val_str
            return (col, op, val)
    
    raise ValueError(f"Invalid or unsupported WHERE clause: {where_part}")