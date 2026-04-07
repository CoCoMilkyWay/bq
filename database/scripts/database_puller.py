import dai
from pathlib import Path


def parse_table_names(md_path: Path) -> list[str]:
    with open(md_path) as f:
        lines = f.readlines()

    table_names = []
    for line in lines:
        if '|' not in line:
            continue
        parts = [p.strip() for p in line.split('|')]
        if len(parts) < 4:
            continue
        table_name = parts[3]
        if not table_name:
            continue
        if table_name == '英文名(dai)':
            continue
        if table_name.startswith('-'):
            continue
        table_names.append(table_name)
    return table_names


def query_table_schema(table_name: str) -> str:
    result = dai.query(f"SELECT * FROM {table_name} LIMIT 1", full_db_scan=True)
    df = result.df()

    lines = []
    lines.append(f"columns: {list(df.columns)}")
    lines.append(f"dtypes: {dict(df.dtypes)}")
    lines.append("")
    lines.append(df.to_string(index=False))
    return "\n".join(lines)


def main():
    script_dir = Path(__file__).parent
    md_path = script_dir / "database_tables.md"
    output_dir = script_dir.parent

    table_names = parse_table_names(md_path)
    print(f"Found {len(table_names)} tables")

    for table in table_names:
        print(f"Querying {table}...")
        schema = query_table_schema(table)
        output_path = output_dir / f"{table}.txt"
        with open(output_path, 'w') as f:
            f.write(schema)
        print(f"  -> {output_path}")


if __name__ == "__main__":
    main()
