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
    result = dai.query(f"DESCRIBE {table_name}")
    df = result.df()
    # 只保留 column_name 和 column_type 两列
    df = df[['column_name', 'column_type']]
    return df.to_string(index=False)


def main():
    script_dir = Path(__file__).parent
    md_path = script_dir / "database_tables.md"
    output_dir = script_dir.parent

    table_names = parse_table_names(md_path)
    print(f"Found {len(table_names)} tables")

    table_col_counts = {}
    for table in table_names:
        print(f"Querying {table}...")
        result = dai.query(f"DESCRIBE {table}")
        df = result.df()
        col_count = len(df)
        table_col_counts[table] = col_count

        schema_df = df[['column_name', 'column_type']]
        output_path = output_dir / f"{table}.txt"
        with open(output_path, 'w') as f:
            f.write(schema_df.to_string(index=False))
        print(f"  -> {output_path} ({col_count} columns)")

    # 输出汇总
    summary_path = script_dir / "database_nums.md"
    with open(summary_path, 'w') as f:
        f.write("| table | columns |\n")
        f.write("| --- | --- |\n")
        for table, count in table_col_counts.items():
            f.write(f"| {table} | {count} |\n")
    print(f"\nSummary -> {summary_path}")


if __name__ == "__main__":
    main()
