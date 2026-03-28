#!/usr/bin/env python3
import csv
from pathlib import Path

trades_dir = Path(__file__).parent

for md_file in trades_dir.glob("*.md"):
    lines = md_file.read_text().splitlines()
    
    # skip first two lines (description + empty line), data starts from line 3
    assert len(lines) >= 3, f"{md_file.name}: need at least 3 lines"
    header = lines[2].split('\t')
    
    csv_file = md_file.with_suffix('.csv')
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for line in lines[3:]:
            if line.strip():
                writer.writerow(line.split('\t'))
    
    print(f"{md_file.name} -> {csv_file.name}")
