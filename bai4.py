import glob, json, csv, os, sys
from pathlib import Path
from typing import List, Dict

DATA_DIR = Path(__file__).parent / "data"

def flatten(d: Dict, parent_key: str = "", sep: str = "_") -> Dict:
    items = []
    if isinstance(d, list):
        return {parent_key: "|".join(map(str, d))}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, "|".join(map(str, v))))
        else:
            items.append((new_key, v))
    return dict(items)

def json_to_csv(json_path: Path):
    csv_path = json_path.with_suffix(".csv")
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Đưa về list‑of‑dict
    records: List[Dict] = data if isinstance(data, list) else [data]
    flat_records = [flatten(rec) for rec in records]

    # Gộp tất cả key để làm header
    fieldnames = sorted({k for rec in flat_records for k in rec})

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in flat_records:
            writer.writerow(rec)

    print(f"✅ {json_path.relative_to(DATA_DIR.parent)}  →  {csv_path.name}")

def main():
    json_files = glob.glob(str(DATA_DIR / "**" / "*.json"), recursive=True)
    if not json_files:
        sys.exit("❌ Không tìm thấy file JSON nào dưới ./data")
    for jp in json_files:
        json_to_csv(Path(jp))

if __name__ == "__main__":
    main()
