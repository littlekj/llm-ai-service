from datetime import datetime


def parse_iso_datetime(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except:
        return datetime.min
    
    
print(f"parse_iso_datetime:", parse_iso_datetime("2025-09-29T02:46:39.954374"))