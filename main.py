"""
Global House Branch Extractor
Extracts all 97 branch data + location analysis using Google Maps APIs
(Geocoding API + Places Nearby Search, radius 1 km)
"""
import os
import httpx
import pandas as pd
import json
import time
import re
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
GOOGLE_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")

# ─── Province → Region mapping ───────────────────────────────────────────────
PROVINCE_REGION = {
    # BKK & Metro (กรุงเทพ + ปริมณฑล)
    "กรุงเทพมหานคร": "BKK&Metro", "กรุงเทพฯ": "BKK&Metro",
    "นนทบุรี": "BKK&Metro", "ปทุมธานี": "BKK&Metro",
    "สมุทรปราการ": "BKK&Metro", "นครปฐม": "BKK&Metro",
    "สมุทรสาคร": "BKK&Metro",
    # Central (ภาคกลาง)
    "พระนครศรีอยุธยา": "Central", "ลพบุรี": "Central",
    "สิงห์บุรี": "Central", "ชัยนาท": "Central",
    "สระบุรี": "Central", "อ่างทอง": "Central",
    "สุพรรณบุรี": "Central", "นครนายก": "Central",
    "ฉะเชิงเทรา": "Central", "ปราจีนบุรี": "Central",
    # Western (ภาคตะวันตก)
    "กาญจนบุรี": "Western", "ราชบุรี": "Western",
    "สมุทรสงคราม": "Western", "เพชรบุรี": "Western",
    "ประจวบคีรีขันธ์": "Western", "ตาก": "Western",
    # Eastern (ภาคตะวันออก)
    "ชลบุรี": "Eastern", "ระยอง": "Eastern",
    "จันทบุรี": "Eastern", "ตราด": "Eastern",
    "สระแก้ว": "Eastern",
    # Northern (ภาคเหนือ)
    "เชียงใหม่": "Northern", "เชียงราย": "Northern",
    "ลำปาง": "Northern", "ลำพูน": "Northern",
    "พะเยา": "Northern", "แพร่": "Northern",
    "น่าน": "Northern", "แม่ฮ่องสอน": "Northern",
    "อุตรดิตถ์": "Northern", "สุโขทัย": "Northern",
    "พิษณุโลก": "Northern", "พิจิตร": "Northern",
    "กำแพงเพชร": "Northern", "เพชรบูรณ์": "Northern",
    "นครสวรรค์": "Northern", "อุทัยธานี": "Northern",
    # Esan (ภาคตะวันออกเฉียงเหนือ)
    "นครราชสีมา": "Esan", "ขอนแก่น": "Esan",
    "อุดรธานี": "Esan", "อุบลราชธานี": "Esan",
    "เลย": "Esan", "หนองคาย": "Esan",
    "มหาสารคาม": "Esan", "กาฬสินธุ์": "Esan",
    "สกลนคร": "Esan", "นครพนม": "Esan",
    "ชัยภูมิ": "Esan", "ยโสธร": "Esan",
    "ร้อยเอ็ด": "Esan", "มุกดาหาร": "Esan",
    "สุรินทร์": "Esan", "ศรีสะเกษ": "Esan",
    "บุรีรัมย์": "Esan", "อำนาจเจริญ": "Esan",
    "หนองบัวลำภู": "Esan", "บึงกาฬ": "Esan",
    # Southern (ภาคใต้)
    "นครศรีธรรมราช": "Southern", "สุราษฎร์ธานี": "Southern",
    "พัทลุง": "Southern", "สงขลา": "Southern",
    "ปัตตานี": "Southern", "ยะลา": "Southern",
    "นราธิวาส": "Southern", "กระบี่": "Southern",
    "ตรัง": "Southern", "พังงา": "Southern",
    "ภูเก็ต": "Southern", "ระนอง": "Southern",
    "ชุมพร": "Southern", "สตูล": "Southern",
}

# ─── Helpers ─────────────────────────────────────────────────────────────────

def get_contact(contact_data: list, title: str) -> str:
    for c in contact_data:
        if c.get("title") == title:
            return c.get("detail", "")
    return ""


_ALL_PROVINCES = [
    # sorted longest-first to prevent partial matches
    "นครราชสีมา", "นครศรีธรรมราช", "นครสวรรค์", "นครพนม", "นครนายก", "นครปฐม",
    "พระนครศรีอยุธยา", "ประจวบคีรีขันธ์",
    "กรุงเทพมหานคร", "กรุงเทพฯ",
    "สุราษฎร์ธานี", "อุบลราชธานี", "สกลนคร",
    "สมุทรปราการ", "สมุทรสาคร", "สมุทรสงคราม",
    "มหาสารคาม", "กำแพงเพชร",
    "หนองบัวลำภู", "หนองคาย",
    "แม่ฮ่องสอน", "อำนาจเจริญ",
    "กาญจนบุรี", "กาฬสินธุ์", "ขอนแก่น", "อุดรธานี", "อุตรดิตถ์",
    "ชัยภูมิ", "ชัยนาท", "ชลบุรี", "ชุมพร",
    "เชียงใหม่", "เชียงราย",
    "ตราด", "ตาก",
    "นราธิวาส", "นนทบุรี", "น่าน",
    "บุรีรัมย์", "บึงกาฬ",
    "ปทุมธานี", "ปัตตานี", "ปราจีนบุรี",
    "พะเยา", "พัทลุง", "พิจิตร", "พิษณุโลก", "พังงา",
    "เพชรบุรี", "เพชรบูรณ์",
    "ภูเก็ต",
    "มุกดาหาร",
    "ยโสธร", "ยะลา",
    "ระนอง", "ระยอง", "ราชบุรี",
    "ร้อยเอ็ด",
    "ลพบุรี", "ลำปาง", "ลำพูน", "เลย",
    "ศรีสะเกษ", "สงขลา", "สตูล",
    "สระบุรี", "สระแก้ว", "สิงห์บุรี",
    "สุโขทัย", "สุพรรณบุรี", "สุรินทร์",
    "หนองคาย",
    "อ่างทอง",
    "แพร่",
    "จันทบุรี", "ฉะเชิงเทรา",
    "กระบี่", "ตรัง",
]
# deduplicate preserving order (longest first)
_seen = set()
_PROVINCES_SORTED: list[str] = []
for _p in _ALL_PROVINCES:
    if _p not in _seen:
        _seen.add(_p)
        _PROVINCES_SORTED.append(_p)
_PROVINCES_SORTED.sort(key=len, reverse=True)


def extract_province(address: str) -> str:
    """Find province by direct name search in address string."""
    clean = address.strip()
    for prov in _PROVINCES_SORTED:
        if prov in clean:
            return prov
    # Fallback: last Thai word before postcode
    no_postcode = re.sub(r"\s*\d{5}['\"]?\s*$", "", clean).strip()
    tokens = re.findall(r"[ก-๛]+", no_postcode)
    if tokens:
        raw = tokens[-1]
        # Strip จังหวัด prefix if present
        raw = re.sub(r"^จังหว[ัับ][ด]?", "", raw).strip()
        return raw
    return ""


def get_maps_link(lat: str, lon: str) -> str:
    return f"https://maps.google.com/?q={lat.strip()},{lon.strip()}"


# ─── Google Maps — Reverse Geocoding ─────────────────────────────────────────

def google_reverse_geocode(lat: str, lon: str) -> dict:
    """Call Google Geocoding API for road/area info. Returns first result or {}."""
    try:
        r = httpx.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={
                "latlng": f"{lat.strip()},{lon.strip()}",
                "language": "th",
                "key": GOOGLE_API_KEY,
            },
            timeout=12,
        )
        data = r.json()
        if data.get("status") == "OK" and data.get("results"):
            return data["results"][0]
        return {}
    except Exception:
        return {}


# ─── Google Maps — Places Nearby Search ──────────────────────────────────────

# Google place types → Thai label (priority order: most specific first)
_GPLACE_TYPE_TH: dict[str, str] = {
    "hospital": "โรงพยาบาล",
    "university": "มหาวิทยาลัย",
    "school": "โรงเรียน",
    "shopping_mall": "ห้างสรรพสินค้า",
    "department_store": "ห้างสรรพสินค้า",
    "supermarket": "ซูเปอร์มาร์เก็ต",
    "grocery_or_supermarket": "ซูเปอร์มาร์เก็ต",
    "hardware_store": "ร้านฮาร์ดแวร์",
    "home_goods_store": "ร้านของตกแต่งบ้าน",
    "furniture_store": "ร้านเฟอร์นิเจอร์",
    "gas_station": "ปั๊มน้ำมัน",
    "bank": "ธนาคาร",
    "atm": "ตู้ ATM",
    "pharmacy": "ร้านขายยา",
    "place_of_worship": "วัด/ศาสนสถาน",
    "market": "ตลาด",
    "park": "สวนสาธารณะ",
    "lodging": "ที่พัก/โรงแรม",
    "police": "สถานีตำรวจ",
    "restaurant": "ร้านอาหาร",
    "cafe": "คาเฟ่",
    "bakery": "เบเกอรี่",
    "bar": "บาร์",
    "food": "ร้านอาหาร",
    "convenience_store": "ร้านสะดวกซื้อ",
    "car_repair": "อู่ซ่อมรถ",
    "car_wash": "ร้านล้างรถ",
    "gym": "ฟิตเนส",
    "movie_theater": "โรงภาพยนตร์",
    "electronics_store": "ร้านอิเล็กทรอนิกส์",
    "clothing_store": "ร้านเสื้อผ้า",
    "beauty_salon": "ร้านเสริมสวย",
    "laundry": "ร้านซักรีด",
}

# Types that are too generic to be useful for categorization
_SKIP_TYPES = {
    "point_of_interest", "establishment", "premise",
    "street_address", "political", "locality", "sublocality",
    "route", "intersection", "store",
}

_TYPE_PRIORITY = list(_GPLACE_TYPE_TH.keys())


def _primary_type_th(types: list[str]) -> str:
    """Return the most meaningful Thai label from a list of Google place types."""
    for p in _TYPE_PRIORITY:
        if p in types:
            return _GPLACE_TYPE_TH[p]
    for t in types:
        if t not in _SKIP_TYPES and t in _GPLACE_TYPE_TH:
            return _GPLACE_TYPE_TH[t]
    return ""


def google_places_nearby(lat: str, lon: str, radius: int = 1000) -> list:
    """Call Google Places Nearby Search (Legacy). Returns list of place dicts."""
    try:
        r = httpx.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params={
                "location": f"{lat.strip()},{lon.strip()}",
                "radius": radius,
                "language": "th",
                "key": GOOGLE_API_KEY,
            },
            timeout=12,
        )
        data = r.json()
        status = data.get("status", "")
        if status not in ("OK", "ZERO_RESULTS"):
            print(f"  [Places API] status={status}", flush=True)
        return data.get("results", [])
    except Exception as e:
        print(f"  [Places API] error: {e}", flush=True)
        return []


# ─── Location Analysis (Google) ───────────────────────────────────────────────

def _road_description(road_name: str) -> str:
    """Classify a Thai road name into a road-type label."""
    if not road_name:
        return "ไม่พบข้อมูล"
    if "ทางหลวง" in road_name:
        return "ทางหลวงแผ่นดิน"
    if "มอเตอร์เวย์" in road_name or "ทางด่วน" in road_name or "expressway" in road_name.lower():
        return "ทางด่วน/มอเตอร์เวย์"
    if "ซอย" in road_name:
        return "ซอย (ถนนย่อย)"
    if "ถนน" in road_name or "road" in road_name.lower():
        return "ถนน"
    # If it looks like a highway number (e.g. "2", "12", "304")
    if re.fullmatch(r"\d+", road_name.strip()):
        return "ทางหลวง/ถนนหมายเลข"
    return "ถนน"


def analyze_location_google(geocode: dict, places: list) -> dict:
    # ── Road info from Geocoding ──────────────────────────────────────────────
    road_name = ""
    for comp in geocode.get("address_components", []):
        if "route" in comp.get("types", []):
            road_name = comp.get("long_name", "")
            break

    road_class = _road_description(road_name)
    road_detail = f"{road_name} ({road_class})" if road_name else "ไม่พบข้อมูล"

    # ── Nearby places from Places API ────────────────────────────────────────
    poi_categories: dict[str, int] = {}
    poi_names: list[str] = []

    for place in places:
        types = place.get("types", [])
        name = place.get("name", "")

        useful = [t for t in types if t not in _SKIP_TYPES]
        if not useful:
            continue

        label = _primary_type_th(useful)
        if label:
            poi_categories[label] = poi_categories.get(label, 0) + 1
        if name:
            poi_names.append(name)

    top_pois = sorted(poi_categories.items(), key=lambda x: -x[1])
    nearby_summary = ", ".join(f"{c}({n})" for c, n in top_pois[:7]) or "ไม่พบข้อมูล"
    nearby_places = ", ".join(list(dict.fromkeys(poi_names))[:10]) or "ไม่พบข้อมูล"

    # ── Location type (ทำเลเป็นแบบไหน) ────────────────────────────────────────
    cat_keys = {k for k, _ in top_pois}
    parts: list[str] = []

    if "ห้างสรรพสินค้า" in cat_keys or "ซูเปอร์มาร์เก็ต" in cat_keys:
        parts.append("ย่านพาณิชย์/ใกล้ห้าง")
    if "ร้านฮาร์ดแวร์" in cat_keys or "ร้านของตกแต่งบ้าน" in cat_keys or "ร้านเฟอร์นิเจอร์" in cat_keys:
        parts.append("ย่านค้าวัสดุ/ตกแต่งบ้าน")
    if "ตลาด" in cat_keys or "ร้านอาหาร" in cat_keys or "ร้านสะดวกซื้อ" in cat_keys:
        parts.append("ย่านชุมชน/ตลาด")
    if "โรงเรียน" in cat_keys or "มหาวิทยาลัย" in cat_keys:
        parts.append("ใกล้สถานศึกษา")
    if "โรงพยาบาล" in cat_keys:
        parts.append("ใกล้โรงพยาบาล")
    if "ปั๊มน้ำมัน" in cat_keys:
        parts.append("ใกล้ปั๊มน้ำมัน")
    if "ที่พัก/โรงแรม" in cat_keys:
        parts.append("ใกล้ที่พัก/โรงแรม")
    if not parts:
        parts.append("ย่านทั่วไป")

    return {
        "ทำเลเป็นแบบไหน": " / ".join(parts),
        "อยู่บนเส้นถนนแบบไหน": road_detail,
        "รอบๆมักเป็นอะไร": nearby_summary,
        "สถานที่ใกล้เคียง": nearby_places,
    }


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not GOOGLE_API_KEY:
        raise SystemExit(
            "ERROR: GOOGLE_MAPS_API_KEY environment variable is not set.\n"
            "  export GOOGLE_MAPS_API_KEY=your_key_here"
        )

    cache_file = Path("cache_google.json")

    cache: dict = {}
    if cache_file.exists():
        cache = json.loads(cache_file.read_text(encoding="utf-8"))
        print(f"Loaded {len(cache)} cached Google entries")

    print("Fetching branch data from API...")
    resp = httpx.get(
        "https://globalhouse.co.th/api/storefinder/storeDataOnline", timeout=30
    )
    branches = resp.json()["data"]
    print(f"Total branches: {len(branches)}")

    rows = []

    for i, b in enumerate(branches, 1):
        name = b.get("branch_name", "")
        print(f"[{i:02d}/{len(branches)}] {name}", flush=True)

        lat = b.get("branch_lut", "").strip()
        lon = b.get("branch_long", "").strip()
        address = b.get("branch_address", "")
        contact = b.get("contact_data", [])

        province = extract_province(address)
        region = PROVINCE_REGION.get(province, "Unknown")
        maps_link = get_maps_link(lat, lon)

        cache_key = f"{lat},{lon}"
        if cache_key in cache:
            loc = cache[cache_key]
        else:
            geocode = google_reverse_geocode(lat, lon)
            places = google_places_nearby(lat, lon, radius=1000)
            time.sleep(0.2)  # stay well within Google quota limits
            loc = analyze_location_google(geocode, places)
            cache[cache_key] = loc
            cache_file.write_text(
                json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        rows.append({
            "branch_code": b.get("branch_code", ""),
            "สาขา": name,
            "ที่อยู่": address,
            "จังหวัด": province,
            "ภูมิภาค": region,
            "postcode": b.get("postcode", ""),
            "ผอ.สาขา": get_contact(contact, "ผอ.สาขา"),
            "เคาน์เตอร์ขาย": get_contact(contact, "เคาน์เตอร์ขาย"),
            "แคชเชียร์โครงสร้าง": get_contact(contact, "แคชเชียร์โครงสร้าง"),
            "โทรศัพท์": get_contact(contact, "โทรศัพท์"),
            "เวลาบริการ": get_contact(contact, "เวลาบริการ"),
            "line": b.get("page_line", ""),
            "facebook": b.get("page_fb", ""),
            "เส้นทาง (Google Maps)": maps_link,
            "latitude": lat,
            "longitude": lon,
            "ทำเลเป็นแบบไหน": loc.get("ทำเลเป็นแบบไหน", ""),
            "อยู่บนเส้นถนนแบบไหน": loc.get("อยู่บนเส้นถนนแบบไหน", ""),
            "รอบๆมักเป็นอะไร": loc.get("รอบๆมักเป็นอะไร", ""),
            "สถานที่ใกล้เคียง": loc.get("สถานที่ใกล้เคียง", ""),
        })

    df = pd.DataFrame(rows)
    out_csv = "global_house_branches.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\nSaved {len(df)} branches to {out_csv}")

    # ─── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("จังหวัดละกี่สาขา")
    print("=" * 60)
    prov_count = df["จังหวัด"].value_counts().reset_index()
    prov_count.columns = ["จังหวัด", "จำนวนสาขา"]
    print(prov_count.to_string(index=False))

    print("\n" + "=" * 60)
    print("แยกตาม ภูมิภาค")
    print("=" * 60)
    region_order = ["BKK&Metro", "Esan", "Eastern", "Western", "Northern", "Southern", "Central", "Unknown"]
    region_count = df["ภูมิภาค"].value_counts().reindex(region_order).dropna().astype(int)
    for region, cnt in region_count.items():
        print(f"  {region}: {cnt} สาขา")

    prov_count.to_csv("summary_province.csv", index=False, encoding="utf-8-sig")
    region_df = region_count.reset_index()
    region_df.columns = ["ภูมิภาค", "จำนวนสาขา"]
    region_df.to_csv("summary_region.csv", index=False, encoding="utf-8-sig")

    print("\n" + "=" * 60)
    print("Province breakdown by Region")
    print("=" * 60)
    for region in region_order:
        subset = df[df["ภูมิภาค"] == region]
        if len(subset) == 0:
            continue
        provinces = subset["จังหวัด"].value_counts()
        print(f"\n{region} ({len(subset)} สาขา):")
        for prov, cnt in provinces.items():
            print(f"    {prov}: {cnt}")

    print("\nDone!")


if __name__ == "__main__":
    main()
