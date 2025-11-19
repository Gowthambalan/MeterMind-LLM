# import os
# import json
# import subprocess
# import re
# from datetime import datetime
# import requests

# # === PATHS ===
# input_folder = "20250719"                     # Input folder with subfolders
# output_folder = "20250719_converted_jsons"    # Output folder with same structure
# os.makedirs(output_folder, exist_ok=True)

# # === LOGGING ===
# total_files = 0
# converted_files = 0
# failed_files = 0
# failed_list = []

# # === WEATHER CACHE ===
# weather_cache_file = "weather_cache.json"
# weather_cache = {}
# if os.path.exists(weather_cache_file):
#     with open(weather_cache_file, "r", encoding="utf-8") as f:
#         weather_cache = json.load(f)

# # === LOCATION MAP (expand as needed) ===
# location_map = {
#     "NTB18.00": {
#         "lat": 9.9312,
#         "lon": 76.2673,
#         "city": "Thiruvananthapuram",
#         "state": "Kerala",
#         "country": "India"
#     }
# }

# # === HELPERS ===
# def iso_format(datetime_str):
#     """Convert DD-MM-YYYY HH:MM:SS → ISO 8601 Zulu"""
#     try:
#         dt = datetime.strptime(datetime_str, "%d-%m-%Y %H:%M:%S")
#         return dt.isoformat() + "Z"
#     except:
#         return datetime_str

# def process_d3_to_events(d3_list):
#     """Convert d3 readings into events"""
#     events = []
#     for entry in d3_list:
#         mechanism = entry.get("mechanism") or "AUTO"
#         for r in entry.get("readings", []):
#             try:
#                 value = float(r.get("VALUE", 0))
#             except:
#                 value = 0
#             events.append({
#                 "event_type": r.get("type"),
#                 "mechanism": mechanism,
#                 "param_code": r.get("PARAMCODE"),
#                 "value": value,
#                 "unit": r.get("UNIT", "")
#             })
#     return events

# def safe_json_loads(text):
#     """Extract JSON from LLM response"""
#     match = re.search(r'\{.*\}', text, re.DOTALL)
#     if match:
#         try:
#             return json.loads(match.group(0))
#         except:
#             return None
#     return None

# def convert_asset_info_with_ollama(d1_data):
#     """Use Ollama (deepseekr1:latest) to map asset_info"""
#     prompt = f"""
# Map this JSON object to asset_info with fields:
# asset_id, location_code, voltage_class, standard, installation_date (ISO format)
# JSON:
# {json.dumps(d1_data)}
# """
#     try:
#         result = subprocess.run(
#             ["ollama", "generate", "deepseekr1:latest", "--prompt", prompt, "--max-tokens", "500"],
#             capture_output=True, text=True, timeout=15
#         )
#         return safe_json_loads(result.stdout)
#     except Exception as e:
#         print(f" Ollama call failed: {e}")
#         return None

# # === WEATHER ===
# def get_weather_and_location(location_code, timestamp):
#     """Fetch dynamic weather from Open-Meteo (cached)"""
#     loc = location_map.get(location_code)
#     if not loc:
#         return None, None

#     lat, lon = loc["lat"], loc["lon"]

#     # Weather code mapping
#     weather_map = {
#         0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
#         45: "Fog", 48: "Depositing rime fog",
#         51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
#         61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
#         71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
#         80: "Rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
#         95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Heavy thunderstorm with hail"
#     }

#     try:
#         dt = datetime.fromisoformat(timestamp.replace("Z", ""))
#         date_str = dt.strftime("%Y-%m-%d")
#         hour = dt.hour

#         cache_key = f"{location_code}_{date_str}_{hour}"
#         if cache_key in weather_cache:
#             return weather_cache[cache_key]["weather"], weather_cache[cache_key]["location"]

#         url = (
#             f"https://api.open-meteo.com/v1/forecast?"
#             f"latitude={lat}&longitude={lon}"
#             f"&hourly=temperature_2m,relative_humidity_2m,precipitation,"
#             f"shortwave_radiation,windspeed_10m,weathercode"
#             f"&start_date={date_str}&end_date={date_str}"
#         )
#         resp = requests.get(url, timeout=10)
#         data = resp.json()

#         times = data["hourly"]["time"]
#         idx = times.index(f"{date_str}T{hour:02d}:00")

#         code = data["hourly"]["weathercode"][idx]
#         condition = weather_map.get(code, f"Unknown ({code})")

#         weather = {
#             "temperature_c": data["hourly"]["temperature_2m"][idx],
#             "humidity_pct": data["hourly"]["relative_humidity_2m"][idx],
#             "rainfall_mm": data["hourly"]["precipitation"][idx],
#             "solar_irradiance_wm2": data["hourly"]["shortwave_radiation"][idx],
#             "wind_speed_ms": data["hourly"]["windspeed_10m"][idx],
#             "condition": condition
#         }

#         location = {
#             "substation": location_code,
#             "latitude": lat,
#             "longitude": lon,
#             "city": loc["city"],
#             "state": loc["state"],
#             "country": loc["country"]
#         }

#         weather_cache[cache_key] = {"weather": weather, "location": location}

#         return weather, location
#     except Exception as e:
#         print(f" Weather fetch failed for {location_code}: {e}")
#         return None, None

# # === MAIN CONVERSION ===
# def convert_file(input_path, output_path):
#     global converted_files, failed_files
#     try:
#         with open(input_path, "r", encoding="utf-8") as f:
#             raw_json = json.load(f)

#         converted = {
#             "utility_code": raw_json.get("utility_code"),
#             "discom": raw_json.get("d1", {}).get("discom"),
#             "meter_type": raw_json.get("d1", {}).get("meter_type"),
#             "modem_serial_number": raw_json.get("d1", {}).get("modem_serial_number")
#         }

#         asset_info = convert_asset_info_with_ollama(raw_json.get("d1", {}))
#         if not asset_info:
#             d1 = raw_json.get("d1", {})
#             asset_info = {
#                 "asset_id": d1.get("g1"),
#                 "location_code": d1.get("g17"),
#                 "voltage_class": d1.get("g15"),
#                 "standard": d1.get("g31"),
#                 "installation_date": iso_format(d1.get("g2", ""))
#             }
#         converted["asset_info"] = asset_info

#         d3_list = raw_json.get("d3", [])
#         if d3_list:
#             converted["timestamp"] = iso_format(d3_list[0].get("datetime", ""))

#         converted["events"] = process_d3_to_events(d3_list)

#         # Weather + location
#         if asset_info.get("location_code") and converted.get("timestamp"):
#             weather, location = get_weather_and_location(asset_info["location_code"], converted["timestamp"])
#             if weather and location:
#                 converted["weather"] = weather
#                 converted["location"] = location

#         os.makedirs(os.path.dirname(output_path), exist_ok=True)
#         with open(output_path, "w", encoding="utf-8") as f:
#             json.dump(converted, f, indent=2)

#         converted_files += 1
#         print(f" Converted: {input_path}")

#     except Exception as e:
#         failed_files += 1
#         failed_list.append(input_path)
#         print(f" Failed: {input_path} → {e}")

# # === WALK FOLDER ===
# for root, dirs, files in os.walk(input_folder):
#     for file in files:
#         if file.endswith(".json"):
#             total_files += 1
#             input_path = os.path.join(root, file)
#             relative_path = os.path.relpath(root, input_folder)
#             output_dir = os.path.join(output_folder, relative_path)
#             output_path = os.path.join(output_dir, file)
#             convert_file(input_path, output_path)

# # === SAVE CACHE ===
# with open(weather_cache_file, "w", encoding="utf-8") as f:
#     json.dump(weather_cache, f, indent=2)

# # === SUMMARY ===
# print("\n=== Conversion Summary ===")
# print(f"Total files: {total_files}")
# print(f"Successfully converted: {converted_files}")
# print(f"Failed: {failed_files}")
# if failed_list:
#     print("Failed files:")
#     for f in failed_list:
#         print(f" - {f}")


# # ===================================================most correct below========================
# import os
# import json
# import subprocess
# import re
# from datetime import datetime
# import requests

# # === PATHS ===
# input_folder = "20250719"                     # Input folder with subfolders
# output_folder = "20250719_converted_jsons"    # Output folder with same structure
# os.makedirs(output_folder, exist_ok=True)

# # === LOGGING ===
# total_files = 0
# converted_files = 0
# failed_files = 0
# failed_list = []

# # === WEATHER CACHE ===
# weather_cache_file = "weather_cache.json"
# weather_cache = {}
# if os.path.exists(weather_cache_file):
#     with open(weather_cache_file, "r", encoding="utf-8") as f:
#         weather_cache = json.load(f)

# # === LOCATION MAP ===
# location_map = {
#     "NTB18.00": {
#         "lat": 9.9312,
#         "lon": 76.2673,
#         "city": "Thiruvananthapuram",
#         "state": "Kerala",
#         "country": "India"
#     }
# }

# # === HELPERS ===
# def iso_format(datetime_str):
#     """Convert DD-MM-YYYY HH:MM:SS → ISO 8601 Zulu"""
#     try:
#         dt = datetime.strptime(datetime_str, "%d-%m-%Y %H:%M:%S")
#         return dt.isoformat() + "Z"
#     except:
#         return datetime_str

# def safe_json_loads(text):
#     """Extract JSON from LLM response"""
#     match = re.search(r'\{.*\}', text, re.DOTALL)
#     if match:
#         try:
#             return json.loads(match.group(0))
#         except:
#             return None
#     return None

# # === OLLAMA: asset_info ONLY ===
# def convert_asset_info_with_ollama(d1_data):
#     """Use Ollama to generate asset_info only"""
#     prompt = f"""
# Map this JSON object to asset_info with fields:
# asset_id, location_code, voltage_class, standard, installation_date (ISO format).
# Return only valid JSON object.
# JSON:
# {json.dumps(d1_data)}
# """
#     try:
#         result = subprocess.run(
#             ["ollama", "generate", "deepseekr1:latest", "--prompt", prompt, "--max-tokens", "500"],
#             capture_output=True, text=True, timeout=20
#         )
#         return safe_json_loads(result.stdout)
#     except Exception as e:
#         print(f" Ollama call failed: {e}")
#         return None

# # === SCRIPT-BASED: measurements ===
# def process_d2_to_measurements(d2_list):
#     """Convert d2 readings into full measurement structure"""
#     voltages = {}
#     currents = {}
#     frequency = {"value": None, "unit": ""}
#     power_factors = {"avg": None, "per_phase": {}}
#     energy_kwh = {"import": 0, "export": 0, "reactive": 0, "cumulative": 0}

#     for entry in d2_list:
#         code = entry.get("PARAMCODE", "")
#         try:
#             val = float(entry.get("VALUE", 0))
#         except:
#             val = 0
#         unit = entry.get("UNIT", "")

#         if code == "V_R":
#             voltages["phase_r"] = {"value": val, "unit": unit}
#         elif code == "V_Y":
#             voltages["phase_y"] = {"value": val, "unit": unit}
#         elif code == "V_B":
#             voltages["phase_b"] = {"value": val, "unit": unit}
#         elif code == "I_R":
#             currents["phase_r"] = {"value": val, "unit": unit}
#         elif code == "I_Y":
#             currents["phase_y"] = {"value": val, "unit": unit}
#         elif code == "I_B":
#             currents["phase_b"] = {"value": val, "unit": unit}
#         elif code == "FREQ":
#             frequency = {"value": val, "unit": unit}
#         elif code == "PF_R":
#             power_factors["per_phase"]["r"] = val
#         elif code == "PF_Y":
#             power_factors["per_phase"]["y"] = val
#         elif code == "PF_B":
#             power_factors["per_phase"]["b"] = val
#         elif code == "ENERGY_IMPORT":
#             energy_kwh["import"] = val
#         elif code == "ENERGY_EXPORT":
#             energy_kwh["export"] = val
#         elif code == "ENERGY_REACTIVE":
#             energy_kwh["reactive"] = val
#         elif code == "ENERGY_CUMULATIVE":
#             energy_kwh["cumulative"] = val

#     # Compute avg PF
#     if power_factors["per_phase"]:
#         power_factors["avg"] = round(sum(power_factors["per_phase"].values()) / len(power_factors["per_phase"]), 2)

#     return {
#         "voltages": voltages,
#         "currents": currents,
#         "frequency": frequency,
#         "power_factors": power_factors,
#         "energy_kwh": energy_kwh
#     }

# # === SCRIPT-BASED: events ===
# def process_d3_to_events(d3_list):
#     events = []
#     for entry in d3_list:
#         mech = entry.get("mechanism", "AUTO")
#         for r in entry.get("readings", []):
#             try:
#                 val = float(r.get("VALUE", 0))
#             except:
#                 val = 0
#             events.append({
#                 "event_type": r.get("type"),
#                 "mechanism": mech,
#                 "param_code": r.get("PARAMCODE"),
#                 "value": val,
#                 "unit": r.get("UNIT", "")
#             })
#     return events

# # === WEATHER + LOCATION ===
# def get_weather_and_location(location_code, timestamp):
#     loc = location_map.get(location_code)
#     if not loc:
#         return None, None

#     lat, lon = loc["lat"], loc["lon"]
#     weather_map = {
#         0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
#         45: "Fog", 48: "Depositing rime fog",
#         51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
#         61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
#         71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
#         80: "Rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
#         95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Heavy thunderstorm with hail"
#     }

#     try:
#         dt = datetime.fromisoformat(timestamp.replace("Z", ""))
#         date_str = dt.strftime("%Y-%m-%d")
#         hour = dt.hour
#         cache_key = f"{location_code}_{date_str}_{hour}"
#         if cache_key in weather_cache:
#             return weather_cache[cache_key]["weather"], weather_cache[cache_key]["location"]

#         url = (
#             f"https://api.open-meteo.com/v1/forecast?"
#             f"latitude={lat}&longitude={lon}"
#             f"&hourly=temperature_2m,relative_humidity_2m,precipitation,"
#             f"shortwave_radiation,windspeed_10m,weathercode"
#             f"&start_date={date_str}&end_date={date_str}"
#         )
#         resp = requests.get(url, timeout=10)
#         data = resp.json()
#         times = data["hourly"]["time"]
#         idx = times.index(f"{date_str}T{hour:02d}:00")
#         code = data["hourly"]["weathercode"][idx]
#         condition = weather_map.get(code, f"Unknown ({code})")

#         weather = {
#             "temperature_c": data["hourly"]["temperature_2m"][idx],
#             "humidity_pct": data["hourly"]["relative_humidity_2m"][idx],
#             "rainfall_mm": data["hourly"]["precipitation"][idx],
#             "solar_irradiance_wm2": data["hourly"]["shortwave_radiation"][idx],
#             "wind_speed_ms": data["hourly"]["windspeed_10m"][idx],
#             "condition": condition
#         }

#         location = {
#             "substation": location_code,
#             "latitude": lat,
#             "longitude": lon,
#             "city": loc["city"],
#             "state": loc["state"],
#             "country": loc["country"]
#         }

#         weather_cache[cache_key] = {"weather": weather, "location": location}
#         return weather, location
#     except Exception as e:
#         print(f" Weather fetch failed for {location_code}: {e}")
#         return None, None

# # === MAIN CONVERSION ===
# def convert_file(input_path, output_path):
#     global converted_files, failed_files
#     try:
#         with open(input_path, "r", encoding="utf-8") as f:
#             raw_json = json.load(f)

#         converted = {
#             "utility_code": raw_json.get("utility_code"),
#             "discom": raw_json.get("d1", {}).get("discom"),
#             "meter_type": raw_json.get("d1", {}).get("meter_type"),
#             "modem_serial_number": raw_json.get("d1", {}).get("modem_serial_number")
#         }

#         # --- asset_info via Ollama ---
#         asset_info = convert_asset_info_with_ollama(raw_json.get("d1", {}))
#         if not asset_info:
#             d1 = raw_json.get("d1", {})
#             asset_info = {
#                 "asset_id": d1.get("g1"),
#                 "location_code": d1.get("g17"),
#                 "voltage_class": d1.get("g15"),
#                 "standard": d1.get("g31"),
#                 "installation_date": iso_format(d1.get("g2", ""))
#             }
#         converted["asset_info"] = asset_info

#         # --- measurements ---
#         converted["measurements"] = process_d2_to_measurements(raw_json.get("d2", []))

#         # --- events ---
#         converted["events"] = process_d3_to_events(raw_json.get("d3", []))
#         if raw_json.get("d3"):
#             converted["timestamp"] = iso_format(raw_json["d3"][0].get("datetime", ""))

#         # --- weather + location ---
#         if asset_info.get("location_code") and converted.get("timestamp"):
#             weather, location = get_weather_and_location(asset_info["location_code"], converted["timestamp"])
#             if weather and location:
#                 converted["weather"] = weather
#                 converted["location"] = location

#         os.makedirs(os.path.dirname(output_path), exist_ok=True)
#         with open(output_path, "w", encoding="utf-8") as f:
#             json.dump(converted, f, indent=2)

#         converted_files += 1
#         print(f" Converted: {input_path}")

#     except Exception as e:
#         failed_files += 1
#         failed_list.append(input_path)
#         print(f" Failed: {input_path} → {e}")

# # === WALK FOLDER ===
# for root, dirs, files in os.walk(input_folder):
#     for file in files:
#         if file.endswith(".json"):
#             total_files += 1
#             input_path = os.path.join(root, file)
#             relative_path = os.path.relpath(root, input_folder)
#             output_dir = os.path.join(output_folder, relative_path)
#             output_path = os.path.join(output_dir, file)
#             convert_file(input_path, output_path)

# # === SAVE WEATHER CACHE ===
# with open(weather_cache_file, "w", encoding="utf-8") as f:
#     json.dump(weather_cache, f, indent=2)

# # === SUMMARY ===
# print("\n=== Conversion Summary ===")
# print(f"Total files: {total_files}")
# print(f"Successfully converted: {converted_files}")
# print(f"Failed: {failed_files}")
# if failed_list:
#     print("Failed files:")
#     for f in failed_list:
#         print(f" - {f}")


import os
import json
import subprocess
import re
from datetime import datetime
import requests

# === PATHS ===
input_folder = "BI"                     # Input folder with subfolders
output_folder = "BI_converted_jsons"    # Output folder with same structure
os.makedirs(output_folder, exist_ok=True)

# === LOGGING ===
total_files = 0
converted_files = 0
failed_files = 0
failed_list = []

# === WEATHER CACHE ===
weather_cache_file = "weather_cache.json"
weather_cache = {}
if os.path.exists(weather_cache_file):
    with open(weather_cache_file, "r", encoding="utf-8") as f:
        weather_cache = json.load(f)

# === LOCATION MAP ===
location_map = {
    "NTB18.00": {
        "lat": 9.9312,
        "lon": 76.2673,
        "city": "Thiruvananthapuram",
        "state": "Kerala",
        "country": "India"
    }
}

# === HELPERS ===
def iso_format(datetime_str):
    """Convert DD-MM-YYYY HH:MM:SS → ISO 8601 Zulu"""
    try:
        dt = datetime.strptime(datetime_str, "%d-%m-%Y %H:%M:%S")
        return dt.isoformat() + "Z"
    except:
        return datetime_str

def safe_json_loads(text):
    """Extract JSON from LLM response"""
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except:
            return None
    return None

# === OLLAMA: asset_info ONLY ===
def convert_asset_info_with_ollama(d1_data):
    """Use Ollama to generate asset_info only"""
    prompt = f"""
Map this JSON object to asset_info with fields:
asset_id, location_code, voltage_class, standard, installation_date (ISO format).
Return only valid JSON object.
JSON:
{json.dumps(d1_data)}
"""
    try:
        result = subprocess.run(
            ["ollama", "generate", "deepseekr1:latest", "--prompt", prompt, "--max-tokens", "500"],
            capture_output=True, text=True, timeout=20
        )
        return safe_json_loads(result.stdout)
    except Exception as e:
        print(f"❌ Ollama call failed: {e}")
        return None

# === MEASUREMENT MAPPINGS ===
D2_MAPPING = {
    "P1-2-1-1-0": ("voltages", "phase_r"),
    "P1-2-2-1-0": ("voltages", "phase_y"),
    "P1-2-3-1-0": ("voltages", "phase_b"),
    "P2-1-1-1-0": ("currents", "phase_r"),
    "P2-1-2-1-0": ("currents", "phase_y"),
    "P2-1-3-1-0": ("currents", "phase_b"),
    "P4-1-1-0-0": ("pf", "r"),
    "P4-2-1-0-0": ("pf", "y"),
    "P4-3-1-0-0": ("pf", "b"),
    "P4-4-1-0-0": ("pf", "avg"),
    "P9-1-0-0-0": ("frequency", None)
}

D3_ENERGY_MAPPING = {
    "P7-1-5-2-0": "import",
    "P7-2-1-0-0": "export",
    "P7-2-2-0-0": "reactive",
    "P7-3-5-1-0": "cumulative"
}

# === SCRIPT-BASED: measurements ===
def process_d2_to_measurements(d2_list):
    voltages, currents = {}, {}
    frequency = {"value": None, "unit": ""}
    power_factors = {"avg": None, "per_phase": {}}

    for entry in d2_list:
        code = entry.get("code", "")
        try:
            val = float(entry.get("value", 0))
        except:
            val = 0
        unit = entry.get("unit", "")

        if code in D2_MAPPING:
            category, sub = D2_MAPPING[code]
            if category == "voltages":
                voltages[sub] = {"value": val, "unit": unit}
            elif category == "currents":
                currents[sub] = {"value": val, "unit": unit}
            elif category == "pf":
                if sub == "avg":
                    power_factors["avg"] = val
                else:
                    power_factors["per_phase"][sub] = val
            elif category == "frequency":
                frequency = {"value": val, "unit": unit}

    # Compute avg PF if missing
    if not power_factors["avg"] and power_factors["per_phase"]:
        power_factors["avg"] = round(sum(power_factors["per_phase"].values()) / len(power_factors["per_phase"]), 2)

    return {
        "voltages": voltages,
        "currents": currents,
        "frequency": frequency,
        "power_factors": power_factors
    }

def process_d3_energy(d3_list):
    energy_kwh = {"import": 0, "export": 0, "reactive": 0, "cumulative": 0}
    for entry in d3_list:
        for r in entry.get("readings", []):
            try:
                val = float(r.get("VALUE", 0))
            except:
                val = 0
            param = r.get("PARAMCODE")
            if param in D3_ENERGY_MAPPING:
                energy_kwh[D3_ENERGY_MAPPING[param]] = val
    return energy_kwh

# === SCRIPT-BASED: events ===
def process_d3_to_events(d3_list):
    events = []
    for entry in d3_list:
        mech = entry.get("mechanism", "AUTO")
        for r in entry.get("readings", []):
            try:
                val = float(r.get("VALUE", 0))
            except:
                val = 0
            events.append({
                "event_type": r.get("type"),
                "mechanism": mech,
                "param_code": r.get("PARAMCODE"),
                "value": val,
                "unit": r.get("UNIT", "")
            })
    return events

# === WEATHER + LOCATION ===
def get_weather_and_location(location_code, timestamp):
    loc = location_map.get(location_code)
    if not loc:
        return None, None

    lat, lon = loc["lat"], loc["lon"]
    weather_map = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Fog", 48: "Depositing rime fog",
        51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
        61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
        71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
        80: "Rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
        95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Heavy thunderstorm with hail"
    }

    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", ""))
        date_str = dt.strftime("%Y-%m-%d")
        hour = dt.hour
        cache_key = f"{location_code}_{date_str}_{hour}"
        if cache_key in weather_cache:
            return weather_cache[cache_key]["weather"], weather_cache[cache_key]["location"]

        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}"
            f"&hourly=temperature_2m,relative_humidity_2m,precipitation,"
            f"shortwave_radiation,windspeed_10m,weathercode"
            f"&start_date={date_str}&end_date={date_str}"
        )
        resp = requests.get(url, timeout=10)
        data = resp.json()
        times = data["hourly"]["time"]
        idx = times.index(f"{date_str}T{hour:02d}:00")
        code = data["hourly"]["weathercode"][idx]
        condition = weather_map.get(code, f"Unknown ({code})")

        weather = {
            "temperature_c": data["hourly"]["temperature_2m"][idx],
            "humidity_pct": data["hourly"]["relative_humidity_2m"][idx],
            "rainfall_mm": data["hourly"]["precipitation"][idx],
            "solar_irradiance_wm2": data["hourly"]["shortwave_radiation"][idx],
            "wind_speed_ms": data["hourly"]["windspeed_10m"][idx],
            "condition": condition
        }

        location = {
            "substation": location_code,
            "latitude": lat,
            "longitude": lon,
            "city": loc["city"],
            "state": loc["state"],
            "country": loc["country"]
        }

        weather_cache[cache_key] = {"weather": weather, "location": location}
        return weather, location
    except Exception as e:
        print(f" Weather fetch failed for {location_code}: {e}")
        return None, None

# === MAIN CONVERSION ===
def convert_file(input_path, output_path):
    global converted_files, failed_files
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            raw_json = json.load(f)

        converted = {
            "utility_code": raw_json.get("utility_code"),
            "discom": raw_json.get("d1", {}).get("discom"),
            "meter_type": raw_json.get("d1", {}).get("meter_type"),
            "modem_serial_number": raw_json.get("d1", {}).get("modem_serial_number")
        }

        # --- asset_info via Ollama ---
        asset_info = convert_asset_info_with_ollama(raw_json.get("d1", {}))
        if not asset_info:
            d1 = raw_json.get("d1", {})
            asset_info = {
                "asset_id": d1.get("g1"),
                "location_code": d1.get("g17"),
                "voltage_class": d1.get("g15"),
                "standard": d1.get("g31"),
                "installation_date": iso_format(d1.get("g2", ""))
            }
        converted["asset_info"] = asset_info

        # --- measurements ---
        measurements = process_d2_to_measurements(raw_json.get("d2", []))
        measurements["energy_kwh"] = process_d3_energy(raw_json.get("d3", []))
        converted["measurements"] = measurements

        # --- events ---
        converted["events"] = process_d3_to_events(raw_json.get("d3", []))
        if raw_json.get("d3"):
            converted["timestamp"] = iso_format(raw_json["d3"][0].get("datetime", ""))

        # --- weather + location ---
        if asset_info.get("location_code") and converted.get("timestamp"):
            weather, location = get_weather_and_location(asset_info["location_code"], converted["timestamp"])
            if weather and location:
                converted["weather"] = weather
                converted["location"] = location

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(converted, f, indent=2)

        converted_files += 1
        print(f"✅ Converted: {input_path}")

    except Exception as e:
        failed_files += 1
        failed_list.append(input_path)
        print(f"❌ Failed: {input_path} → {e}")

# === WALK FOLDER ===
for root, dirs, files in os.walk(input_folder):
    for file in files:
        if file.endswith(".json"):
            total_files += 1
            input_path = os.path.join(root, file)
            relative_path = os.path.relpath(root, input_folder)
            output_dir = os.path.join(output_folder, relative_path)
            output_path = os.path.join(output_dir, file)
            convert_file(input_path, output_path)

# === SAVE WEATHER CACHE ===
with open(weather_cache_file, "w", encoding="utf-8") as f:
    json.dump(weather_cache, f, indent=2)

# === SUMMARY ===
print("\n=== Conversion Summary ===")
print(f"Total files: {total_files}")
print(f"Successfully converted: {converted_files}")
print(f"Failed: {failed_files}")
if failed_list:
    print("Failed files:")
    for f in failed_list:
        print(f" - {f}")

