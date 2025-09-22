# ⚡ MeterMind-LLM...

> An **AI-powered data pipeline** that transforms raw **smart meter JSONs** into a unified, enriched schema using **LLMs (Ollama + DeepSeek R1)**.  
> Includes **weather enrichment, location mapping, and energy metrics extraction** — built for large-scale power utility data.

---

## 🚀 Overview

Smart meters and discoms generate massive volumes of JSON data, but these files often:
- Differ in structure (`d1`, `d2`, `d3` sections, events, codes).
- Lack standardized schemas for analytics.
- Miss contextual metadata like **weather** and **location**.

**MeterMind-LLM** solves this by:
- Using **LLMs** (DeepSeek-R1 via Ollama) to restructure raw JSON into a clean, standardized format.
- Enriching with **real-time weather data** (via [Open-Meteo API](https://open-meteo.com/)).
- Adding **geospatial context** (substation, lat/lon, city, state, country).
- Processing **tens of thousands of JSON files in batch mode**.

---

## ✨ Features===================================================

- 🔄 **Automated JSON Standardization** → Converts raw `d1/d2/d3` blocks into a clean schema.
- 🧠 **LLM-Powered Parsing** → Ollama + DeepSeek R1 for intelligent restructuring.
- 📊 **Measurement Extraction** → Voltages, currents, frequency, power factors, energy kWh.
- ⚡ **Event Logs** → Maps event codes (B3, B4, etc.) with values and units.
- 🌦️ **Weather Enrichment** → Temperature, humidity, rainfall, irradiance, windspeed, condition.
- 📍 **Location Metadata** → Substation code → lat/lon + city/state mapping.
- 📂 **Batch Processing** → Walks through folders/subfolders with >50k JSONs.
- 💾 **Weather Caching** → Avoids redundant API calls for speed.

---

## 🏗️ Architecture

Raw JSONs (d1/d2/d3)
│
▼
[Ollama LLM (DeepSeek-R1)]
│
▼
Standardized JSON Schema
│
├── Asset Info
├── Measurements
├── Events
├── Weather (via API)
└── Location (geo-mapping)
