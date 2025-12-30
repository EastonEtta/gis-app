from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import geopandas as gpd
from shapely.geometry import Point, shape
import json
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import httpx
from datetime import datetime, timedelta
from typing import Dict, List
import asyncio

app = FastAPI(title="GIS Application API")

# CORS middleware for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/gisdb")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

# Pydantic models
class PointFeature(BaseModel):
    name: str
    latitude: float
    longitude: float
    properties: Optional[dict] = {}

class Feature(BaseModel):
    type: str = "Feature"
    geometry: dict
    properties: dict

@app.on_event("startup")
async def startup_event():
    """Initialize database tables"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create points table WITHOUT PostGIS (simpler for deployment)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS points (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            properties JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS points_lat_idx ON points(latitude);
        CREATE INDEX IF NOT EXISTS points_lon_idx ON points(longitude);
    """)
    
    conn.commit()
    cur.close()
    conn.close()

@app.get("/")
async def root():
    return {
        "message": "GIS Application API",
        "version": "1.0",
        "endpoints": {
            "points": "/api/points",
            "features": "/api/features",
            "upload": "/api/upload"
        }
    }

@app.post("/api/points")
async def create_point(point: PointFeature):
    """Create a new point feature"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO points (name, latitude, longitude, properties)
        VALUES (%s, %s, %s, %s)
        RETURNING id, name, latitude, longitude, properties
    """, (point.name, point.latitude, point.longitude, json.dumps(point.properties)))
    
    result = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    
    return {
        "type": "Feature",
        "id": result["id"],
        "geometry": {
            "type": "Point",
            "coordinates": [result["longitude"], result["latitude"]]
        },
        "properties": {
            "name": result["name"],
            **result["properties"]
        }
    }

@app.get("/api/points")
async def get_points():
    """Get all points as GeoJSON"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            id,
            name,
            latitude,
            longitude,
            properties
        FROM points
        ORDER BY created_at DESC
    """)
    
    features = []
    for row in cur.fetchall():
        features.append({
            "type": "Feature",
            "id": row["id"],
            "geometry": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]]
            },
            "properties": {
                "name": row["name"],
                **row["properties"]
            }
        })
    
    cur.close()
    conn.close()
    
    return {
        "type": "FeatureCollection",
        "features": features
    }

@app.get("/api/points/bbox")
async def get_points_in_bbox(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float
):
    """Get points within bounding box"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            id,
            name,
            latitude,
            longitude,
            properties
        FROM points
        WHERE longitude >= %s AND longitude <= %s
          AND latitude >= %s AND latitude <= %s
    """, (minx, maxx, miny, maxy))
    
    features = []
    for row in cur.fetchall():
        features.append({
            "type": "Feature",
            "id": row["id"],
            "geometry": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]]
            },
            "properties": {
                "name": row["name"],
                **row["properties"]
            }
        })
    
    cur.close()
    conn.close()
    
    return {
        "type": "FeatureCollection",
        "features": features
    }

@app.delete("/api/points/{point_id}")
async def delete_point(point_id: int):
    """Delete a point"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("DELETE FROM points WHERE id = %s RETURNING id", (point_id,))
    result = cur.fetchone()
    
    if not result:
        raise HTTPException(status_code=404, detail="Point not found")
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {"message": "Point deleted", "id": point_id}

@app.post("/api/upload/geojson")
async def upload_geojson(file: UploadFile = File(...)):
    """Upload and import GeoJSON file"""
    contents = await file.read()
    geojson_data = json.loads(contents)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    imported = 0
    for feature in geojson_data.get("features", []):
        geom = feature.get("geometry")
        props = feature.get("properties", {})
        name = props.get("name", "Unnamed")
        
        if geom.get("type") == "Point":
            coords = geom.get("coordinates")
            lon, lat = coords[0], coords[1]
            cur.execute("""
                INSERT INTO points (name, latitude, longitude, properties)
                VALUES (%s, %s, %s, %s)
            """, (name, lat, lon, json.dumps(props)))
            imported += 1
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {
        "message": f"Imported {imported} features",
        "count": imported
    }
@app.get("/api/wildfire/risk")
async def get_wildfire_risk():
    """
    Get wildfire risk data for Texas
    Combines multiple data sources:
    - Weather conditions (temperature, humidity, wind)
    - Vegetation dryness
    - Active fire data from NASA FIRMS
    """
    try:
        # Get active fires from NASA FIRMS
        fires = await get_active_fires_texas()
        
        # Get weather conditions that affect fire risk
        weather_risk = await get_weather_risk_texas()
        
        # Combine into risk zones
        risk_zones = calculate_risk_zones(fires, weather_risk)
        
        return {
            "type": "FeatureCollection",
            "features": risk_zones,
            "metadata": {
                "timestamp": datetime.utcnow().isoformat(),
                "source": "NASA FIRMS + Weather Data",
                "update_frequency": "6 hours"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def get_active_fires_texas():
    """
    Fetch active fire data from NASA FIRMS (Fire Information for Resource Management System)
    Free API - no key required for MODIS data
    """
    # Texas bounding box
    texas_bbox = {
        "min_lat": 25.8,
        "max_lat": 36.5,
        "min_lon": -106.6,
        "max_lon": -93.5
    }
    
    # Get fires from last 7 days
    url = "https://firms.modaps.eosdis.nasa.gov/api/country/csv/7d2b18dc5b5c8d18e2e2f9e5f9d72c08/MODIS_NRT/USA/1"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            if response.status_code == 200:
                # Parse CSV data
                lines = response.text.strip().split('\n')
                fires = []
                
                if len(lines) > 1:
                    headers = lines[0].split(',')
                    for line in lines[1:]:
                        values = line.split(',')
                        if len(values) >= 4:
                            try:
                                lat = float(values[0])
                                lon = float(values[1])
                                
                                # Filter for Texas
                                if (texas_bbox["min_lat"] <= lat <= texas_bbox["max_lat"] and
                                    texas_bbox["min_lon"] <= lon <= texas_bbox["max_lon"]):
                                    
                                    fires.append({
                                        "type": "Feature",
                                        "geometry": {
                                            "type": "Point",
                                            "coordinates": [lon, lat]
                                        },
                                        "properties": {
                                            "type": "active_fire",
                                            "confidence": values[8] if len(values) > 8 else "unknown",
                                            "brightness": values[2] if len(values) > 2 else "unknown",
                                            "acq_date": values[5] if len(values) > 5 else "unknown"
                                        }
                                    })
                            except (ValueError, IndexError):
                                continue
                
                return fires
        except Exception as e:
            print(f"Error fetching fire data: {e}")
            return []
    
    return []

async def get_weather_risk_texas():
    """
    Get weather-based fire risk for Texas regions using a grid pattern
    Using Open-Meteo API (free, no key required)
    """
    # Create a grid of sample points across Texas
    # Spacing of 1.5 degrees = ~100 miles between points = ~50 points total
    grid_points = []
    
    # Texas bounding box
    min_lat, max_lat = 26.0, 36.5
    min_lon, max_lon = -106.5, -93.5
    
    # Create grid
    lat = min_lat
    while lat <= max_lat:
        lon = min_lon
        while lon <= max_lon:
            grid_points.append({
                "name": f"Grid_{len(grid_points)}",
                "lat": round(lat, 2),
                "lon": round(lon, 2)
            })
            lon += 1.5  # ~100 miles spacing
        lat += 1.5
    
    print(f"Sampling {len(grid_points)} points across Texas...")
    
    weather_risks = []
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Process in batches to avoid overwhelming the API
        for point in grid_points:
            try:
                # Get weather data from Open-Meteo
                url = f"https://api.open-meteo.com/v1/forecast?latitude={point['lat']}&longitude={point['lon']}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&temperature_unit=fahrenheit&wind_speed_unit=mph"
                
                response = await client.get(url)
                if response.status_code == 200:
                    data = response.json()
                    current = data.get("current", {})
                    
                    # Get weather values
                    temp = current.get("temperature_2m", 70)
                    humidity = current.get("relative_humidity_2m", 50)
                    wind_speed = current.get("wind_speed_10m", 5)
                    precipitation = current.get("precipitation", 0)
                    
                    # FIRE RISK CALCULATION FORMULA:
                    # Based on Fire Weather Index principles
                    
                    risk_score = 0
                    
                    # 1. TEMPERATURE FACTOR (max 40 points)
                    # Higher temps dry out vegetation and increase fire intensity
                    if temp > 75:
                        risk_score += 10
                    if temp > 85:
                        risk_score += 15  # Total: 25
                    if temp > 95:
                        risk_score += 15  # Total: 40
                    
                    # 2. HUMIDITY FACTOR (max 35 points)
                    # Low humidity = dry conditions = fires spread faster
                    if humidity < 40:
                        risk_score += 10
                    if humidity < 25:
                        risk_score += 15  # Total: 25
                    if humidity < 15:
                        risk_score += 10  # Total: 35
                    
                    # 3. WIND FACTOR (max 30 points)
                    # Wind spreads fires and makes them unpredictable
                    if wind_speed > 10:
                        risk_score += 10
                    if wind_speed > 20:
                        risk_score += 10  # Total: 20
                    if wind_speed > 30:
                        risk_score += 10  # Total: 30
                    
                    # 4. PRECIPITATION PENALTY (reduces risk)
                    # Recent rain = wet vegetation = lower fire risk
                    if precipitation > 0:
                        risk_score -= 20  # Recent rain reduces risk
                    
                    # Keep score in 0-100 range
                    risk_score = max(0, min(100, risk_score))
                    
                    # Determine risk level and color
                    # RISK LEVELS:
                    # 0-25: LOW (green)
                    # 26-50: MODERATE (yellow/orange)  
                    # 51-75: HIGH (orange/red)
                    # 76-100: EXTREME (dark red)
                    
                    if risk_score >= 76:
                        risk_level = "extreme"
                        color = "#8B0000"  # Dark red
                        radius_km = 60
                    elif risk_score >= 51:
                        risk_level = "high"
                        color = "#FF4500"  # Orange-red
                        radius_km = 50
                    elif risk_score >= 26:
                        risk_level = "moderate"
                        color = "#FFA500"  # Orange
                        radius_km = 40
                    else:
                        risk_level = "low"
                        color = "#90EE90"  # Light green
                        radius_km = 30
                    
                    weather_risks.append({
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [point["lon"], point["lat"]]
                        },
                        "properties": {
                            "type": "weather_risk",
                            "location": f"Grid Point ({point['lat']}, {point['lon']})",
                            "risk_level": risk_level,
                            "risk_score": risk_score,
                            "color": color,
                            "temperature": round(temp, 1),
                            "humidity": round(humidity, 1),
                            "wind_speed": round(wind_speed, 1),
                            "precipitation": round(precipitation, 2),
                            "radius_km": radius_km
                        }
                    })
                    
            except Exception as e:
                print(f"Error fetching weather for point {point}: {e}")
                continue
            
            # Small delay to be nice to the API
            await asyncio.sleep(0.1)
    
    print(f"Successfully retrieved {len(weather_risks)} risk zones")
    return weather_risks

def calculate_risk_zones(fires: List[Dict], weather_risk: List[Dict]) -> List[Dict]:
    """
    Combine fire and weather data to create risk zones
    """
    all_features = []
    
    # Add all active fires
    all_features.extend(fires)
    
    # Add weather-based risk zones
    for risk in weather_risk:
        props = risk["properties"]
        coords = risk["geometry"]["coordinates"]
        
        # Create a circular risk zone around each city (simplified)
        # In production, you'd use actual geographic polygons
        radius_km = 50 if props["risk_level"] == "extreme" else 30
        
        all_features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": coords
            },
            "properties": {
                **props,
                "radius_km": radius_km
            }
        })
    
    return all_features

@app.get("/api/wildfire/alerts")
async def get_wildfire_alerts():
    """
    Get current wildfire alerts and warnings for Texas
    """
    # This would integrate with NWS (National Weather Service) API
    # For now, return based on risk calculation
    risk_data = await get_wildfire_risk()
    
    alerts = []
    for feature in risk_data["features"]:
        props = feature["properties"]
        if props.get("type") == "weather_risk":
            if props["risk_level"] in ["high", "extreme"]:
                alerts.append({
                    "location": props["location"],
                    "level": props["risk_level"],
                    "message": f"{props['risk_level'].upper()} fire risk in {props['location']} area. "
                              f"Temp: {props['temperature']}°F, Humidity: {props['humidity']}%, "
                              f"Wind: {props['wind_speed']} mph"
                })
    
    return {
        "alerts": alerts,
        "count": len(alerts),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/wildfire/stats")
async def get_wildfire_stats():
    """
    Get statistics about current wildfire situation in Texas
    """
    risk_data = await get_wildfire_risk()
    
    active_fires = sum(1 for f in risk_data["features"] if f["properties"].get("type") == "active_fire")
    
    risk_counts = {"low": 0, "moderate": 0, "high": 0, "extreme": 0}
    for feature in risk_data["features"]:
        if feature["properties"].get("type") == "weather_risk":
            level = feature["properties"].get("risk_level", "low")
            risk_counts[level] += 1
    
    return {
        "active_fires": active_fires,
        "risk_zones": risk_counts,
        "last_updated": datetime.utcnow().isoformat()
    }
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


