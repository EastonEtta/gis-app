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
# Add these imports at the top of main.py
import httpx
from datetime import datetime, timedelta
from typing import Dict, List
import asyncio

# Add these endpoints to your FastAPI app

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
    Get weather-based fire risk for Texas counties
    Using county centroids and Open-Meteo API (free, no key required)
    Returns county names with FIPS codes for proper matching
    """
    # Texas counties with their centroids (lat/lon) and FIPS codes
    # FIPS format: 48XXX where 48 = Texas
    texas_counties = [
        {"name": "Harris", "fips": "48201", "lat": 29.8580, "lon": -95.3885},
        {"name": "Dallas", "fips": "48113", "lat": 32.7668, "lon": -96.7780},
        {"name": "Tarrant", "fips": "48439", "lat": 32.7555, "lon": -97.3308},
        {"name": "Bexar", "fips": "48029", "lat": 29.4498, "lon": -98.5254},
        {"name": "Travis", "fips": "48453", "lat": 30.3323, "lon": -97.7663},
        {"name": "Collin", "fips": "48085", "lat": 33.1886, "lon": -96.5733},
        {"name": "El Paso", "fips": "48141", "lat": 31.8113, "lon": -106.3505},
        {"name": "Denton", "fips": "48121", "lat": 33.2115, "lon": -97.1331},
        {"name": "Fort Bend", "fips": "48157", "lat": 29.5696, "lon": -95.7603},
        {"name": "Montgomery", "fips": "48339", "lat": 30.3158, "lon": -95.5016},
        {"name": "Williamson", "fips": "48491", "lat": 30.6580, "lon": -97.6726},
        {"name": "Hidalgo", "fips": "48215", "lat": 26.3424, "lon": -98.1615},
        {"name": "Nueces", "fips": "48355", "lat": 27.7305, "lon": -97.5934},
        {"name": "Cameron", "fips": "48061", "lat": 26.1315, "lon": -97.4450},
        {"name": "Brazoria", "fips": "48039", "lat": 29.1652, "lon": -95.4349},
        {"name": "Webb", "fips": "48479", "lat": 27.7319, "lon": -99.4965},
        {"name": "McLennan", "fips": "48309", "lat": 31.5493, "lon": -97.1467},
        {"name": "Bell", "fips": "48027", "lat": 31.0693, "lon": -97.4789},
        {"name": "Galveston", "fips": "48167", "lat": 29.4404, "lon": -94.8851},
        {"name": "Lubbock", "fips": "48303", "lat": 33.6151, "lon": -101.8552},
        {"name": "Jefferson", "fips": "48245", "lat": 29.9483, "lon": -94.0307},
        {"name": "Smith", "fips": "48423", "lat": 32.3985, "lon": -95.2609},
        {"name": "Brazos", "fips": "48041", "lat": 30.6630, "lon": -96.2983},
        {"name": "Hays", "fips": "48209", "lat": 30.0585, "lon": -98.0336},
        {"name": "Johnson", "fips": "48251", "lat": 32.3974, "lon": -97.3697},
        {"name": "Ector", "fips": "48135", "lat": 31.8876, "lon": -102.4448},
        {"name": "Midland", "fips": "48329", "lat": 31.9210, "lon": -102.0132},
        {"name": "Taylor", "fips": "48441", "lat": 32.3285, "lon": -99.8645},
        {"name": "Potter", "fips": "48375", "lat": 35.3962, "lon": -101.8767},
        {"name": "Guadalupe", "fips": "48187", "lat": 29.6299, "lon": -97.9586},
        {"name": "Wichita", "fips": "48485", "lat": 33.9693, "lon": -98.6978},
        {"name": "Tom Green", "fips": "48451", "lat": 31.4306, "lon": -100.4608},
        {"name": "Randall", "fips": "48381", "lat": 34.9820, "lon": -101.9171},
        {"name": "Gregg", "fips": "48183", "lat": 32.4899, "lon": -94.8266},
        {"name": "Comal", "fips": "48091", "lat": 29.8102, "lon": -98.2964},
        {"name": "Kaufman", "fips": "48257", "lat": 32.5882, "lon": -96.2892},
        {"name": "Ellis", "fips": "48139", "lat": 32.3507, "lon": -96.7892},
        {"name": "Rockwall", "fips": "48397", "lat": 32.8945, "lon": -96.4097},
        {"name": "Cherokee", "fips": "48073", "lat": 31.8518, "lon": -95.1541},
        {"name": "Angelina", "fips": "48005", "lat": 31.2304, "lon": -94.6191}
    ]
    
    print(f"Sampling {len(texas_counties)} counties across Texas...")
    
    weather_risks = []
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for county in texas_counties:
            try:
                url = f"https://api.open-meteo.com/v1/forecast?latitude={county['lat']}&longitude={county['lon']}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&temperature_unit=fahrenheit&wind_speed_unit=mph"
                
                response = await client.get(url)
                if response.status_code == 200:
                    data = response.json()
                    current = data.get("current", {})
                    
                    temp = current.get("temperature_2m", 70)
                    humidity = current.get("relative_humidity_2m", 50)
                    wind_speed = current.get("wind_speed_10m", 5)
                    precipitation = current.get("precipitation", 0)
                    
                    # FIRE RISK CALCULATION
                    risk_score = 0
                    
                    # Temperature factor (max 40)
                    if temp > 75: risk_score += 10
                    if temp > 85: risk_score += 15
                    if temp > 95: risk_score += 15
                    
                    # Humidity factor (max 35)
                    if humidity < 40: risk_score += 10
                    if humidity < 25: risk_score += 15
                    if humidity < 15: risk_score += 10
                    
                    # Wind factor (max 30)
                    if wind_speed > 10: risk_score += 10
                    if wind_speed > 20: risk_score += 10
                    if wind_speed > 30: risk_score += 10
                    
                    # Precipitation penalty
                    if precipitation > 0: risk_score -= 20
                    
                    risk_score = max(0, min(100, risk_score))
                    
                    # Determine risk level
                    if risk_score >= 76:
                        risk_level = "extreme"
                        color = "#8B0000"
                    elif risk_score >= 51:
                        risk_level = "high"
                        color = "#FF4500"
                    elif risk_score >= 26:
                        risk_level = "moderate"
                        color = "#FFA500"
                    else:
                        risk_level = "low"
                        color = "#90EE90"
                    
                    weather_risks.append({
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [county["lon"], county["lat"]]
                        },
                        "properties": {
                            "type": "weather_risk",
                            "county": county["name"],
                            "fips": county["fips"],
                            "risk_level": risk_level,
                            "risk_score": risk_score,
                            "color": color,
                            "temperature": round(temp, 1),
                            "humidity": round(humidity, 1),
                            "wind_speed": round(wind_speed, 1),
                            "precipitation": round(precipitation, 2)
                        }
                    })
                    
            except Exception as e:
                print(f"Error fetching weather for {county['name']}: {e}")
                continue
            
            await asyncio.sleep(0.05)
    
    print(f"Successfully retrieved {len(weather_risks)} county risk zones")
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

@app.get("/api/wildfire/counties")
async def get_texas_counties_geojson():
    """
    Fetch and return Texas county boundaries as GeoJSON
    Acts as a proxy to avoid CORS issues
    """
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            response = await client.get('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json')
            if response.status_code == 200:
                all_counties = response.json()
                
                # Filter for Texas counties (FIPS codes 48xxx)
                texas_counties = {
                    "type": "FeatureCollection",
                    "features": [f for f in all_counties["features"] if f.get("id", "").startswith("48")]
                }
                
                return texas_counties
            else:
                raise HTTPException(status_code=500, detail="Failed to fetch county data")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching counties: {str(e)}")

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




