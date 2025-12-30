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
    
    # Create points table with PostGIS
    cur.execute("""
        CREATE EXTENSION IF NOT EXISTS postgis;
        
        CREATE TABLE IF NOT EXISTS points (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            geom GEOMETRY(Point, 4326),
            properties JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS points_geom_idx ON points USING GIST(geom);
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
        INSERT INTO points (name, geom, properties)
        VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
        RETURNING id, name, ST_AsGeoJSON(geom)::json as geometry, properties
    """, (point.name, point.longitude, point.latitude, json.dumps(point.properties)))
    
    result = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    
    return {
        "type": "Feature",
        "id": result["id"],
        "geometry": result["geometry"],
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
            ST_AsGeoJSON(geom)::json as geometry,
            properties
        FROM points
        ORDER BY created_at DESC
    """)
    
    features = []
    for row in cur.fetchall():
        features.append({
            "type": "Feature",
            "id": row["id"],
            "geometry": row["geometry"],
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
            ST_AsGeoJSON(geom)::json as geometry,
            properties
        FROM points
        WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
    """, (minx, miny, maxx, maxy))
    
    features = []
    for row in cur.fetchall():
        features.append({
            "type": "Feature",
            "id": row["id"],
            "geometry": row["geometry"],
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
            cur.execute("""
                INSERT INTO points (name, geom, properties)
                VALUES (%s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s)
            """, (name, json.dumps(geom), json.dumps(props)))
            imported += 1
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {
        "message": f"Imported {imported} features",
        "count": imported
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)