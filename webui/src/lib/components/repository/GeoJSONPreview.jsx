import React from "react";
import { MapContainer, TileLayer, GeoJSON } from "react-leaflet";
import L from 'leaflet';
import "leaflet/dist/leaflet.css";

const parseGeoJSON = data => {
    try {
        return JSON.parse(data);
    } catch {
        return null;
    }
};

export const GeoJSONPreview = ({ data }) => {
    if (!data) return <div>No data to preview</div>;

    const geoJsonData = parseGeoJSON(data);
    if (!geoJsonData) return <div>Invalid GeoJSON data</div>;

    const bounds = L.geoJSON(geoJsonData).getBounds();

    const mapProps = bounds.isValid()
        ? { bounds }
        : { center: [0, 0], zoom: 2 };

    return (
        <div className="geojson-map-wrapper">
            <MapContainer {...mapProps} className="geojson-map-container">
                <TileLayer
                    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    attribution='&copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap</a>'
                />
                <GeoJSON data={geoJsonData} />
            </MapContainer>
        </div>
    );
};
