import React from "react";
import { MapContainer, TileLayer, GeoJSON } from "react-leaflet";
import L from 'leaflet';
import "leaflet/dist/leaflet.css";
import "leaflet-defaulticon-compatibility/dist/leaflet-defaulticon-compatibility.css";
import "leaflet-defaulticon-compatibility";

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
                    // url="https://{s}.tile.openstreetmap.de/{z}/{x}/{y}.png" tells Leaflet to load map tiles from the OpenStreetMap Germany server.
                    // The {s} in the URL is a placeholder for a “subdomain”.
                    // By listing ['a','b','c'], Leaflet will pick one of those letters each time it requests a tile.
                    // That way, the requests are spread across a.tile.openstreetmap.de, b.tile… and c.tile…, which helps balance the load and speed up loading.
                    // You’re telling the map where to get its background tiles (using three mirror subdomains for performance) and providing
                    // fallback servers if one goes down.
                    url="https://{s}.tile.openstreetmap.de/{z}/{x}/{y}.png"
                    subdomains={['a', 'b', 'c']}

                    // .org credits the OpenStreetMap project and contributors (the source of the map data)
                    // .de credits the German tile hosting server (the infrastructure serving the tiles).
                    attribution={`
                        &copy; <a href="https://openstreetmap.org/">OpenStreetMap</a>
                        &copy; <a href="https://openstreetmap.de/">OSM Germany</a>
                    `}
                    /*
                      This part is an error handler for when a map tile fails to load.
                      If the request to tile.openstreetmap.de fails (the default tile server), the onTileError handler triggers.
                      Inside the handler, we check the URL of the failed image, and replace the domain with
                      an alternative one (tile.openstreetmap.fr/osmfr) to try loading it again.
                      If that also fails, it retries once more - this time falling back to the main OpenStreetMap tile server (tile.openstreetmap.org).
                      So it's basically a retry chain that tries up to three different tile sources,
                      all using the same tile path pattern - just switching the base domain.
                    */
                    onTileError={(e) => {
                        const img = e.tile;
                        const src = img.src;

                        // e.tile is a reference to the actual <img> DOM element that failed to load.
                        // By updating img.src, we’re directly modifying that element’s src attribute in the DOM - so the browser will
                        // automatically retry loading the image from the new URL.
                        // That’s how the fallback mechanism works here: when a tile fails, we catch it and point the browser
                        // to a backup tile server by changing the image’s src.
                        if (src.includes("tile.openstreetmap.de")) {
                            img.src = src.replace(
                                "tile.openstreetmap.de",
                                "tile.openstreetmap.fr/osmfr"
                            );
                        } else if (src.includes("tile.openstreetmap.fr/osmfr")) {
                            img.src = src.replace(
                                "tile.openstreetmap.fr/osmfr",
                                "tile.openstreetmap.org"
                            );
                        }
                    }}
                />
                <GeoJSON data={geoJsonData} />
            </MapContainer>
        </div>
    );
};
