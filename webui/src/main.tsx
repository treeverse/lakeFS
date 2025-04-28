import React from 'react';
import { createRoot } from 'react-dom/client';
// styles
import 'bootstrap/dist/css/bootstrap.css';
import './styles/globals.css';
import "leaflet/dist/leaflet.css";

// app and plugins system
import LakeFSApp from "./extendable/lakefsApp";
import { PluginManager } from "./extendable/plugins/pluginManager";

const pluginManager = new PluginManager();

const container = document.getElementById('root');
if (!container) throw new Error("Failed to find root element!");

const root = createRoot(container);
root.render(<LakeFSApp pluginManager={pluginManager} />);