import React from 'react';
import { createRoot } from 'react-dom/client';
// styles
import 'bootstrap/dist/css/bootstrap.css';
import './styles/globals.css';

// Areas
import './styles/navigation/navigation.css';
import './styles/repositories/repositories.css';
import './styles/objects/objects.css';
import './styles/objects/object-viewer.css';
import './styles/objects/upload.css';
import './styles/objects/tree.css';
import './styles/objects/diff.css';
import './styles/auth.css';
// Components
import './styles/components/buttons.css';
import './styles/components/cards.css';
import './styles/components/tables.css';
import './styles/components/forms.css';
import './styles/components/ui-components.css';
import './styles/components/bootstrap-compat.css';
import './styles/quickstart.css';
import './styles/ghsyntax.css';

import { IndexPage } from './pages';

const container = document.getElementById('root');
if (!container) throw new Error('Failed to find root element!');

const root = createRoot(container);
root.render(<IndexPage />);
