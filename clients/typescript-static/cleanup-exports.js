#!/usr/bin/env node
/**
 * Removes duplicate API exports caused by operations with multiple tags.
 * See: https://github.com/OpenAPITools/openapi-generator/issues/7519
 */

const fs = require('fs');
const path = require('path');

const apisIndexPath = path.join(__dirname, '..', 'typescript', 'src', 'apis', 'index.ts');

if (!fs.existsSync(apisIndexPath)) {
  console.error('Error: APIs index.ts not found');
  process.exit(1);
}

let content = fs.readFileSync(apisIndexPath, 'utf8');

// Remove exports for duplicate API files
// These APIs contain operations that are already exported from their primary API files
const duplicateApis = [
  'ExternalApi',      // Operations also in AuthApi
  'ExperimentalApi',  // Operations also in AuthApi, ObjectsApi, PullsApi, LicenseApi
];

let modified = false;
duplicateApis.forEach(api => {
  const exportLine = `export * from './${api}';`;
  if (content.includes(exportLine)) {
    content = content.replace(exportLine + '\n', '');
    console.log(`Removed duplicate export: ${api}`);
    modified = true;
  }
});

if (modified) {
  fs.writeFileSync(apisIndexPath, content, 'utf8');
  console.log('✓ Cleaned up duplicate API exports');
} else {
  console.log('✓ No duplicate exports found');
}
