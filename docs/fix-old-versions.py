#!/usr/bin/env python3
"""
Script to find and update JavaScript code in HTML files.
Replaces Object.entries() pattern with array iteration pattern.
"""

import os
import re
import argparse
from pathlib import Path

def find_and_replace_js_code(content):
    """
    Find and replace the JavaScript code pattern in HTML content.
    
    Args:
        content (str): HTML file content
        
    Returns:
        tuple: (updated_content, was_modified)
    """
    # Pattern to match the old JavaScript code
    old_pattern = r'''window\.addEventListener\("load",\s*async\s*\(\)\s*=>\s*\{[^}]*?const\s+versions\s*=\s*await\s+response\.json\(\);[^}]*?for\s*\(\s*let\s*\[key,\s*value\]\s*of\s*Object\.entries\(versions\)\)[^}]*?\}[^}]*?\}\);'''
    
    # More flexible pattern to capture the essential parts
    pattern = r'''(window\.addEventListener\("load",\s*async\s*\(\)\s*=>\s*\{.*?)(for\s*\(\s*let\s*\[key,\s*value\]\s*of\s*Object\.entries\(versions\)\)\s*\{.*?el\.value\s*=\s*key;.*?el\.textContent\s*=\s*value;.*?if\s*\(\s*key\s*===\s*selectedVersion\)\s*\{.*?el\.selected\s*=\s*true;.*?\}.*?selectVersionElmFirst\.after\(el\);.*?\})(.*?\}\);)'''
    
    def replacement(match):
        before = match.group(1)
        after = match.group(3)
        
        # New JavaScript code for array iteration
        new_for_loop = '''for (let versionObj of versions) {
        const el = document.createElement("option");
        el.value = versionObj.version;
        el.textContent = versionObj.title;
        if (versionObj.version === selectedVersion) {
            el.selected = true;
        }
        selectVersionElmFirst.after(el);
    }'''
        
        return before + new_for_loop + after
    
    # Try the more flexible pattern first
    modified_content = re.sub(pattern, replacement, content, flags=re.DOTALL | re.MULTILINE)
    
    if modified_content != content:
        return modified_content, True
    
    # Fallback: Look for the specific problematic lines and replace them
    # This is a more targeted approach
    lines_to_replace = [
        (r'for\s*\(\s*let\s*\[key,\s*value\]\s*of\s*Object\.entries\(versions\)\)', 'for (let versionObj of versions)'),
        (r'el\.value\s*=\s*key;', 'el.value = versionObj.version;'),
        (r'el\.textContent\s*=\s*value;', 'el.textContent = versionObj.title;'),
        (r'if\s*\(\s*key\s*===\s*selectedVersion\)', 'if (versionObj.version === selectedVersion)')
    ]
    
    modified = False
    for old_pattern, new_text in lines_to_replace:
        new_content = re.sub(old_pattern, new_text, modified_content)
        if new_content != modified_content:
            modified = True
            modified_content = new_content
    
    return modified_content, modified

def process_html_file(file_path, dry_run=False):
    """
    Process a single HTML file.
    
    Args:
        file_path (Path): Path to the HTML file
        dry_run (bool): If True, don't write changes to file
        
    Returns:
        bool: True if file was modified
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        updated_content, was_modified = find_and_replace_js_code(content)
        
        if was_modified:
            print(f"✓ Found and updated JavaScript code in: {file_path}")
            
            if not dry_run:
                # Write updated content
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(updated_content)
                print(f"  Updated: {file_path}")
            else:
                print(f"  [DRY RUN] Would update: {file_path}")
            
            return True
        else:
            print(f"- No matching JavaScript code found in: {file_path}")
            return False
            
    except Exception as e:
        print(f"✗ Error processing {file_path}: {e}")
        return False

def scan_folder(folder_path, dry_run=False):
    """
    Scan folder for index.html files and update them.
    
    Args:
        folder_path (str): Path to folder to scan
        dry_run (bool): If True, don't write changes to files
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        print(f"Error: Folder '{folder_path}' does not exist")
        return
    
    if not folder.is_dir():
        print(f"Error: '{folder_path}' is not a directory")
        return
    
    print(f"Scanning folder: {folder.absolute()}")
    print(f"Mode: {'DRY RUN' if dry_run else 'UPDATE FILES'}")
    print("-" * 50)
    
    # Find all index.html files recursively
    html_files = list(folder.rglob('index.html'))
    
    if not html_files:
        print("No index.html files found in the specified folder")
        return
    
    print(f"Found {len(html_files)} index.html files")
    print("-" * 50)
    
    modified_count = 0
    for html_file in html_files:
        if process_html_file(html_file, dry_run):
            modified_count += 1
    
    print("-" * 50)
    print(f"Summary: {modified_count} files {'would be' if dry_run else 'were'} modified out of {len(html_files)} files")

def main():
    parser = argparse.ArgumentParser(
        description="Update JavaScript code in HTML files from Object.entries() to array iteration"
    )
    parser.add_argument(
        "folder",
        help="Path to folder containing HTML files to scan"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying files"
    )
    
    args = parser.parse_args()
    
    scan_folder(args.folder, args.dry_run)

if __name__ == "__main__":
    main()
