#!/usr/bin/env python3

import os
import sys
import re
import glob

def replace_code_in_file(file_path, search_pattern, replacement_code):
    """
    Replace the matching pattern and next 8 lines with replacement code.
    Returns True if file was modified, False otherwise.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        modified = False
        new_lines = []
        i = 0
        
        while i < len(lines):
            if re.search(search_pattern, lines[i]):
                # Found the pattern, add replacement code
                new_lines.append(replacement_code + '\n')
                # Skip the next 8 lines (total 9 lines including current)
                i += 9
                modified = True
            else:
                new_lines.append(lines[i])
                i += 1
        
        if modified:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
        
        return modified
    
    except Exception as e:
        print(f"  Error processing {file_path}: {e}")
        return False

def main():
    # Get directory from command line argument or use current directory
    scan_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
    
    # Check if directory exists
    if not os.path.isdir(scan_dir):
        print(f"Error: Directory '{scan_dir}' does not exist.")
        sys.exit(1)
    
    # The regex pattern to search for
    search_pattern = r'for \(let \[key, value\] of Object\.entries\(versions\)\)'
    
    # The replacement code block
    replacement_code = '''                    for (let versionObj of versions) {
                        const el = document.createElement("option");
                        el.value = versionObj.version;
                        el.textContent = versionObj.title;
                        if (versionObj.version === selectedVersion) {
                            el.selected = true;
                        }
                        selectVersionElmFirst.after(el);
                    }'''
    
    # Counters
    files_processed = 0
    files_modified = 0
    
    print(f"Scanning HTML files in directory: {scan_dir}")
    print("-" * 40)
    
    # Find all HTML files recursively
    pattern = os.path.join(scan_dir, '**', '*.html')
    html_files = glob.glob(pattern, recursive=True)
    
    for file_path in html_files:
        print(f"Processing: {file_path}")
        
        # Check if file contains the search pattern
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if re.search(search_pattern, content):
                print(f"  Found matching pattern in: {file_path}")
                
                if replace_code_in_file(file_path, search_pattern, replacement_code):
                    print(f"  Successfully modified: {file_path}")
                    files_modified += 1
                else:
                    print(f"  Failed to modify: {file_path}")
            else:
                print(f"  No matching pattern found in: {file_path}")
            
            files_processed += 1
            
        except Exception as e:
            print(f"  Error reading {file_path}: {e}")
    
    print("-" * 40)
    print("Processing complete!")
    print(f"Files processed: {files_processed}")
    print(f"Files modified: {files_modified}")

if __name__ == "__main__":
    main()
