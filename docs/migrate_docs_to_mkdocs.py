import os
import re
import yaml
from pathlib import Path
from collections import defaultdict

DOCS_DIR = Path(".").resolve()
SRC_DIR = DOCS_DIR / "src"
DEST_DIR = DOCS_DIR  # we transform files in-place (idempotent)

# Patterns to capture liquid tags
LINK_RE = re.compile(r"{%\s*link\s+([^\s%]+)\s*%}")
INCLUDE_REL_RE = re.compile(r"{%\s*include_relative\s+([^\s%]+)\s*%}")
INCLUDE_RE = re.compile(r"{%\s*include\s+([^\s%]+)\s*%}")
BASEURL_RE = re.compile(r"{{\s*site\.baseurl\s*}}")
TOC_RE = re.compile(r"{%\s*include\s+toc\.html\s*%}")


def strip_liquid(content: str, md_path: Path) -> str:
    """Replace common Jekyll liquid tags with MkDocs-friendly equivalents."""

    # 1. site.baseurl -> empty string (root)
    content = BASEURL_RE.sub("", content)

    # 2. toc include -> markdown [TOC]
    content = TOC_RE.sub("[TOC]", content)

    # 3. {% link path/to/file.md %} -> /path/to/file/
    def repl_link(match):
        target = match.group(1).strip()
        # ensure leading slash for absolute link
        target = target.lstrip('/')
        if target.endswith('.md'):
            target = target[:-3]
        return f"/{target}/"

    content = LINK_RE.sub(repl_link, content)

    # 4. {% include_relative some/file.md %} -> embed file contents
    def repl_include_rel(match):
        rel_path = match.group(1).strip()
        inc_path = md_path.parent / rel_path
        if inc_path.exists():
            return inc_path.read_text(encoding='utf-8')
        return ""

    content = INCLUDE_REL_RE.sub(repl_include_rel, content)

    # 5. {% include xyz %} -> remove (not supported) unless image or html snippet
    content = INCLUDE_RE.sub("", content)

    return content



def transform_markdown(md_path: Path):
    text = md_path.read_text(encoding='utf-8')
    new_text = strip_liquid(text, md_path)
    if new_text != text:
        md_path.write_text(new_text, encoding='utf-8')


def collect_nav():
    """Scan markdown files and build a nested nav tree preserving nav_order."""
    entries = []  # list of (levels tuple, order number, file_path)
    for md_file in SRC_DIR.rglob('*.md'):
        # ignore jekyll meta dirs
        if any(part.startswith('_') for part in md_file.relative_to(SRC_DIR).parts):
            continue

        # parse front matter if exists
        levels = []
        order_levels = []
        title = None
        nav_parent = None
        nav_grand = None
        nav_order = None

        with md_file.open('r', encoding='utf-8') as fh:
            first_line = fh.readline()
            if first_line.strip() == '---':
                fm_lines = []
                for line in fh:
                    if line.strip() == '---':
                        break
                    fm_lines.append(line)
                fm = yaml.safe_load(''.join(fm_lines)) or {}
                title = fm.get('title')
                nav_parent = fm.get('parent')
                nav_grand = fm.get('grand_parent')
                nav_order = fm.get('nav_order')
            else:
                # fallback: use first heading line or filename
                if first_line.startswith('#'):
                    title = first_line.lstrip('#').strip()
                else:
                    title = md_file.stem.replace('-', ' ').capitalize()

        # Ensure we have some title fallback
        if not title:
            title = md_file.stem.replace('-', ' ').capitalize()

        if nav_grand:
            levels.extend([nav_grand, nav_parent or '', title])
        elif nav_parent:
            levels.extend([nav_parent, title])
        else:
            levels.append(title)

        # ordering: nav_grand/nav_parent levels use nav_order of index file; simplified here by nav_order else 1000
        order = int(nav_order) if nav_order is not None else 1000
        order_levels.append(order)

        entries.append((tuple(levels), order, md_file))

    # Build tree structure
    tree = {}
    for (levels, order, path) in entries:
        current = tree
        for i, level in enumerate(levels):
            if level not in current:
                current[level] = {"_meta": {"order": 1000}}
            if i == len(levels) - 1:
                current[level]["_file"] = str(path.relative_to(SRC_DIR))
                current[level]["_meta"]['order'] = order
            else:
                current = current[level]
                # propagate order for sections
                if current['_meta']['order'] > order:
                    current['_meta']['order'] = order

    # convert tree to nav list
    def build_nav(subtree):
        items = []
        # sort keys by _meta.order then alpha
        for key, value in sorted(
            ((k, v) for k, v in subtree.items() if k != '_meta'),
            key=lambda kv: (kv[1]['_meta']['order'], kv[0].lower())):
            children = [k for k in value.keys() if not k.startswith('_')]
            has_children = len(children) > 0
            if '_file' in value and not has_children:
                items.append({key: value['_file']})
            elif '_file' in value and has_children:
                # Build list starting with overview page then children
                children_subtree = {k: v for k, v in value.items() if k not in ('_file', '_meta')}
                children_subtree['_meta'] = value['_meta']
                children_nav = build_nav(children_subtree)
                section = [{'Overview': value['_file']}] + children_nav
                items.append({key: section})
            else:
                items.append({key: build_nav(value)})
        return items

    nav = build_nav(tree)

    # build redirect map from *.html to directory URLs
    redirect_map = {}
    for (_, _, path) in entries:
        rel = path.relative_to(SRC_DIR)
        rel_str = rel.as_posix()
        if rel_str.startswith('src/'):
            rel_str = rel_str[4:]
        url_no_ext = rel_str[:-3]  # strip .md
        # file.html -> url/
        target_to = url_no_ext + '/'
        redirect_map[url_no_ext + '.html'] = target_to
        # index files: directory/index.html -> directory/
        if rel.name == 'index.md':
            dir_url = rel.parent.as_posix()
            target_idx = (dir_url + '/') if dir_url else ''
            redirect_map[dir_url + '/index.html'] = target_idx

    return nav, redirect_map


def generate_mkdocs_yaml(nav):
    config = {
        'site_name': 'lakeFS Documentation',
        'site_url': 'https://docs.lakefs.io',
        'repo_url': 'https://github.com/treeverse/lakeFS',
        'site_description': 'Open Source Data Version Control for Data Lakes and Lakehouses',
        'theme': {
            'name': 'material',
            'logo': 'assets/logo.svg',
        },
        # when config is inside docs/ we keep markdown files alongside it
        'site_dir': 'site',  # relative to docs/
        'docs_dir': 'src',
        'use_directory_urls': True,
        'plugins': [
            'search',
            'redirects',
        ],
        'markdown_extensions': [
            'toc',
            'tables',
            'fenced_code',
            'attr_list',
            'admonition',
            'codehilite',
        ],
        'nav': nav,
    }
    mkdocs_yml = DOCS_DIR / 'mkdocs.yml'
    with mkdocs_yml.open('w', encoding='utf-8') as out:
        yaml.dump(config, out, sort_keys=False)
    print(f"Written {mkdocs_yml}")


def main():
    print("Transforming markdown files...")
    for md_file in SRC_DIR.rglob('*.md'):
        if any(part.startswith('_') for part in md_file.relative_to(SRC_DIR).parts):
            continue  # skip jekyll asset dirs
        transform_markdown(md_file)
    print("Collecting nav structure and redirects...")
    nav, redirects = collect_nav()
    print("Generating mkdocs.yml ...")
    generate_mkdocs_yaml(nav)
    # update mkdocs.yml with redirects mapping
    mkdocs_yml = DOCS_DIR / 'mkdocs.yml'
    cfg = yaml.safe_load(mkdocs_yml.read_text(encoding='utf-8'))
    # Remove any legacy top-level redirects key
    if 'redirects' in cfg:
        cfg.pop('redirects')

    plugins = cfg.get('plugins', [])
    redirects_plugin = None
    for p in plugins:
        if (isinstance(p, str) and p == 'redirects') or (isinstance(p, dict) and 'redirects' in p):
            redirects_plugin = p
            break
    if redirects_plugin is None:
        plugins.append({'redirects': {'redirect_maps': redirects}})
    else:
        if isinstance(redirects_plugin, str):
            plugins[plugins.index(redirects_plugin)] = {'redirects': {'redirect_maps': redirects}}
        else:
            redirects_plugin['redirects']['redirect_maps'] = redirects
    cfg['plugins'] = plugins
    mkdocs_yml.write_text(yaml.dump(cfg, sort_keys=False), encoding='utf-8')
    # Remove legacy root mkdocs.yml if exists to prevent confusion
    legacy_cfg = DOCS_DIR.parent / 'mkdocs.yml'
    if legacy_cfg.exists():
        legacy_cfg.unlink()
        print("Removed legacy mkdocs.yml from repository root.")
    print(f"Updated redirects in {mkdocs_yml}")
    print("Done.")

if __name__ == '__main__':
    main() 