# Confluence Visualization 2.0 - Cleanup & Reorganization Plan

**Created:** 2026-01-23
**Updated:** 2026-01-28
**Status:** In Progress
**Purpose:** Document current state, identify issues, plan systematic cleanup

---

## Quick Wins Completed

- [x] **Phase 1:** Extracted timeline search to separate repo, removed `confluence_unified_search/`
- [x] **README:** Added Quick Start guide
- [x] **README:** Added "Which Script Should I Use?" decision tree
- [x] **requirements.txt:** Updated with actual dependencies

---

## Current State Summary

The project has grown organically into a comprehensive Confluence data extraction, analysis, and visualization toolkit. It works but has accumulated:
- Multiple variants of similar scripts (now documented)
- Inconsistent configuration approaches
- Some stubs and unused code
- ~~A subdirectory (`confluence_unified_search`) that should be its own repo~~ **DONE - removed**
- ~~Missing/incomplete requirements.txt~~ **DONE - updated**

---

## Phase 1: Extract Confluence Timeline Search to Separate Repo

**Status:** ✅ COMPLETE (2026-01-28)

Timeline search has been extracted to a separate repository. The `confluence_unified_search/` directory has been removed from this repo.

---

## Phase 2: Verify & Document Working Features

### Core Data Pipeline
| Script | Status | Action Needed |
|--------|--------|---------------|
| `sample_and_pickle_spaces.py` | WORKING | Document modes (FULL, SAMPLE, resume) |
| `sample_and_pickle_attachments.py` | WORKING | Verify still needed |
| `config_loader.py` | WORKING | None |
| `utils/html_cleaner.py` | WORKING | Apply namespace fix (see Phase 3) |

### Exploration Tools
| Script | Status | Action Needed |
|--------|--------|---------------|
| `explore_clusters.py` | WORKING | Recently fixed (namespace handling) |
| `explore_pickle_content.py` | WORKING | None |
| `space_explorer.py` | WORKING | Verify vs explore_pickle_content overlap |
| `diagnose_pickle_bodies.py` | WORKING | None - diagnostic tool |

### Visualization
| Script | Status | Action Needed |
|--------|--------|---------------|
| `render_html.py` | WORKING | Recently refactored |
| `scatter_plot_visualizer.py` | WORKING | None |
| `proximity_visualizer.py` | WORKING | None |
| `render_semantic_html.py` | WORKING | Document when to use |
| `confluence_treemap_visualizer.py` | UNCLEAR | Check if duplicate of render_html.py |

### Open-WebUI Integration
| Script | Status | Action Needed |
|--------|--------|---------------|
| `open-webui.py` | WORKING | Main uploader |
| `open-webui-parallel.py` | WORKING | Document when to use over main |
| `open-webui-sideloader-gpu.py` | WORKING | Document GPU requirements |
| `open-webui-sideloader-gpu-inspect.py` | WORKING | Diagnostic variant |
| `open-webui-out_of_band_doc_loader.py` | WORKING | Tika integration |
| `probe_openwebui.py` | WORKING | Connection test |

---

## Phase 3: Apply Pending Fixes (NEEDS TESTING FIRST)

### HTML Cleaner Namespace Fix
**Issue:** `utils/html_cleaner.py` uses `html.parser` which doesn't handle Confluence XML namespace tags (`ac:`, `ri:`).
**Hypothesis:** This causes content inside `<ac:rich-text-body>` etc. to be lost during cleaning.
**Fix Applied:** `explore_clusters.py` (commit f2b78bb)
**Still Needed:** Apply same fix to `utils/html_cleaner.py`

**CAVEAT:** This fix is theoretical - not yet confirmed to solve the work Confluence issue. Need to:
1. First diagnose actual cause using `explore_pickle_content.py`
2. Confirm raw HTML has content but cleaned is empty
3. Only then apply fix

```python
# Proposed fix - add before BeautifulSoup parsing:
normalized = re.sub(r'<(/?)(\w+):', r'<\1\2-', html_content)
normalized = re.sub(r'<(\w+):(\w+)\s*/>', r'<\1-\2/>', normalized)
try:
    soup = BeautifulSoup(normalized, 'lxml')
except Exception:
    soup = BeautifulSoup(normalized, 'html.parser')
```

### Testing Steps
- [ ] Pull latest at work
- [ ] Run `python explore_pickle_content.py SPACENAME`
- [ ] Option 5 → press `r` to view raw HTML
- [ ] Check: Does raw HTML have content?
  - **YES, raw has content** → press `c` to view cleaned → is it empty?
    - **YES, cleaned is empty** → namespace fix is likely the solution
    - **NO, cleaned has content** → issue is elsewhere (not this fix)
  - **NO, raw is empty** → issue is in data collection, not cleaning
- [ ] If fix confirmed needed, apply to `utils/html_cleaner.py`
- [ ] Test again to verify fix works
- [ ] Commit and push

---

## Phase 4: Consolidate GENERIC_SCRIPTS

### Current State
```
GENERIC_SCRIPTS/
├── qdrant_markdown_uploader.py              [Main markdown uploader]
├── qdrant_md_uploader_with_postgres.py      [NEW - untracked]
├── qdrant_confluence_pickle_uploader.py     [With Ollama]
├── qdrant_confluence_pickle_uploader_direct.py    [Direct Nomic]
├── qdrant_confluence_pickle_uploader_no_ollama.py [sentence-transformers]
├── qdrant_tika_uploader.py                  [Office docs via Tika]
├── qdrant_confluence_update_after_baseline.py
├── check_knowledge_stats.py
├── SYNC_PROPOSAL.md                         [NEW - untracked]
├── README.md
└── settings.example.ini
```

### Actions
- [ ] Commit uncommitted files (qdrant_md_uploader_with_postgres.py, SYNC_PROPOSAL.md)
- [ ] Review if markdown uploaders can be merged
- [ ] Document which confluence pickle uploader variant to use when:
  - `_no_ollama.py` - When Ollama not available, use sentence-transformers locally
  - `_direct.py` - Direct Nomic embeddings
  - Main - Full Ollama integration
- [ ] Consider if these should be a separate repo (`qdrant-loaders`?)

---

## Phase 5: Clean Up Duplicate/Legacy Code

### Clarified "Duplicates" (2026-01-28)

These are NOT duplicates - they serve different purposes:

| File 1 | File 2 | Difference |
|--------|--------|------------|
| `render_html.py` | `confluence_treemap_visualizer.py` | **render_html.py** reads from pickles (current workflow). **confluence_treemap_visualizer.py** fetches live from API (legacy). |
| `space_explorer.py` | `explore_pickle_content.py` | **space_explorer.py** explores live API. **explore_pickle_content.py** browses pickle files. |

**Decision:** Keep both pairs. Mark `confluence_treemap_visualizer.py` as legacy in its docstring.

### Scripts to Verify Still Needed

### Potential Duplicates to Investigate

| File 1 | File 2 | Action |
|--------|--------|--------|
| Multiple OpenWebUI uploaders | - | Document when to use each (see README) |

### Scripts to Verify Still Needed
- [ ] `semantic_analysis.py` - Not in main workflow, keep or archive?
- [ ] `confluence_test_data_generator.py` - Test data generator, keep for dev?
- [ ] `flexible_data_types.py` - Check if used anywhere
- [ ] `fix_json_float_error.py` - One-time fix or ongoing need?

### Test Scripts Audit
```
test_*.py files (10+)
```
- [ ] Verify each test script still runs
- [ ] Remove tests for deprecated features
- [ ] Consider pytest migration for better test organization

---

## Phase 6: Fix Configuration Inconsistencies

### Issue: Hardcoded Values
`counter_pages_from_pickles.py` has:
```python
settings['api_base_url']  # Should be settings['base_url']
```

- [ ] Audit all scripts for hardcoded URLs/paths
- [ ] Migrate all to use `config_loader.py`

### Issue: Multiple Settings Files
```
settings.ini                      [Main config]
settings.example.ini              [Template]
settings_gpu_load.ini             [GPU variant]
settings_gpu_load.example.ini     [GPU template]
GENERIC_SCRIPTS/settings.example.ini  [Extended for Qdrant]
```

- [ ] Document which settings file each script uses
- [ ] Consider consolidating into single settings.ini with all sections

---

## Phase 7: Update requirements.txt

### Current (Incomplete)
```
requests
numpy
beautifulsoup4
```

### Actual Dependencies
```
# Core
requests
numpy
beautifulsoup4
lxml

# Analysis
scikit-learn
whoosh  # optional, for full-text search

# Visualization
matplotlib

# Database (optional - for OpenWebUI integration)
psycopg2-binary
pgvector

# Vector DB (optional - for Qdrant integration)
qdrant-client

# Embeddings (optional - pick one)
ollama
sentence-transformers

# Document Processing (optional)
tika
tqdm

# Web Service (optional - for unified search)
flask
```

- [ ] Create requirements.txt with core deps
- [ ] Create requirements-full.txt with all optional deps
- [ ] Document which features need which optional deps

---

## Phase 8: Documentation Updates

### README.md Improvements
- [x] Added troubleshooting section (commit 9ffa0aa)
- [ ] Add architecture diagram
- [ ] Add "which script to use when" decision tree
- [ ] Document all settings.ini sections

### New Documentation Needed
- [ ] GENERIC_SCRIPTS/README.md - Expand with usage examples
- [ ] CONTRIBUTING.md - How to add new features
- [ ] ARCHITECTURE.md - System design and data flow

---

## Phase 9: Project Structure Reorganization (TBD)

**Status:** NOT DECIDED - needs more thought

### Options

#### Option A: Simple Directory Reorganization
Move files into subdirectories but keep as standalone scripts.
```
confluence_visualization/
├── src/
│   ├── collectors/           [Data extraction]
│   ├── explorers/            [Interactive tools]
│   ├── visualizers/          [Rendering]
│   ├── uploaders/            [External integrations]
│   └── utils/
├── tests/
├── docs/
├── data/                     [Replaces temp/]
└── config/
```
**Pros:** Organized, low effort
**Cons:** Not installable, import paths change

#### Option B: Python Package
Convert to installable package with CLI entry points.
```
confluence_viz/
├── pyproject.toml
├── src/
│   └── confluence_viz/
│       ├── __init__.py
│       ├── collectors/
│       ├── explorers/
│       ├── visualizers/
│       ├── uploaders/
│       └── utils/
├── tests/
└── docs/
```
**Pros:** `pip install .`, proper imports, versioning, distributable
**Cons:** More effort, breaking change, need to learn packaging

#### Option C: Keep Flat, Just Clean Up
Remove dead code, fix inconsistencies, but keep current structure.
**Pros:** Minimal disruption, works today
**Cons:** Doesn't scale, harder to navigate

### Decision Needed
- [ ] Evaluate actual pain points with current structure
- [ ] Decide if package distribution is a goal
- [ ] Consider if GENERIC_SCRIPTS should be separate package
- [ ] Make decision after Phases 1-8 complete

**Note:** This is a breaking change - defer until other cleanup complete and we have clearer requirements

---

## Execution Order

1. ~~**Phase 1** - Extract timeline search~~ ✅ DONE
2. **Phase 3** - TEST HTML cleaner fix at work first, then apply if confirmed
3. **Phase 2** - Verify features (builds understanding)
4. **Phase 4** - Commit GENERIC_SCRIPTS changes
5. **Phase 5** - Clean up legacy scripts (mark, don't delete)
6. **Phase 6** - Fix config inconsistencies
7. ~~**Phase 7** - Update requirements~~ ✅ DONE
8. ~~**Phase 8** - Documentation~~ ✅ Quick Start + decision tree added
9. **Phase 9** - Restructure (TBD - maybe Python package, decide later)

---

## Quick Reference: What Works Today

### To fetch Confluence data:
```bash
python sample_and_pickle_spaces.py
```

### To explore/analyze spaces:
```bash
python explore_clusters.py
```

### To debug content issues:
```bash
python explore_pickle_content.py SPACENAME
# Option 5 → 'r' for raw → 'c' for cleaned
```

### To generate treemap:
```bash
python render_html.py
```

### To upload to OpenWebUI:
```bash
python open-webui.py              # Sequential
python open-webui-parallel.py     # Parallel (faster)
python open-webui-sideloader-gpu.py  # With GPU embeddings
```

### To upload to Qdrant:
```bash
cd GENERIC_SCRIPTS
python qdrant_confluence_pickle_uploader.py --space-keys SPACE1 SPACE2
```

---

## Notes & Decisions Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-01-23 | Extract unified search first | Independent Flask app, different concerns |
| 2026-01-23 | Keep multiple uploader variants | Different use cases (GPU, no-Ollama, parallel) |
| 2026-01-23 | Defer restructure to Phase 9 | Breaking change, need stability first |
