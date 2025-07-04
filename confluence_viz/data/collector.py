"""
Data collection module for fetching and processing Confluence data.
"""

import json
import pickle
import sys
from datetime import datetime
from typing import List, Dict, Any, Tuple

from ..api.client import ConfluenceAPIClient
from ..utils.color_utils import (
    calculate_percentile_thresholds,
    get_color_for_avg_timestamp_percentile,
    generate_gradient_colors,
    GRADIENT_STEPS,
    GREY_COLOR_HEX
)


class ConfluenceDataCollector:
    """
    Handles the collection and processing of Confluence data.
    """
    
    def __init__(self, config_path: str = 'settings.ini'):
        """Initialize the data collector."""
        self.api_client = ConfluenceAPIClient(config_path)
        self.spaces_page_limit = 50
        self.content_page_limit = 100
    
    def fetch_all_spaces(self) -> List[Dict[str, Any]]:
        """
        Fetch all non-user spaces from Confluence.
        
        Returns:
            List of space dictionaries with 'key' and 'name' fields
        """
        print("Fetching all spaces...")
        spaces = []
        start = 0
        idx = 0
        
        while True:
            resp = self.api_client.get_spaces(start=start, limit=self.spaces_page_limit)
            if resp.status_code != 200:
                print(f"Failed to fetch spaces. Status code: {resp.status_code}", file=sys.stderr)
                break
                
            results = resp.json().get("results", [])
            if not results:
                break
                
            for sp in results:
                if sp.get("key", "").startswith("~"):  # Exclude user spaces
                    print(f"Skipping user space: key={sp.get('key')}, name={sp.get('name')}")
                    continue
                    
                idx += 1
                print(f"[{idx}] Fetched space: key={sp.get('key')}, name={sp.get('name')}")
                spaces.append({"key": sp.get("key"), "name": sp.get("name")})
                
            if len(results) < self.spaces_page_limit:
                break
            start += self.spaces_page_limit
            
        print(f"Finished fetching spaces. Total fetched: {len(spaces)}")
        return spaces
    
    def fetch_page_data_for_space(self, space_key: str) -> Tuple[int, List[float]]:
        """
        Fetch page data and timestamps for a specific space.
        
        Args:
            space_key: The space key to fetch data for
            
        Returns:
            Tuple of (page_count, timestamp_list)
        """
        count = 0
        timestamps = []
        start = 0
        print(f"  Fetching pages for space: {space_key}")
        
        while True:
            resp = self.api_client.get_pages_for_space(
                space_key=space_key,
                start=start,
                limit=self.content_page_limit
            )
            
            if resp.status_code != 200:
                print(f"  Failed to fetch pages for space {space_key}. Status code: {resp.status_code}", file=sys.stderr)
                break
                
            pages = resp.json().get("results", [])
            if not pages and start == 0:
                print(f"  No pages found for space: {space_key}")
                break
            if not pages:
                print(f"  No more pages found for space: {space_key} on subsequent pages.")
                break
                
            for p in pages:
                when = p.get("version", {}).get("when")
                if when:
                    try:
                        ts = datetime.fromisoformat(when.replace("Z", "+00:00")).timestamp()
                        timestamps.append(ts)
                    except ValueError:
                        print(f"Warning: Could not parse timestamp '{when}' for page ID {p.get('id')} in space {space_key}", file=sys.stderr)
                        pass  # Skip invalid timestamps
                        
            count += len(pages)
            if len(pages) < self.content_page_limit:
                break
            start += self.content_page_limit
            
        return count, timestamps
    
    def process_spaces_with_page_data(self, spaces: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[float], int]:
        """
        Process all spaces and collect page data.
        
        Args:
            spaces: List of space dictionaries
            
        Returns:
            Tuple of (processed_spaces, all_timestamps, total_pages_fetched)
        """
        all_ts = []  # All individual page edit timestamps
        total_pages_fetched = 0
        space_avg_timestamps = []  # List to store average timestamps for percentile calculation
        
        if not spaces:
            print("No spaces fetched. Cannot proceed with page fetching or visualization.")
            return [], [], 0
            
        print("\nStarting page data fetching for each space...")
        
        for idx, sp in enumerate(spaces, start=1):
            print(f"Processing space {idx}/{len(spaces)}: key={sp['key']} ({sp['name']})")
            count, ts = self.fetch_page_data_for_space(sp["key"])
            
            # Calculate AVERAGE timestamp for this space. Avg is 0 if no timestamps found.
            avg = (sum(ts) / len(ts)) if ts else 0
            sp["value"] = count  # Use count as size
            sp["avg"] = avg      # Store avg timestamp for color mapping
            all_ts.extend(ts)    # Add ALL individual timestamps to the collective list
            total_pages_fetched += count
            
            if avg > 0:  # Only include spaces with pages/valid timestamps in percentile calculation
                space_avg_timestamps.append(avg)
                
            try:
                avg_iso = datetime.fromtimestamp(avg).isoformat(sep=' ', timespec='seconds') if avg > 0 else 'N/A'
            except (ValueError, OSError):
                avg_iso = "Invalid Timestamp"
            print(f"  Finished processing space {sp['key']}. Pages: {count}, Avg Last Edit Timestamp: {avg:.4f} ({avg_iso})")
            
        print(f"\nFinished processing all spaces. Total pages fetched across all spaces: {total_pages_fetched}")
        
        # Print overall statistics
        minT_overall, maxT_overall = (min(all_ts), max(all_ts)) if all_ts else (0, 0)
        try:
            minT_iso = datetime.fromtimestamp(minT_overall).isoformat(sep=' ', timespec='seconds') if minT_overall > 0 else 'N/A'
            maxT_iso = datetime.fromtimestamp(maxT_overall).isoformat(sep=' ', timespec='seconds') if maxT_overall > 0 else 'N/A'
        except (ValueError, OSError):
            minT_iso = maxT_iso = "Invalid Timestamp"
            
        print(f"\nOverall Min Timestamp (Oldest Page Edit): {minT_overall:.4f} ({minT_iso})")
        print(f"Overall Max Timestamp (Newest Page Edit): {maxT_overall:.4f} ({maxT_iso})")
        
        return spaces, space_avg_timestamps, total_pages_fetched
    
    def calculate_color_assignments(self, spaces: List[Dict[str, Any]], space_avg_timestamps: List[float]) -> Tuple[List[float], List[str]]:
        """
        Calculate color assignments for spaces based on percentile thresholds.
        
        Args:
            spaces: List of space dictionaries
            space_avg_timestamps: List of average timestamps for spaces with activity
            
        Returns:
            Tuple of (percentile_thresholds, color_range_hex)
        """
        print("\n--- Percentile Thresholds for Space Average Timestamps ---")
        num_spaces_with_avg = len(space_avg_timestamps)
        print(f"Calculating {GRADIENT_STEPS} color bins based on {num_spaces_with_avg} spaces with page activity.")
        
        if num_spaces_with_avg < GRADIENT_STEPS:
            print(f"Warning: Fewer spaces with page activity ({num_spaces_with_avg}) than gradient steps ({GRADIENT_STEPS}).")
            unique_avg_timestamps = sorted(list(set(space_avg_timestamps)))
            percentile_thresholds = unique_avg_timestamps[:GRADIENT_STEPS - 1]
            print(f"Using {len(percentile_thresholds)} unique average timestamps as thresholds:")
            for i, ts in enumerate(percentile_thresholds):
                try:
                    iso_time = datetime.fromtimestamp(ts).isoformat(sep=' ', timespec='seconds')
                except (ValueError, OSError):
                    iso_time = "Invalid Timestamp"
                print(f"  Threshold {i+1}: {ts:.4f} ({iso_time})")
        elif num_spaces_with_avg == 0:
            percentile_thresholds = []
            print("No spaces with page activity found. No percentile thresholds calculated.")
        else:
            percentile_thresholds = calculate_percentile_thresholds(space_avg_timestamps, GRADIENT_STEPS)
            print(f"Calculated {len(percentile_thresholds)} percentile thresholds:")
            print("Index | Timestamp          | ISO Format")
            print("------|--------------------|----------------------")
            for i, ts in enumerate(percentile_thresholds):
                try:
                    iso_time = datetime.fromtimestamp(ts).isoformat(sep=' ', timespec='seconds')
                except (ValueError, OSError):
                    iso_time = "Invalid Timestamp"
                print(f"{i:<5} | {ts:<18.4f} | {iso_time}")
        
        # Generate color range
        color_range_hex = generate_gradient_colors(GRADIENT_STEPS)
        print(f"\nGenerating {GRADIENT_STEPS} colors for the gradient (Red=Old, Green=New):")
        print("Index | Color (RGB Hex)")
        print("------|----------------")
        for i, hex_color in enumerate(color_range_hex):
            print(f"{i:<5} | {hex_color}")
        print("----------------------------\n")
        
        # Print space coloring details
        self._print_space_coloring_details(spaces, percentile_thresholds, color_range_hex)
        
        return percentile_thresholds, color_range_hex
    
    def _print_space_coloring_details(self, spaces: List[Dict[str, Any]], percentile_thresholds: List[float], color_range_hex: List[str]):
        """Print detailed color assignment information for each space."""
        print("\n--- Space Coloring Details (Based on Percentile Bin) ---")
        if not spaces:
            print("No space data to display coloring details.")
            return
            
        effective_color_range = color_range_hex if color_range_hex else [GREY_COLOR_HEX]
        
        print("Avg Edit Timestamp   | Space Key   | Assigned Color (Hex)")
        print("---------------------|-------------|----------------------")
        
        # Sort spaces by average timestamp for easier review, newest first
        sorted_spaces = sorted(spaces, key=lambda x: x.get('avg', 0), reverse=True)
        
        for sp in sorted_spaces:
            avg_ts = sp.get('avg', 0)
            space_key = sp.get('key', 'N/A')
            
            assigned_color = get_color_for_avg_timestamp_percentile(
                avg_ts, percentile_thresholds, effective_color_range, default_color_hex=GREY_COLOR_HEX
            )
            
            print(f"{avg_ts:<20.4f} | {space_key:<11} | {assigned_color}")
        print("-----------------------------\n")
    
    def build_data_structure(self, spaces: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Build the hierarchical data structure for visualization.
        
        Args:
            spaces: List of processed space dictionaries
            
        Returns:
            Hierarchical data structure
        """
        return {"name": "Confluence", "children": spaces}
    
    def save_data(self, data: Dict[str, Any], output_file: str = "confluence_data.pkl"):
        """
        Save data to pickle file.
        
        Args:
            data: Data structure to save
            output_file: Output file path
        """
        print(f"Saving data to {output_file}...")
        with open(output_file, "wb") as f:
            pickle.dump(data, f)
        print(f"Data successfully saved to {output_file}")
    
    def save_json(self, data: Dict[str, Any], output_file: str = "confluence_data.json"):
        """
        Save data to JSON file.
        
        Args:
            data: Data structure to save
            output_file: Output file path
        """
        print(f"Writing data to {output_file}...")
        with open(output_file, "w", encoding="utf-8") as jf:
            json.dump(data, jf, indent=2)
        print(f"Successfully created {output_file}")


# Convenience functions for backward compatibility
def fetch_all_spaces() -> List[Dict[str, Any]]:
    """Fetch all spaces using the data collector."""
    collector = ConfluenceDataCollector()
    return collector.fetch_all_spaces()


def fetch_page_data_for_space(space_key: str) -> Tuple[int, List[float]]:
    """Fetch page data for a space using the data collector."""
    collector = ConfluenceDataCollector()
    return collector.fetch_page_data_for_space(space_key)


def build_data(spaces: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build data structure using the data collector."""
    collector = ConfluenceDataCollector()
    return collector.build_data_structure(spaces)