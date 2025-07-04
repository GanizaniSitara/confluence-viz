"""
Main CLI entry point for Confluence visualization.
Refactored version of the original viz.py using modular components.
"""

import sys
from typing import Optional

from ..data.collector import ConfluenceDataCollector
from ..visualization.treemap import TreemapGenerator


def main(
    config_path: str = 'settings.ini',
    output_json: str = "confluence_data.json",
    output_html: str = "confluence_treepack.html",
    auto_open: bool = True
):
    """
    Main function that orchestrates the complete data collection and visualization process.
    
    Args:
        config_path: Path to configuration file
        output_json: Output JSON file path
        output_html: Output HTML file path
        auto_open: Whether to automatically open the visualization in browser
    """
    try:
        # Initialize data collector
        print("Initializing Confluence data collector...")
        collector = ConfluenceDataCollector(config_path)
        
        # Test connection first
        print("Testing Confluence connection...")
        success, message = collector.api_client.test_connection()
        if not success:
            print(f"Connection test failed: {message}", file=sys.stderr)
            return 1
        print(f"Connection test successful: {message}")
        
        # Fetch all spaces
        spaces = collector.fetch_all_spaces()
        if not spaces:
            print("No spaces found. Exiting.", file=sys.stderr)
            return 1
            
        # Process spaces and collect page data
        processed_spaces, space_avg_timestamps, total_pages = collector.process_spaces_with_page_data(spaces)
        
        # Calculate color assignments
        percentile_thresholds, color_range_hex = collector.calculate_color_assignments(
            processed_spaces, space_avg_timestamps
        )
        
        # Build data structure
        data = collector.build_data_structure(processed_spaces)
        
        # Save data files
        collector.save_json(data, output_json)
        collector.save_data(data, "confluence_data.pkl")
        
        # Generate visualization
        generator = TreemapGenerator()
        generator.save_html(
            data=data,
            percentile_thresholds=percentile_thresholds,
            color_range_hex=color_range_hex,
            output_file=output_html,
            auto_open=auto_open
        )
        
        print(f"\n🎉 Process completed successfully!")
        print(f"📊 Processed {len(processed_spaces)} spaces with {total_pages} total pages")
        print(f"📁 Data saved to: {output_json} and confluence_data.pkl")
        print(f"🌐 Visualization saved to: {output_html}")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"❌ Error during processing: {e}", file=sys.stderr)
        return 1


def cli_main():
    """Command-line interface entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Confluence Visualization Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Run with default settings
  %(prog)s --config custom.ini      # Use custom config file
  %(prog)s --no-browser             # Don't open browser automatically
  %(prog)s --output viz.html        # Custom output filename
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        default='settings.ini',
        help='Path to configuration file (default: settings.ini)'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='confluence_treepack.html',
        help='Output HTML file path (default: confluence_treepack.html)'
    )
    
    parser.add_argument(
        '--json-output',
        default='confluence_data.json',
        help='Output JSON file path (default: confluence_data.json)'
    )
    
    parser.add_argument(
        '--no-browser',
        action='store_true',
        help='Do not automatically open the visualization in browser'
    )
    
    parser.add_argument(
        '--version', '-v',
        action='version',
        version='confluence-viz 0.1.0'
    )
    
    args = parser.parse_args()
    
    # Run the main process
    exit_code = main(
        config_path=args.config,
        output_json=args.json_output,
        output_html=args.output,
        auto_open=not args.no_browser
    )
    
    sys.exit(exit_code)


if __name__ == '__main__':
    cli_main()