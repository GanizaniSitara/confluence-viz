"""
Color utility functions for Confluence visualization.
Handles color calculations, gradients, and percentile-based coloring.
"""

import math
from typing import List, Tuple, Optional


# Default color configuration
GRADIENT_COLORS_FOR_INTERP = ['#ffcccc', '#ffffcc', '#ccffcc']  # Red (old) -> Yellow (mid) -> Green (new)
GREY_COLOR_HEX = '#cccccc'
GRADIENT_STEPS = 10  # Number of percentile bins/color steps


def hex_to_rgb(hex_color: str) -> Tuple[int, int, int]:
    """Converts a hex color string (e.g., '#RRGGBB') to an RGB tuple."""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def rgb_to_hex(rgb_color: Tuple[int, int, int]) -> str:
    """Converts an RGB tuple to a hex color string."""
    return '#{:02x}{:02x}{:02x}'.format(rgb_color[0], rgb_color[1], rgb_color[2])


def lerp_rgb(color1: Tuple[int, int, int], color2: Tuple[int, int, int], t: float) -> Tuple[int, int, int]:
    """Linear interpolation between two RGB colors. t is between 0 and 1."""
    return tuple(max(0, min(255, round(color1[i] + t * (color2[i] - color1[i])))) for i in range(3))


def get_interpolated_color_from_fraction(f: float, colors_rgb_stops: List[Tuple[int, int, int]]) -> Tuple[int, int, int]:
    """
    Gets a color from a gradient defined by color stops, based on fraction f (0 to 1).
    Uses linear interpolation between stops. f=0 maps to first color, f=1 maps to last.
    """
    num_stops = len(colors_rgb_stops)
    if num_stops == 0:
        return (0, 0, 0)  # Default black
    
    # Clamp f to [0, 1]
    f = max(0.0, min(1.0, f))

    if num_stops == 1:
        return colors_rgb_stops[0]
    if f == 1.0:  # Handle f=1.0 explicitly to ensure we get the last color
        return colors_rgb_stops[-1]

    # Calculate which segment of the gradient f falls into
    segment_index = int(f * (num_stops - 1))
    # Ensure segment_index doesn't exceed the second-to-last stop
    segment_index = min(segment_index, num_stops - 2)

    # Normalize position within the segment (0 to 1)
    t = (f * (num_stops - 1)) - segment_index

    return lerp_rgb(colors_rgb_stops[segment_index], colors_rgb_stops[segment_index + 1], t)


def calculate_percentile_thresholds(data: List[float], num_bins: int) -> List[float]:
    """
    Calculates the threshold values that divide sorted data into num_bins parts.
    Returns num_bins - 1 threshold values. Data must be sorted.
    Returns an empty list if data is empty or has fewer elements than num_bins - 1.
    """
    data = sorted(data)  # Ensure data is sorted
    n = len(data)
    if n == 0 or num_bins <= 1:
        return []  # Cannot calculate thresholds

    # We need num_bins - 1 thresholds to create num_bins bins.
    thresholds = []
    for i in range(num_bins - 1):
        rank = ((i + 1) / num_bins) * (n - 1)
        if rank < 0:  # Should not happen with i >= 0
            rank = 0
        if rank >= n - 1:  # Should not happen with i < num_bins - 1 unless n is too small
            rank = n - 1

        # Linear interpolation for fractional ranks
        lower_idx = math.floor(rank)
        upper_idx = math.ceil(rank)

        if lower_idx == upper_idx:
            thresholds.append(data[lower_idx])
        else:
            # Interpolate between the two values
            weight = rank - lower_idx
            interpolated_value = data[lower_idx] * (1 - weight) + data[upper_idx] * weight
            thresholds.append(interpolated_value)

    return thresholds


def get_color_for_avg_timestamp_percentile(
    avg_timestamp: float, 
    percentile_thresholds: List[float], 
    color_range_hex: List[str], 
    default_color_hex: str = GREY_COLOR_HEX
) -> str:
    """
    Calculates the hex color for an average timestamp based on which percentile
    bin it falls into defined by the thresholds.
    """
    if avg_timestamp == 0:  # Special case for spaces with no pages
        return default_color_hex

    # If there are no thresholds (e.g., < 2 spaces with pages, or num_bins <= 1),
    # or if all non-zero timestamps are the same, they all fall into one bin.
    if not percentile_thresholds:
        # Assign the color for the highest percentile (Green/newest) if no thresholds can be calculated
        return color_range_hex[-1] if color_range_hex else GREY_COLOR_HEX

    # Find which bin the timestamp falls into
    bin_index = 0
    for threshold in percentile_thresholds:
        if avg_timestamp >= threshold:
            bin_index += 1
        else:
            break  # Found the bin

    # Clamp bin_index to the available color range indices
    bin_index = max(0, min(bin_index, len(color_range_hex) - 1))

    return color_range_hex[bin_index]


def generate_gradient_colors(
    gradient_steps: int = GRADIENT_STEPS,
    color_stops: Optional[List[str]] = None
) -> List[str]:
    """
    Generates a list of hex colors for the gradient.
    
    Args:
        gradient_steps: Number of color steps to generate
        color_stops: List of hex color strings for the gradient stops
        
    Returns:
        List of hex color strings
    """
    if color_stops is None:
        color_stops = GRADIENT_COLORS_FOR_INTERP
    
    gradient_colors_rgb_basis = [hex_to_rgb(c) for c in color_stops]
    color_range_hex = []
    
    # Calculate the colors by interpolating the basis colors
    for i in range(gradient_steps):
        # f goes from 0 to 1 across the steps
        f = i / (gradient_steps - 1) if gradient_steps > 1 else 0.0
        rgb = get_interpolated_color_from_fraction(f, gradient_colors_rgb_basis)
        hex_color = rgb_to_hex(rgb)
        color_range_hex.append(hex_color)
    
    return color_range_hex