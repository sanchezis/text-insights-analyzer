from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, StringType, IntegerType, ArrayType
from pyspark.sql.types import ArrayType, DoubleType, FloatType
import re
from collections import Counter

import pyspark.sql.functions as F # type: ignore


# Define patterns to search for
patterns = {
    "s1_restarts": {
        "pattern": r'S1-START.*?(?=S1-START)',
        "description": "Detected S1 system restarts"
    },
    "s2_restarts": {
        "pattern": r'S2-START.*?(?=S2-START)',
        "description": "Detected S2 system restarts"
    },
    "time_to_s2_start_from_first_s1_start": {
        "pattern": r'S1-START.*?(?=S2-START)',
        "description": "time to start s2"
    },
    "reboot_loops": {
        "pattern": r'(S2-REBOOT\s+.*S2-REBOOT)',
        "description": "System caught in reboot loop"
    },
    "error_chains": {
        "pattern": r'S[12]+-[A-Z_]+ERROR.*?(?=S[12]+-[A-Z_]+ERROR)',
        "description": "Multiple errors occurring in sequence"
    },
    "contradictory_equipment": {
        "pattern": r'S[12]+-LOCATE_EQUIPMENT_YES.*?S[12]+-LOCATE_EQUIPMENT_NO|S[12]+-LOCATE_EQUIPMENT_NO.*?S[12]+-LOCATE_EQUIPMENT_YES',
        "description": "Equipment status changes illogically"
    },
    "cable_problems": {
        "pattern": r'S[12]-CABLE_\w+.*?S[12]-CABLE_ERROR',
        "description": "Cable operations ending in errors"
    },
    "step_restart_cycling": {
        "pattern": r'S1-\w+.*?S2-\w+.*?S1-(?!CABLE_PLUG\b)\w+',
        "description": ""
    },
    "setup_device": {
        "pattern": r'S[12]+[A-Z_]*-SETUP_DEVICE',
        "description": "Setup device operations followed by errors"
    },
    "error_after_setup": {
        "pattern": r'S[12]+-[A-Z_-]*SETUP_DEVICE\s+(?:S[12]+[A-Z_-]+\s+){0,5}?S[12]+[A-Z_-]*ERROR',
        "description": "Error shortly after device setup - potential setup failure"
    },
    "ai_to_error": {
        "pattern": r'S[12]+-[A-Z_]+_AI\s+(?:S[12]+[A-Z_-]+\s+){0,5}?S[12]+-[A-Z_]+ERROR',
        "description": "AI and shortly error happened."
    },    
    "time_need_camera": {
        "pattern": r'NEEDCAMERA',
        "description": "Asking to need the camera time spent"
    },    
    "taking_pictures": {
        "pattern": r'(S[12]+[A-Z_]*-TAKE-PHOTO|IMAGE_ANALYSIS|_AI)',
        "description": "time in picture analysis or take photo"
    },    

}


@F.udf(MapType(StringType(), FloatType()))
def analyze_log_sequence(log_sequence, elapsed_times=None):
    """
    Analyze a log sequence for various patterns and return a dictionary of results.
    
    Args:
        log_sequence (list): List of log event strings
        elapsed_times (list, optional): List of elapsed times corresponding to each log event
        
    Returns:
        dict: Dictionary of analysis results
    """
    try:
        if not log_sequence or not isinstance(log_sequence, list):
            return {}
        
        # Convert the list to a string for regex operations
        log_string = " ".join(log_sequence)
        
        # Dictionary to store all results
        results = {}
        
        global patterns

        # Search for each pattern and collect results with time analysis if available
        for pattern_name, pattern_info in patterns.items():
            pattern = pattern_info["pattern"]
            matches = list(re.finditer(pattern, log_string))
            
            if matches:
                # Store count of matches
                results[f"{pattern_name}_count"] = len(matches)
                # Store matched sequences (limited to first 5 to avoid excessive data)
                # matched_sequences = [match.group(0) for match in matches[:5]]
                # results[f"{pattern_name}_examples"] = matched_sequences
                
                # Calculate time information if elapsed_times is provided
                if elapsed_times and len(elapsed_times) == len(log_sequence):
                    pattern_times = []
                    for match in matches:
                        # Get the matched text and find its position in the original sequence
                        match_text = match.group(0).split()
                        
                        # Find all occurrences of the pattern in the original sequence
                        for i in range(len(log_sequence) - len(match_text) + 1):
                            if log_sequence[i:i+len(match_text)] == match_text:
                                # Calculate the total elapsed time for this pattern occurrence
                                pattern_elapsed_time = sum(elapsed_times[i:i+len(match_text)])
                                pattern_times.append(pattern_elapsed_time)
                                break
                    
                    # Store time metrics for this pattern
                    if pattern_times:
                        results[f"{pattern_name}_total_time"] = sum(pattern_times)

            else:
                results[f"{pattern_name}_count"] = 0
                if elapsed_times:
                    results[f"{pattern_name}_total_time"] = 0

        event_counts = Counter(log_sequence)
        
        # Calculate summary statistics
        for k, v in {
            "total_events": len(log_sequence),
            "unique_events": len(event_counts),
            "start_ratio": (event_counts.get("S1-START", 0) + event_counts.get("S2-START", 0)) / len(log_sequence) if log_sequence else 0,
            "error_ratio": sum(count for event, count in event_counts.items() if "ERROR" in event) / len(log_sequence) if log_sequence else 0,
        }.items():
            results[f"summary_{k}"] = v
        
        # Add time-based summary if elapsed_times is provided
        if elapsed_times and len(elapsed_times) == len(log_sequence):
            total_time = sum(elapsed_times)
            
            # Calculate time spent in different event types
            event_times = {}
            for event_type, count in event_counts.items():
                event_time = sum(elapsed_times[i] for i, event in enumerate(log_sequence) if event == event_type)
                event_times[f"{event_type}_total_time"] = event_time
                event_times[f"{event_type}_percentage"] = (event_time / total_time * 100) if total_time > 0 else 0
            
            # Calculate time spent in error states
            error_time = sum(elapsed_times[i] for i, event in enumerate(log_sequence) if "ERROR" in event)
            
            # Calculate time spent in restart processes
            restart_time = sum(elapsed_times[i] for i, event in enumerate(log_sequence) 
                                if event in ["S1-START", "S2-START"])
            
            # Add time metrics to summary
            for k, v in {
                "total_elapsed_time": total_time,
                "error_time": error_time,
                "error_time_percentage": (error_time / total_time * 100) if total_time > 0 else 0,
                "restart_time": restart_time,
                "restart_time_percentage": (restart_time / total_time * 100) if total_time > 0 else 0
            }.items():
                results[f"summary_{k}"] = v
            
            # Store event times
            for k,v in event_times.items():
                results[f"{k}_event_times"] = v
        
        return results
    except:
        return None


# Advanced version with schema-compatible return type
def create_structured_log_analyzer_udf():
    """
    Creates a more structured PySpark UDF that returns results in a format 
    that's more compatible with Spark SQL schema.
    """
    
    from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType, MapType, DoubleType
    
    # Define a more specific return type schema
    return_schema = MapType(StringType(), StringType())
    
    def analyze_log_sequence_structured(log_sequence, elapsed_times=None):
        """Same analyzer function but restructured return format for better Spark compatibility"""
        # Get the analysis results
        results = analyze_log_sequence(log_sequence, elapsed_times)
        
        # Convert all values to strings to comply with the MapType return type
        string_results = {}
        for key, value in results.items():
            if isinstance(value, (dict, list, tuple)):
                string_results[key] = str(value)
            else:
                string_results[key] = str(value)
                
        return string_results
        
    # For the standard analyzer function implementation, see the previous function
    
    return udf(analyze_log_sequence_structured, return_schema)

