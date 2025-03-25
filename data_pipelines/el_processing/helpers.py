import re
import json
from datetime import datetime

def parse_data_string(data_str):
    """
    Parses a complex data string containing Python-specific data types
    into a JSON-compatible Python dictionary.
    """
    # Step 1: Replace single quotes with double quotes, except for apostrophes within words
    pattern = r"'"
    def replace_single_quotes(match):
        start, end = match.span()
        before = data_str[start - 1] if start > 0 else ''
        after = data_str[end] if end < len(data_str) else ''
        if before.isalpha() and after.isalpha():
            return "'"
        else:
            return '"'
    data_str = re.sub(pattern, replace_single_quotes, data_str)
    # Replace instances of "Bezeq" with 'Bezeq'
    data_str = data_str.replace('"Bezeq"', "'Bezeq'")

    # Step 2: Replace Python None with JSON null
    data_str = data_str.replace('None', 'null')

    # Step 3: Replace datetime objects with ISO format strings
    def replace_datetime(match):
        date_str = match.group(0)
        components = re.findall(r'\d+', date_str)
        int_components = [int(comp) for comp in components[:7]]  # Up to microseconds
        dt = datetime(*int_components)
        return f'"{dt.isoformat()}"'
    data_str = re.sub(r'datetime\.datetime\([^\)]+\)', replace_datetime, data_str, flags=re.DOTALL)

    # Step 4: Remove 'array' constructs entirely, keeping the content inside
    def remove_array_constructs(s):
        pattern = re.compile(r'array\s*(\(|\{|\[)', re.MULTILINE)
        while True:
            match = pattern.search(s)
            if not match:
                break
            start_idx = match.start()
            open_bracket = match.group(1)
            close_bracket = { '(': ')', '[': ']', '{': '}' }[open_bracket]
            content_start = match.end()
            content_end, end_idx = find_matching_bracket(s, content_start, open_bracket, close_bracket)
            content = s[content_start:content_end]
            content = remove_array_constructs(content)
            s = s[:start_idx] + content + s[end_idx:]
        return s

    def find_matching_bracket(s, idx, open_bracket, close_bracket):
        stack = 1
        i = idx
        while i < len(s):
            if s[i] == open_bracket:
                stack += 1
            elif s[i] == close_bracket:
                stack -= 1
                if stack == 0:
                    return i, i + 1
            i += 1
        raise ValueError(f"Unmatched {open_bracket} at position {idx}")

    data_str = remove_array_constructs(data_str)

    # Step 5: Remove any remaining 'dtype=object' and 'tzinfo' strings
    data_str = re.sub(r',\s*dtype=object', '', data_str)
    data_str = re.sub(r',\s*tzinfo=<[^>]*>', '', data_str)

    # Step 6: Remove HTML content inside strings
    def remove_html_content(match):
        return '""'
    html_pattern = r'"\<.*?\>"'
    data_str = re.sub(html_pattern, remove_html_content, data_str, flags=re.DOTALL)

    # Step 7: Clean up extra spaces and newlines
    data_str = re.sub(r'\s+', ' ', data_str)

    # Replace Python True/False with JSON true/false
    data_str = data_str.replace('True', 'true').replace('False', 'false')

    # Step 8: Parse the string into a Python dictionary
    try:
        data = json.loads(data_str)
    except json.JSONDecodeError as e:
        print("Error parsing JSON:", e)
        print("Problematic data string:", data_str)
        data = None

    return data
