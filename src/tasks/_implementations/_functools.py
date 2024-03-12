import traceback
import fnmatch
import re

def match_pattern(url, patterns = [
        "*/_implementations/error/*.py",
        "*/_implementations/error/abstract.py",
        "*/_implementations/*.py",
    ]):
    # Define the list of patterns to match against
    

    # Convert patterns to regular expressions
    regex_patterns = [fnmatch.translate(pattern) for pattern in patterns]

    # Check if the url matches any pattern
    for regex_pattern in regex_patterns:
        if re.match(regex_pattern, url):
            return True

    return False


def clean_raise(exception: BaseException):
    exception.__traceback__ = None
    raise exception from None