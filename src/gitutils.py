import re


def commit_shortref(commit: str) -> str:
    return commit[:8]


def clean_branch(branch: str) -> str:
    """Strip anything but alphanum from branch name."""
    return re.sub(r"[\W_]+", "", branch)
