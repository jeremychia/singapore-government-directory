import argparse

from ministries import ministries_url


def validate_ministry(ministry_name):
    """
    Validate if the input ministry name is present in the ministries_url dictionary.
    """

    if ministry_name in ministries_url:
        return ministry_name
    else:
        raise argparse.ArgumentTypeError(
            f"Invalid ministry name: '{ministry_name}'. Please choose from: {', '.join(ministries_url.keys())}"
        )
