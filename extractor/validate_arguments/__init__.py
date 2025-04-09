import argparse

from ministries import ministries_url, organs_of_state_url


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


def validate_organs_of_state(organs_of_state):
    """
    Validate if the input ministry name is present in the organs_of_state_url dictionary.
    """

    if organs_of_state in organs_of_state_url:
        return organs_of_state
    else:
        raise argparse.ArgumentTypeError(
            f"Invalid ministry name: '{organs_of_state}'. Please choose from: {', '.join(organs_of_state_url.keys())}"
        )
