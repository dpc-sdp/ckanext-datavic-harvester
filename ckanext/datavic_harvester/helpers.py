import logging

import ckan.plugins.toolkit as tk


log = logging.getLogger(__name__)


def convert_date_to_isoformat(value):
    """
    Example dates:
        '2020-10-13t05:00:11'
        u'2006-12-31t13:00:00.000z'
    :param value:
    :return:
    """
    date = None
    try:
        # Remove any microseconds
        value = value.split(".")[0]
        if "t" in value.lower():
            date = tk.get_converter("isodate")(value, {})
    except tk.Invalid as ex:
        log.debug("Date format incorrect {0}".format(value))
    # TODO: Do we return None or value if date string cannot be converted?
    return date.isoformat() if date else None
