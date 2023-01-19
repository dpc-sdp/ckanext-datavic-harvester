# Based on https://gist.github.com/revotu/21d52bd20a073546983985ba3bf55deb
import requests

from bs4 import BeautifulSoup


# remove all attributes
def _remove_all_attrs(soup):
    for tag in soup.find_all(True):
        tag.attrs = {}
    return soup


# remove all attributes except some tags
def _remove_all_attrs_except(soup):
    whitelist = ["a"]
    for tag in soup.find_all(True):
        if tag.name not in whitelist:
            tag.attrs = {}
    return soup


# remove all attributes except some tags(only saving ['href','src'] attr)
def _remove_all_attrs_except_saving(soup):
    whitelist = ["a", "br"]
    for tag in soup.find_all(True):
        if tag.name not in whitelist:
            tag.attrs = {}
        else:
            attrs = dict(tag.attrs)
            for attr in attrs:
                if attr not in ["target", "href"]:
                    del tag.attrs[attr]
    return soup


def _unwrap_all_except(soup, whitelist):
    for tag in soup.find_all(True):
        if tag.name not in whitelist:
            tag.unwrap()
    return str(soup)


def _extract_metadata_url(soup, base_url):
    url = None
    for tag in soup.find_all("a"):
        if "href" in tag.attrs and base_url in tag["href"]:
            url = tag["href"]
    return url


def _fetch_update_frequency(full_metadata_url):
    update_frequency = "unknown"
    try:
        response = requests.get(full_metadata_url)
        soup = BeautifulSoup(response.content, "html.parser")

        for tag in soup(
            "script", attrs={"id": "tpx_ExternalView_Frequency_of_Updates"}
        ):
            tag_text = tag.get_text()
            if "deemed" in tag_text:
                update_frequency = "asNeeded"
                break
            elif "week " in tag_text:
                update_frequency = "weekly"
                break
            elif "twice" in tag_text:
                update_frequency = "biannually"
                break
            elif "year" in tag_text:
                update_frequency = "annually"
                break
            elif "month" in tag_text:
                update_frequency = "monthly"
                break
            elif "quarter" in tag_text:
                update_frequency = "quarterly"
                break
    except Exception as e:
        print("An error occured: %s" % str(e))

    return update_frequency
