import os
import requests
import logging
import urllib3


# --- NEW: suppress insecure request warnings ---
# when we disable SSL verification, requests will print a warning for every
# single file. This line disables those specific warnings to keep our logs clean
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def download_file(url, target_dir):
    """
    Downloads a single file from a URL into a target directory.
    Skips the download if the file already exists.

    Now includes SSL verification disabling for certain academic servers.
    """
    local_filename = os.path.join(target_dir, url.split('/')[-1])

    if os.path.exists(local_filename):
        logging.info(f"File already exists, skipping: {local_filename}")
        return local_filename

    logging.info(f"Downloading: {url}")
    
    try:
        # 'verify=False' argument tells requests to proceed with the
        # download even if the SSL certificate verification fails
        with requests.get(url, stream=True, timeout=60, verify=False) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logging.info(f"Successfully downloaded to: {local_filename}")
        return local_filename
    except requests.RequestException as e:
        logging.error(f"Failed to download {url}. Reason: {e}")
        # cleaning up partially downloaded file
        if os.path.exists(local_filename):
            os.remove(local_filename)
        return None