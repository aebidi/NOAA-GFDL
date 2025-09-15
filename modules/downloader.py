import os
import requests
import logging

def download_file(url, target_dir):
    """
    Downloads a single file from a URL into a target directory.
    Skips the download if the file already exists.
    """
    local_filename = os.path.join(target_dir, url.split('/')[-1])

    if os.path.exists(local_filename):
        logging.info(f"File already exists, skipping: {local_filename}")
        return local_filename

    logging.info(f"Downloading: {url}")
    try:
        with requests.get(url, stream=True, timeout=60) as r:
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