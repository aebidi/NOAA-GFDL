import requests
import logging

# official ESGF search API endpoint
ESGF_SEARCH_URL = "https://esgf-node.llnl.gov/esg-search/search/"

def find_download_urls(model, experiment, variable, ensemble_member):
    """
    Searches the ESGF API to find direct download URLs for a given dataset.
    It prioritises standard HTTPServer links but will also find and use
    Globus web server links, which are also downloadable via HTTPS.
    Returns a list of valid URLs.
    """
    search_params = {
        'source_id': model,
        'experiment_id': experiment,
        'variable_id': variable,
        'member_id': ensemble_member,
        'distrib': 'true',
        'format': 'application/solr+json',
        'limit': 50
    }

    logging.info(f"Searching for URLs for: {variable} ({ensemble_member})")
    try:
        response = requests.get(ESGF_SEARCH_URL, params=search_params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data['response']['numFound'] == 0:
            logging.warning(f"No files found via API search for {variable} ({ensemble_member}). It may not be available.")
            return []

        # --- Extract URLs ---
        http_urls = []
        globus_urls = []
        available_services = set()

        for doc in data['response']['docs']:
            for url_entry in doc.get('url', []):
                try:
                    url, mime_type, service_name = url_entry.split('|')
                    available_services.add(service_name)
                    
                    if service_name == 'HTTPServer' and url.endswith('.nc'):
                        http_urls.append(url)
                    # check for globus URLs that are simple HTTPS links
                    elif service_name == 'Globus' and url.startswith('https') and url.endswith('.nc'):
                        globus_urls.append(url)
                except ValueError:
                    logging.warning(f"Could not parse URL entry: {url_entry}")
                    continue

        # --- Prioritise and return the best available URLs ---
        if http_urls:
            logging.info(f"Found {len(http_urls)} HTTPServer URLs for {variable} ({ensemble_member}).")
            return http_urls
        elif globus_urls:
            logging.info(f"Found {len(globus_urls)} Globus HTTPS URLs for {variable} ({ensemble_member}). Using these.")
            return globus_urls
        else:
            logging.warning(f"Found dataset for {variable} ({ensemble_member}), but no compatible download URL was found. Available services: {list(available_services)}")
            return []

    except requests.RequestException as e:
        logging.error(f"API search failed for {variable} ({ensemble_member}). Error: {e}")
        return []