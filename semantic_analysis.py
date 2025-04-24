import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pickle
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import Normalizer
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import re
import os

from config_loader import load_confluence_settings

# Constants
OUTPUT_PICKLE = "confluence_semantic_data.pkl"
ORIGINAL_PICKLE = "confluence_data.pkl"
N_COMPONENTS = 50  # Dimensions for LSA


def extract_text_from_space(space_key, api_base_url, auth, verify_ssl=False):
    """Fast fetch of page content from a space"""
    print(f"Starting extraction for space: {space_key}")
    pages_url = f"{api_base_url}/content?spaceKey={space_key}&expand=body.storage&limit=100"
    response = requests.get(pages_url, auth=auth, verify=verify_ssl)

    if response.status_code != 200:
        print(f"Failed to fetch space {space_key}: HTTP {response.status_code}")
        return ""

    # Extract text from all pages
    all_text = []
    for page in response.json().get('results', []):
        if 'body' in page and 'storage' in page['body'] and 'value' in page['body']['storage']:
            html_content = page['body']['storage']['value']
            soup = BeautifulSoup(html_content, 'html.parser')
            text = soup.get_text(separator=' ', strip=True)
            text = re.sub(r'\s+', ' ', text)
            all_text.append(text)

    print(f"Completed extraction for space: {space_key}")
    return " ".join(all_text)


def process_spaces_parallel(spaces, api_base_url, auth, verify_ssl=False):
    """Process all spaces in parallel for faster computation"""
    space_texts = {}

    # Exclude user spaces
    spaces = [space for space in spaces if not space.get("key", "").startswith("~")]

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_space = {
            executor.submit(extract_text_from_space, space["key"], api_base_url, auth, verify_ssl): space["key"]
            for space in spaces
        }
        for future in concurrent.futures.as_completed(future_to_space):
            space_key = future_to_space[future]
            try:
                text = future.result()
                if text:
                    space_texts[space_key] = text
                print(f"Successfully processed space: {space_key}")
            except Exception as e:
                print(f"Error processing space {space_key}: {e}")

    return space_texts


def compute_semantic_vectors(space_texts):
    """Convert text to semantic vectors using TF-IDF and LSA"""
    if not space_texts:
        return {}, None

    # Keys and texts for vectorization
    keys = list(space_texts.keys())
    texts = [space_texts[k] for k in keys]

    # Create pipeline for tf-idf + dimensionality reduction
    vectorizer = TfidfVectorizer(max_features=10000, stop_words='english')
    svd = TruncatedSVD(n_components=N_COMPONENTS)
    normalizer = Normalizer(copy=False)
    lsa_pipeline = make_pipeline(vectorizer, svd, normalizer)

    # Transform documents to LSA space
    lsa_vectors = lsa_pipeline.fit_transform(texts)

    # Create mapping from space key to vector
    vector_map = {keys[i]: lsa_vectors[i] for i in range(len(keys))}

    return vector_map, lsa_pipeline


def main():
    settings = load_confluence_settings()

    api_base_url = settings['api_base_url']
    auth = (settings['username'], settings['password'])
    verify_ssl = settings['verify_ssl']

    # Load original data
    if not os.path.exists(ORIGINAL_PICKLE):
        print(f"Error: Original data file {ORIGINAL_PICKLE} not found")
        return

    with open(ORIGINAL_PICKLE, "rb") as f:
        original_data = pickle.load(f)

    # Extract spaces from hierarchy
    spaces = []

    def extract_spaces(node):
        if 'key' in node and 'avg' in node:
            spaces.append(node)
        if 'children' in node:
            for child in node['children']:
                extract_spaces(child)

    extract_spaces(original_data)


    # Process spaces to get text content
    print("Extracting text from spaces...")
    space_texts = process_spaces_parallel(spaces, api_base_url, auth, verify_ssl)

    # Compute semantic vectors
    print("Computing semantic vectors...")
    vector_map, lsa_pipeline = compute_semantic_vectors(space_texts)

    # Update original data with semantic vectors
    def add_vectors_to_data(node):
        if 'key' in node and node['key'] in vector_map:
            node['semantic_vector'] = vector_map[node['key']].tolist()
        if 'children' in node:
            for child in node['children']:
                add_vectors_to_data(child)

    add_vectors_to_data(original_data)

    # Save enhanced data
    print(f"Saving semantic data to {OUTPUT_PICKLE}...")
    with open(OUTPUT_PICKLE, "wb") as f:
        pickle.dump({
            'data': original_data,
            'vector_map': {k: v.tolist() for k, v in vector_map.items()},
            'has_semantic': True
        }, f)

    print("Semantic analysis completed successfully")


if __name__ == "__main__":
    main()