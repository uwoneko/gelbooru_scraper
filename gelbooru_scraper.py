from concurrent.futures import ThreadPoolExecutor
import io
import os
import re
import signal
import glob
import asyncio
import sys
import time
import requests
from requests_html import HTMLSession
import argparse
import threading
import urllib3
from PIL import Image

# ===

async def main():
    log('Starting...', 'MAIN', Color.CYAN)
    page_number = 0
    global total_queued, executor, console_lock
    total = 0
    while True:
        try:
            while args.max_queue != -1 and total_queued > args.max_queue:
                await asyncio.sleep(0.1)
            request_url = f'{args.site}/index.php?page=post&s=list&tags={args.query}&pid={page_number}'
            with console_lock:
                log(f'Fetching from "{request_url}"', 'MAIN', Color.CYAN)
            webpage_request = session.get(request_url)
            request_html = webpage_request.html
            thumbnails_div = request_html.find('.thumbnail-container', first=True)
            if len(thumbnails_div.find('a')) == 0:
                break
            queued = 0
            image_urls = [anchor.attrs['href'] for anchor in thumbnails_div.find('a')]
            for image_url in image_urls:
                if args.image_count != -1 and args.image_count < total:
                    break
                total += 1
                image_id = get_image_id(image_url)
                if image_exists(args.directory_name, image_id):
                    continue
                total_queued += 1
                queued += 1
                executor.submit(download_image, image_url)
            if args.image_count != -1 and args.image_count < total:
                break
            with console_lock:
                log(f'Added {queued} downloads to queue.', 'MAIN', Color.BLUE)
            with console_lock:
                log(f'{total_queued} downloads pending.', 'MAIN', Color.BLUE)
            page_number += len(thumbnails_div.find('a'))
        except Exception as e:
            with console_lock:
                log(f'An error occurred: {e}', 'MAIN', error=True)
            await asyncio.sleep(5)
            continue

def download_image(image_url):
    global total_queued, executor, console_lock
    def requeue():
        global total_queued
        time.sleep(3)
        total_queued += 1
        executor.submit(download_image, image_url)
    total_queued -= 1
    try:
        image_id = get_image_id(image_url)
        if image_exists(args.directory_name, image_id):
            return
        with console_lock:
            log(f'Starting download...', image_id, Color.MAGENTA)

        post_request = session.get(image_url)
        post_html = post_request.html
        image_container = post_html.find('.image-container', first=True)
        tags = image_container.attrs['data-tags'].strip().split(' ')
        tags.insert(0, image_container.attrs['data-rating'])
        comma_separated_tags = ', '.join(tags)
        if args.tags:
            write_tags_to_file(args.directory_name, image_id, comma_separated_tags)

        for hyperlink in post_html.find('a'):
            if hyperlink.text == 'Original image':
                image_request = requests.get(hyperlink.attrs['href'])
                image_data = image_request.content
                expected_legnth = int(image_request.headers["Content-Length"])
                if abs(len(image_data) - expected_legnth) > expected_legnth/5:
                    with console_lock:
                        log(f'Expected {expected_legnth} bytes, got {len(image_data)}.', image_id, error=True)
                    requeue()
                    return
                if not args.soft_length_validation and len(image_data) < expected_legnth:
                    with console_lock:
                        log(f'Expected {expected_legnth} bytes, got {len(image_data)}.', image_id, error=True)
                    requeue()
                    return
                if args.validation and not validate_image(image_data):
                    with console_lock:
                        log(f'Validation failed.', image_id, error=True)
                    requeue()
                    return
                image_extension = hyperlink.attrs['href'].split('.')[-1]
                save_image(args.directory_name, image_id, image_data, image_extension)
                with console_lock:
                    log(f'Download complete.', image_id, Color.GREEN)
                return
    except requests.exceptions.RequestException:
        with console_lock:
            log(f'Got RequestException.', image_id, error=True)
        requeue()
    except urllib3.connectionpool.MaxRetryError:
        with console_lock:
            log(f'Got MaxRetryError.', image_id, error=True)
        requeue()
    except Exception as e:
        with console_lock:
            log(f'{type(e).__name__} occurred with image: {e}', image_id, error=True)

# ===

def parse_args():
    parser = argparse.ArgumentParser(description='Scrapes images from gelbooru.')
    parser.add_argument('-o', '--output', dest='directory_name', type=str, default='outputs',
                        help='Output directory, defaults to "outputs"', required=False)
    parser.add_argument('-s', '--site', dest='site', type=str, default='https://gelbooru.com/',
                        help='Domain, defaults to "https://gelbooru.com/"', required=False)
    parser.add_argument('-t', '--threads', dest='threads', type=int, default=16,
                        help='Max thread count, defaults to 16', required=False)
    parser.add_argument('-c', '--count', dest='image_count', type=int, default=-1,
                        help='Max image count (-1 for infinity), defaults to -1', required=False)
    parser.add_argument('--max-queue', dest='max_queue', type=int, default=500,
                        help='Max queue length (-1 for infinity), defaults to 500', required=False)
    parser.add_argument('-q', '--query', dest='query', type=str, default='sort:score:desc -video -real_life -animated -3d',
                        help='Search query, defaults to "sort:score:desc -video -real_life -animated -3d"', required=False)
    parser.add_argument('--enable-tags', dest='tags', action='store_true',
                        help='Enables tag saving')
    parser.add_argument('--soft-length-check', dest='soft_length_validation', action='store_true',
                        help='Makes the Content-Length validation (Content-Length=actual length) pass if the diffrence between Content-Length and actual length is less than 1/5 of Content-Length')
    parser.add_argument('--validate-images', dest='validation', action='store_true',
                        help='Enables validation of images by calling .verify() and .transpose(Image.Transpose.FLIP_TOP_BOTTOM), does not always work')
    parser.add_argument('--tag-file', dest='tag_file', type=str, default="tags.psv",
                        help='File to save tags to, defaults to "tags.psv"', required=False)
    args = parser.parse_args()
    args.site = args.site.strip('/')
    return args

# ===

class Color:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'

def log(text, thread, color = None, error=False):
    if error:
        print(f'{Color.YELLOW}[{thread}] {Color.RED}{text}', file=sys.stderr)
    else:
        print(f'{Color.YELLOW}[{thread}] {color}{text}')

# ===

def get_image_id(image_url):
    return re.search('id=(\d+)', image_url).group(1)

def image_exists(directory_name, image_id):
    return glob.glob(f'{directory_name}/{image_id}.*')

def write_tags_to_file(directory_name, image_id, tags):
    with tag_file_lock:
        with open(os.path.join(directory_name, args.tag_file), 'a') as tags_file:
            tags_file.write(f'{image_id}|{tags}\n')

def save_image(directory_name, image_id, image_data, image_extension):
    with open(f'{directory_name}/{image_id}.{image_extension}', 'wb') as file:
        file.write(image_data)

def validate_image(image):
    try:
        im = Image.open(io.BytesIO(image))
        im.verify()
        im.close()
        im = Image.open(io.BytesIO(image))
        im.transpose(Image.Transpose.FLIP_TOP_BOTTOM)
        im.verify()
        im.close()
        return True
    except:
        return False
    
def ctrl_c_handler(sig, frame):
    os._exit(-1)

# ===

if __name__ == '__main__':
    os.system('color')

    console_lock = threading.Lock()
    tag_file_lock = threading.Lock()

    session = HTMLSession()
    session.cookies.set('fringeBenefits', 'yup')
    
    signal.signal(signal.SIGINT, ctrl_c_handler)
    
    args = parse_args()
    
    total_queued = 0
    executor = ThreadPoolExecutor(max_workers=args.threads)
    
    if not os.path.isdir(args.directory_name):
        os.mkdir(args.directory_name)
    
    asyncio.run(main())
