import random
import re
import requests as rq
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from fake_useragent import UserAgent
from pathlib import Path
from requests.exceptions import RequestException, Timeout, HTTPError
from time import sleep
from tqdm import tqdm

status_code = defaultdict(lambda: "Not define in table")
status_code[202] = "202: Accepted"
status_code[204] = "204: No Content"
status_code[400] = "400: Bad Request"
status_code[401] = "401: Unauthorized"
status_code[403] = "403: Forbidden"
status_code[404] = "404: Not Found"
status_code[409] = "409: Conflict"
status_code[429] = "429: Too Many Requests"


# https://data.csail.mit.edu/graphics/fivek/img/tiff16_<a b c d e>/*.tif


class FivekCrawler:
    """Crawl the fivek experts.

    Arguments
    ---------
    expert_list : list
        a list contain available experts id, ["a", "b", "c", "d", "e"]
    max_workers : int
        how many images downloaded at the same time.
    saving_dir : str
        the directory path for saving the edited photo,
        default is under current directory.
    num_images : int
        how many images to download for each expert. The maximum value is
        5000, which is also the default.
    """

    def __init__(
        self,
        expert_list,
        max_workers,
        saving_dir=None,
        image_from: int = 0,
        image_to: int = 5000,
    ):
        self.expert_list = expert_list
        self.max_workers = max_workers
        self.saving_dir = saving_dir
        self.image_from = image_from
        self.image_to = image_to
        self.incomplete_images = []
        self.fivek_src = "https://data.csail.mit.edu/graphics/fivek"

        assert image_from < image_to, "image_to must larger than image_from."
        assert 0 <= image_from <= 5000, "image_from is out of range."
        assert 0 <= image_to <= 5000, "image_from is out of range."

        if saving_dir is None:
            self.saving_dir = Path(__file__).parent.resolve()

    def _create_expert_folder(self, expert_id, saving_dir=None):
        """Create the folder for saving images edited by each expert.

        Arguments
        ---------
        expert_id : str
            an expert id in [a, b, c, d, e]
        saving_dir : str
            the directory path for saving

        Returns
        -------
        folder_dir : Path
        """

        folder_dir = Path(f"{saving_dir}/fivek_expert/tiff16_{expert_id}")
        try:
            folder_dir.mkdir(parents=True)
        except FileExistsError:
            print(f"target directory ({folder_dir}) already exists.")

        return folder_dir

    def _choose_header(self):
        """Retrun a random User-Agent."""
        return {"User-Agent": UserAgent().random}

    def _make_request(self, header, timeout):
        """Send a request to ask for the web html.

        Arguments
        ---------
        header : dict
            User-Agent
        timeout : int
            if not reply within the specified time, it will raise the exception.

        Returns
        -------
        html : str
            web html
        """
        try:
            sleep(random.random() + 0.2)
            web_rq = rq.get(url=self.fivek_src, headers=header, timeout=timeout)
        except RequestException as err:
            print(err.__class__.__name__)  # debug
        except Exception as err:
            print(err.__class__.__name__)  # debug
        else:
            if web_rq.status_code == 200:
                print("request successed")
                return web_rq.text
            else:
                print(f"Status_code: {status_code[web_rq.status_code]}")  # debug
                exit()

    def _get_expert_images_url(self, web_html):
        """Select the images URL for the specific experts

        Arguments
        ---------
        web_html : str
            the html of fivek dataset website.
        """
        # https://stackoverflow.com/a/71370571
        expert_list = "".join(self.expert_list)
        urls = re.finditer(
            rf"img/tiff16_[{expert_list}]/\S*.tif",
            web_html,
        )
        for url in urls:
            url_index = int(url.group().split("/")[-1][1:5])
            if self.image_from <= url_index <= self.image_to:
                yield url.group()

    def download_image(self, url, image_path):
        """Download the image.

        Arguments
        ---------
        url : str
            the link to the image witch format is img/tiff16_{expert_id}/*.tif
        image_path : str
            the current saving path for an image.
        """

        retry = 0
        while retry < 3:
            try:
                with rq.get(
                    url=f"{self.fivek_src}/{url}",
                    headers=self._choose_header(),
                    stream=True,
                    timeout=5,
                ) as r:
                    with open(image_path, "wb") as f:
                        for data in r.iter_content(1024):
                            f.write(data)
                sleep(0.7)
                break
            except Timeout:
                retry += 1
                if retry == 3:
                    print(f"\r{Path(url).name:>7s} : [Timeout] Retry({retry})")  # debug
                    self.incomplete_images.append([url, image_path])
                else:
                    print(
                        f"\r{Path(url).name:>7s} : [Timeout] Retry({retry})", end=""
                    )  # debug
            except (HTTPError, RequestException) as err:
                print(f"{Path(url).name:>7s} : {err}")  # debug
                self.incomplete_images.append([url, image_path])
                break

    def main(self):
        urls = self._get_expert_images_url(
            self._make_request(self._choose_header(), timeout=5)
        )

        for i in self.expert_list:
            folder_dir = self._create_expert_folder(i, self.saving_dir)

        total_images = (self.image_to - self.image_from) * len(self.expert_list)

        with tqdm(total=total_images) as pbar:
            with ThreadPoolExecutor(max_workers=self.max_workers) as saver:
                futures = []
                for i, url in enumerate(urls, start=1):
                    image_path = url.replace("img", str(folder_dir.parent))
                    future = saver.submit(self.download_image, url.group(), image_path)
                    futures.append(future)
                    if i % total_images == 0:
                        break
                for future in as_completed(futures):
                    pbar.update(1)

        remains = self.incomplete_images
        self.incomplete_images = []
        while remains:
            with tqdm(total=int(len(remains))) as pbar:
                with ThreadPoolExecutor(max_workers=self.max_workers) as saver:
                    futures = []
                    for i_url, i_path in remains:
                        saver.submit(self.download_image, i_url, i_path)
                        futures.append(future)
                    for future in as_completed(futures):
                        pbar.update(1)
            remains = self.incomplete_images


if __name__ == "__main__":
    crawler = FivekCrawler(
        expert_list=["a", "b", "c", "d", "e"],
        max_workers=5,
        image_from=289,
        image_to=300,
    )

    try:
        crawler.main()
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
