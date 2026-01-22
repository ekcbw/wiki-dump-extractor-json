import os, re, json, time, hashlib, platform, multiprocessing
from tqdm import tqdm
from . import parse_xml_dump, parse_source, WikiPage, \
              __version__ as parser_version

SINGLE_FILE_MAXSIZE = 1 << 20
MAX_FILE_IN_DIRECTORY = 512
INDEX_DIR = "index"
INDEX_DIR_LENGTH = 2

def parse_and_dump_page(page: WikiPage) -> int:
    parsed = parse_source(page.source)
    parsed["title"] = page.title
    parsed["timestamp"] = page.timestamp

    return page.title, json.dumps(parsed,ensure_ascii=False).encode("utf-8")

def estimate_total_count(xml_path: str) -> int:
    # 快速预估页面总数，不解析xml
    with open(xml_path, "rb") as f:
        total: int = 0
        while chunk := f.read(1<<20):
            for match in re.findall(rb'<text [^>]*?bytes="(\d*?)"', chunk):
                if int(match) > 0:
                    total += 1
    return total

def write_index(dest_path: str, index: dict[str], extended_config = None,
                dir_length = INDEX_DIR_LENGTH):
    index_dir = os.path.join(dest_path, INDEX_DIR)
    os.makedirs(index_dir, exist_ok=True)
    shards: list[dict[str]] = [{} for _ in range(1 << (dir_length * 4))]
    for title, data in index.items():
        digest = hashlib.sha256(title.encode("utf-8")).hexdigest()
        idx = int(digest[:dir_length], base=16)
        shards[idx][title] = data
    for idx in range(len(shards)):
        with open(os.path.join(index_dir, f"{idx:02x}.json"),
                  "w", encoding="utf-8") as f:
            json.dump(shards[idx], f, ensure_ascii=False)

    with open(os.path.join(dest_path, "index.json"),
              "w", encoding="utf-8") as f:
        json.dump({
            "index_path": INDEX_DIR,
            "dir_length": dir_length,
            **extended_config
        }, f, ensure_ascii=False)

def load_all_indexes(dest_path: str):
    with open(os.path.join(dest_path, "index.json"),
              "r", encoding="utf-8") as f:
        index_data = json.load(f)
    index_path, dir_length = index_data["index_path"], \
                             index_data["dir_length"]
    index_dir = os.path.join(dest_path, index_path)
    index = {}
    for idx in range(1 << (dir_length * 4)):
        with open(os.path.join(index_dir, f"{idx:02x}.json"),
            "r", encoding="utf-8") as f:
            index.update(json.load(f))
    return index

def extract_wiki_dump(xml_dump_path: str, dest_path: str,
                      num_workers: int | None = None,
                      single_file_maxsize: int = SINGLE_FILE_MAXSIZE,
                      max_file_in_directory: int = MAX_FILE_IN_DIRECTORY):
    if num_workers is None:
        num_workers = os.cpu_count()
    print(f"Using {num_workers} parallel workers")
    print("Estimating total page count ...", end=" ", flush=True)
    total_pages = estimate_total_count(xml_dump_path)
    print("done")

    if os.path.isfile(os.path.join(dest_path, "index.json")): # 从上一次继续提取
        with open(os.path.join(dest_path, "index.json"),
                  "r", encoding="utf-8") as f:
            index_data = json.load(f)
        current_dir_id, current_file_id, current_size, \
            current_lineno = index_data["_progress"]
        index = load_all_indexes(dest_path)
        print(f"Resuming from last progress ({len(index)} pages)")
    else:
        index = {}
        current_size = current_lineno = 0
        current_dir_id = current_file_id = 0

    os.makedirs(os.path.join(dest_path, f"{current_dir_id:02x}"), exist_ok=True)
    current_file = open(os.path.join(
        dest_path, f"{current_dir_id:02x}", f"{current_file_id}.jsonl"), "ab")
    try:
        with open(xml_dump_path, 'rb') as f: # 'rb'速度略快
            with multiprocessing.Pool(processes=num_workers) as pool:
                pbar = tqdm(
                    total=total_pages,
                    unit="page",
                    miniters=1024
                )
                def _iterator():
                    for page in parse_xml_dump(f):
                        if page.title in index:
                            pbar.update(1); continue
                        yield page

                for title, data in pool.imap_unordered(
                    parse_and_dump_page, _iterator(), chunksize=16):
                    if current_size + len(data) > single_file_maxsize:
                        current_size = current_lineno = 0
                        current_file_id += 1
                        if current_file_id >= max_file_in_directory:
                            current_file_id = 0; current_dir_id += 1
                            os.makedirs(os.path.join(dest_path, f"{current_dir_id:02x}"),
                                        exist_ok=True)
                        current_file.close()
                        current_file = open(os.path.join(dest_path,
                            f"{current_dir_id:02x}", f"{current_file_id}.jsonl"),"wb")

                    current_file.write(data)
                    current_file.write(b"\n")
                    index[title] = [current_dir_id, current_file_id, current_lineno]
                    current_size += len(data)
                    current_lineno += 1

                    pbar.update(1)
    finally:
        if current_file is not None and not current_file.closed:
            current_file.close()
        print("\nWriting index ...", end=" ", flush=True)
        write_index(dest_path, index,
                   {"_progress":[current_dir_id, current_file_id,
                                 current_size, current_lineno]})
        print("done")

def benchmark(xml_dump_path: str, max_seconds: float = 10.0):
    print(f"""Environment: {platform.python_implementation()} \
{platform.python_version()} (wiki_parser {parser_version})""")
    print(f"Running {max_seconds:.1f}s benchmark on {xml_dump_path} ...")

    total_size = page_cnt = 0
    total_time = 0.0; begin_time = time.perf_counter()
    with open(xml_dump_path, 'rb') as f:
        for page in parse_xml_dump(f):
            if page.source is None:
                continue
            total_size += len(page.source)
            page_cnt += 1
            start  = time.perf_counter()
            parse_source(page.source)
            current_time = time.perf_counter()
            total_time += current_time - start
            if current_time - begin_time >= max_seconds:
                break

    parse_xml_time = current_time - begin_time - total_time
    print(f"""Benchmark of parse_source(): \
Processed {total_size} chars in {total_time:.2f} seconds \
({total_size/total_time/1e6:.2f} Mchar/s)""")
    print(f"""Benchmark of parse_xml_dump(): \
Page count: {page_cnt} ({page_cnt/parse_xml_time:.2f} it/s)""")

def find_page_from_file(path, dir_id: int, file_id: int, lineno: int):
    with open(os.path.join(
        path, f"{dir_id:02x}", f"{file_id}.jsonl"), "rb") as f:
        json_data = f.read().splitlines()[lineno].decode("utf-8")
    return json.loads(json_data)

def lookup_from_extracted(path: str, title: str, follow_redirection=True) -> dict:
    if not os.path.isfile(os.path.join(path, "index.json")):
        raise ValueError("not a path of dump extraction")
    with open(os.path.join(path, "index.json"), "r", encoding="utf-8") as f:
        index = json.load(f)

    index_path, dir_length = index["index_path"], index["dir_length"]
    digest = hashlib.sha256(title.encode("utf-8")).hexdigest()
    with open(os.path.join(path, index_path, f"{digest[:dir_length]}.json"),
              "r", encoding="utf-8") as f:
        index_data = json.load(f)

    if title not in index_data:
        raise ValueError(f"Title {title!r} not found")
    page = find_page_from_file(path, *index_data[title])
    if follow_redirection and page["redirectedTo"] is not None:
        try:
            return lookup_from_extracted(path, page["redirectedTo"], True)
        except RecursionError:
            raise RecursionError(
                f"circular redirection from {title!r} detected") from None
    return page
