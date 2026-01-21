import os, re, json, time, platform, multiprocessing
from tqdm import tqdm
from . import parse_xml_dump, parse_source, WikiPage, \
              __version__ as parser_version

SINGLE_FILE_MAXSIZE = 1 << 20
MAX_FILE_IN_DIRECTORY = 512

def parse_and_dump_page(page: WikiPage) -> int:
    parsed = parse_source(page.source)
    parsed["title"] = page.title
    parsed["timestamp"] = page.timestamp

    return page.title, json.dumps(parsed,ensure_ascii=False).encode("utf-8")

def estimate_total_count(xml_path: str) -> int:
    # 快速预估页面总数，不解析xml
    with open(xml_path, "rb") as f:
        total: int = 0
        while True:
            chunk = f.read(1<<20)
            if not chunk: break
            for match in re.findall(rb'<text [^>]*?bytes="(\d*?)"', chunk):
                if int(match) > 0:
                    total += 1
    return total

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

    if os.path.isfile(os.path.join(dest_path, "index.json")):
        with open(os.path.join(dest_path, "index.json"),
                  "r", encoding="utf-8") as f:
            index_data = json.load(f) # 从上一次继续提取
            index = index_data["pages"]
            current_dir_id = current_file_id = current_size = \
                             current_lineno = index_data["_progress"]
        print("Loaded last progress from index.json")
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
                for title, data in pool.imap_unordered(
                    parse_and_dump_page, parse_xml_dump(f), chunksize=8):
                    if title in index: # 上一次已提取过
                        pbar.update(1); continue
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
        with open(os.path.join(dest_path, "index.json"),
                  "w", encoding="utf-8") as f:
            json.dump({
                "pages": index,
                "_progress": [current_dir_id, current_file_id,
                              current_size, current_lineno]
            }, f, ensure_ascii=False)

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

def lookup_from_extracted(path: str, title: str):
    if not os.path.isfile(os.path.join(path, "index.json")):
        raise ValueError("not a path of dump extraction")
    with open(os.path.join(path, "index.json"), "r", encoding="utf-8") as f:
        index_data = json.load(f)
    if title not in index_data["pages"]:
        raise ValueError(f"{title!r} not found")
    dir_id, file_id, lineno = index_data["pages"][title]
    with open(os.path.join(
        path, f"{dir_id:02x}", f"{file_id}.jsonl"), "rb") as f:
        data = f.read().splitlines()[lineno].decode("utf-8")
        return json.loads(data)
