import argparse, json
from .extractor import benchmark, extract_wiki_dump, lookup_from_extracted

def main(prog_name='python -m wiki_dump_extractor_json'):
    parser = argparse.ArgumentParser(prog=prog_name, add_help=False)
    parser.add_argument("path", type=str, nargs='?')
    parser.add_argument("-h", "--help", action="store_true")
    parser.add_argument("-o", "--output", type=str, default=None)
    parser.add_argument("--lookup", type=str, default=None)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--benchmark", action="store_true")

    args = parser.parse_args()
    if args.help or not args.path:
        parser.print_help()
        print(f"""\nexamples:
  {prog_name} -o output_path pages.xml
  {prog_name} --benchmark pages.xml
  {prog_name} --lookup <title> output_path
""", end="")
        if not args.help:
            print("\nThe following arguments are required: path")
        return

    if args.benchmark:
        benchmark(args.path)
    elif args.lookup:
        print(json.dumps(lookup_from_extracted(args.path, args.lookup),
                         ensure_ascii=False, indent=2))
    else:
        if args.output is None:
            parser.error("either -o option or --lookup is required")

        extract_wiki_dump(args.path, args.output, args.workers)

def main_cli():
    main(prog_name="wiki-dump-extractor-json")

if __name__ == "__main__":main()
