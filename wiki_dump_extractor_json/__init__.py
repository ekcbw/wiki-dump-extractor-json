import re
import mwparserfromhell
from mwparserfromhell.wikicode import Wikicode
from .dump_xml_parser import parse_xml_dump, WikiPage

__all__ = ["parse_source", "filter_refs", "parse_xml_dump",
           "WikiPage", "lookup_from_extracted"]
__version__ = "1.0.4"

def strip_comments(source: str) -> str:
    return re.sub("<!--.*?-->", "", source)

def filter_refs(source: str) -> tuple[str, list[str]]:
    ref_pattern = r'(<ref[^>]*?( name="([^"\\]|\\.)*")?[^>]*?>([^<]*?)</ref>)'
    refs = re.findall(ref_pattern, source, re.DOTALL)

    unique_refs: list[str] = []
    seen: set[str] = set()
    name_to_ref: dict[str, str] = {}
    for ref in refs:
        if ref[0] in seen:continue
        unique_refs.append(ref[0])
        seen.add(ref[0])
        if ref[2]:
            name_to_ref[ref[2]] = ref[0]

    ref_to_id: dict[str, int] = {ref: i+1 for i, ref in enumerate(unique_refs)}

    def replace_ref(match):
        if match.group(2):
            ref_text = name_to_ref.get(match.group(2), "unknown")
        else:
            ref_text = match.group(0)
        return f"<ref>{ref_to_id.get(ref_text, 'unknown')}</ref>"

    replaced_content = re.sub(
        r'<ref[^>]*?( name="([^"\\]|\\.)*")?[^>]*?(>([^<]*?)</ref>|/>)',
        replace_ref, source)
    return replaced_content, unique_refs

def parse_redirection(source: str) -> str | None:
    if not source[:9].lower() == "#redirect":
        return None
    match = re.match(r'^#redirect(\s)?\[\[([^\[\]]*?)(\|[^\[\]]*?)?\]\]',
                     source, re.IGNORECASE)
    if not match: return None
    return match.group(2)

def get_entire_text_from_section(section: dict):
    text = '\n'.join(get_entire_text_from_section(sec)
                     for sec in section['subSections'])
    return f"{section['leading']}\n{text}"

def parse_section(wikicode: Wikicode, level: int) -> dict:
    sections = wikicode.get_sections(levels=[level])
    leading = str(wikicode)
    if sections:
        leading = leading[:leading.find(str(sections[0]))]
    return {"title":leading.splitlines()[0].replace("=", "").strip(),
            "leading": leading,
            "subSections": [parse_section(sec, level+1) for sec in sections]}

def get_toc_from_sections(wikinode: dict) -> dict:
    return {"title": wikinode["title"],
            "sub": [get_toc_from_sections(sec) for sec in wikinode["subSections"]]}

def parse_source(source: str) -> dict:
    source = strip_comments(source)
    source, refs = filter_refs(source)
    redirection = parse_redirection(source)

    parsed = mwparserfromhell.parse(source)
    lead, *sections = parsed.get_sections(levels=[2], include_lead=True)
    infobox = None
    for template in lead.ifilter_templates():
        if str(template).startswith("{{Infobox"):
            infobox = str(template)
    leading = str(lead)
    if infobox is not None:
        leading = leading.replace(infobox, "")
    subsections = [parse_section(sec, 3) for sec in sections]
    return {"leading": leading,
            "infobox": infobox,
            "toc": [get_toc_from_sections(sec) for sec in subsections],
            "subSections": subsections,
            "references": refs,
            "redirectedTo": redirection}

from .extractor import lookup_from_extracted, \
    load_all_indexes, find_page_from_file # pylint: disable=R0401
