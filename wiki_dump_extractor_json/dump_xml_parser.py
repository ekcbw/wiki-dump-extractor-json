import warnings
from typing import Iterator, TextIO
from dataclasses import dataclass
from dateutil.parser import parse as parse_date
import xml.etree.ElementTree as ET

@dataclass(slots=True)
class WikiPage:
    title: str | None
    timestamp: float | None
    source: str | None

def parse_xml_dump(stream: TextIO) -> Iterator[WikiPage]:
    current_page = current_title = current_timestamp = None
    current_format = current_source = None
    root_elem = ET.Element('dummy')
    in_page = in_revision = found_mediawiki = False

    context = ET.iterparse(stream, events=('start', 'end'))

    for event, elem in context:
        if not found_mediawiki and event == 'start':
            if elem.tag.endswith('mediawiki'):
                found_mediawiki = True; root_elem = elem
            else:
                raise ValueError("root element should be <mediawiki>")

        try:
            if event == 'start' and elem.tag.endswith('page'):
                in_page = True
                current_page = elem
                current_title = current_timestamp = None
                current_format = current_source = None
            elif event == 'end' and elem.tag.endswith('page') and in_page:
                if current_format == 'text/x-wiki' and current_source is not None:
                    yield WikiPage(
                        title=current_title,
                        timestamp=current_timestamp,
                        source=current_source
                    )
                in_page = False
                current_page.clear()
                root_elem.clear()
            elif in_page and event == 'start' and elem.tag.endswith('revision'):
                in_revision = True
            elif in_page and event == 'end' and elem.tag.endswith('revision'):
                in_revision = False
            elif in_page and event == 'end':
                if elem.tag.endswith('title'):
                    current_title = elem.text
                elif in_revision:
                    if elem.tag.endswith('timestamp'):
                        dt = parse_date(elem.text)
                        current_timestamp = dt.timestamp()
                    elif elem.tag.endswith('format'):
                        current_format = elem.text
                    elif elem.tag.endswith('text'):
                        current_source = elem.text
        except Exception as err:
            warnings.warn(
                f"Failed to parse {current_title!r}: {err} ({type(err).__name__})")
