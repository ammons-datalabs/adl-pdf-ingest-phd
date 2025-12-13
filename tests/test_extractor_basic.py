from pathlib import Path

from pdf_ingest.extractor import extract_text, ExtractionError


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "pdfs"


def test_extract_text_from_sample_pdf():
    """Extract text from a synthetic test PDF."""
    pdfs = sorted(FIXTURES_DIR.glob("*.pdf"))
    assert pdfs, f"no PDFs found in {FIXTURES_DIR}"

    sample = pdfs[0]

    text = extract_text(sample)

    assert isinstance(text, str)
    assert text.strip(), "extracted text is empty"
