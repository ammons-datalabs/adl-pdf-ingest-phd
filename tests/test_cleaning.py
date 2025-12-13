from pdf_ingest.cleaning import clean_text


def test_preserves_normal_sentences():
    raw = "This is a normal sentence.\nAnd another one."
    result = clean_text(raw)
    assert "This is a normal sentence." in result
    assert "And another one." in result


def test_removes_page_number_lines():
    raw = "Some text here.\n42\nMore text after page number."
    result = clean_text(raw)
    assert "42" not in result.split("\n")
    assert "Some text here." in result
    assert "More text after page number." in result


def test_collapses_whitespace():
    raw = "Too   many    spaces   here."
    result = clean_text(raw)
    assert result == "Too many spaces here."


def test_collapses_multiple_blank_lines():
    raw = "Paragraph one.\n\n\n\n\nParagraph two."
    result = clean_text(raw)
    # Should have at most 2 newlines between paragraphs
    assert "\n\n\n" not in result
    assert "Paragraph one." in result
    assert "Paragraph two." in result


def test_normalizes_line_endings():
    raw = "Windows\r\nline endings\rand old mac\r\n"
    result = clean_text(raw)
    assert "\r" not in result
    assert "Windows" in result
    assert "line endings" in result
