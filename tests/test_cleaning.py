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


def test_normalizes_ligatures():
    """Expands typographic ligatures to ASCII equivalents."""
    raw = "ﬁlesystems and ﬂow with coﬀee and eﬃcient eﬄuent"
    result = clean_text(raw)
    assert "filesystems" in result
    assert "flow" in result
    assert "coffee" in result
    assert "efficient" in result
    assert "effluent" in result
    # Ligatures should be gone
    assert "\ufb00" not in result  # ff
    assert "\ufb01" not in result  # fi
    assert "\ufb02" not in result  # fl
    assert "\ufb03" not in result  # ffi
    assert "\ufb04" not in result  # ffl


def test_normalizes_st_ligatures():
    """Expands st ligatures to ASCII equivalents."""
    raw = "ﬅandard and ﬆyle"
    result = clean_text(raw)
    assert "standard" in result
    assert "style" in result
    assert "\ufb05" not in result  # st
    assert "\ufb06" not in result  # st
