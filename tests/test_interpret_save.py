from sales_agg.interpret import save_sentences


def test_small_data_says_larger_en():
    text = save_sentences("en", True, 220_000, 420_000)[1]
    assert "larger" in text and "smaller" not in text and "1.9x" in text


def test_small_data_says_larger_ko():
    text = save_sentences("ko", True, 220_000, 420_000)[1]
    assert "큽니다" in text and "작고" not in text


def test_big_data_keeps_smaller():
    assert "2.0x smaller" in save_sentences("en", True, 400, 200)[1]
    assert "2.0배 작고" in save_sentences("ko", True, 400, 200)[1]


def test_zero_parquet_bytes_no_crash():
    assert len(save_sentences("en", False, 100, 0)) == 2
    assert len(save_sentences("en", False, 0, 0)) == 2
