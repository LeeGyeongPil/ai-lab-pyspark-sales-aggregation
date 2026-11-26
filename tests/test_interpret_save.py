from sales_agg.interpret import save_sentences

ALL = [f"month={m}" for m in range(1, 13)]


def test_small_data_says_larger_en():
    text = save_sentences("en", True, 220_000, 420_000, ALL)[1]
    assert "larger" in text and "smaller" not in text and "1.9x" in text


def test_small_data_says_larger_ko():
    text = save_sentences("ko", True, 220_000, 420_000, ALL)[1]
    assert "큽니다" in text and "작고" not in text


def test_big_data_keeps_smaller():
    assert "2.0x smaller" in save_sentences("en", True, 400, 200, ALL)[1]
    assert "2.0배 작고" in save_sentences("ko", True, 400, 200, ALL)[1]


def test_zero_parquet_bytes_no_crash():
    assert len(save_sentences("en", False, 100, 0, ALL)) == 2
    assert len(save_sentences("en", False, 0, 0, [])) == 2


def test_twelve_folders_keep_11_and_12():
    en = save_sentences("en", True, 220_000, 420_000, ALL)
    assert "skips the other 11" in en[0] and "12 month folders" in en[1]
    ko = save_sentences("ko", True, 220_000, 420_000, ALL)
    assert "나머지 11개" in ko[0] and "월 폴더 12개" in ko[1]


def test_single_folder_without_month3_en():
    en = save_sentences("en", True, 220_000, 420_000, ["month=7"])
    assert "no month=3 folder" in en[0].lower() and "month=7" in en[0]
    assert "opens only" not in en[0] and "other 11" not in en[0]
    assert "1 month folder " in en[1] and "1 month folders" not in en[1] and "only month=7 exists" in en[0] and "12" not in en[1]


def test_single_folder_without_month3_ko():
    ko = save_sentences("ko", True, 220_000, 420_000, ["month=7"])
    assert "month=3 폴더가 없어" in ko[0] and "month=7" in ko[0]
    assert "11" not in ko[0] and "월 폴더 1개" in ko[1] and "12" not in ko[1]


def test_month3_present_in_partial_data():
    en = save_sentences("en", True, 220_000, 420_000, ["month=3", "month=7"])
    assert "skips the other 1 folder " in en[0] and "2 month folders" in en[1]
