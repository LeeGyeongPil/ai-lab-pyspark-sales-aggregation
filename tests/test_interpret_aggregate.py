from sales_agg.interpret import aggregate_sentences

MULTI = [("S1", 1, 100), ("S1", 2, 50), ("S2", 1, 30), ("S2", 2, 20)]


def test_single_store_and_month_en():
    text = " ".join(aggregate_sentences("en", [("S1", 7, 5800)]))
    assert "the least" not in text and "weakest" not in text and "year-end" not in text
    assert "only one store" in text and "S1" in text and "5,800" in text
    assert "only one month" in text and "month 7" in text


def test_single_store_and_month_ko():
    text = " ".join(aggregate_sentences("ko", [("S1", 7, 5800)]))
    assert "가장 적게" not in text and "2월 부진" not in text
    assert "하나뿐" in text and "5,800" in text


def test_single_store_many_months_keeps_season():
    text = aggregate_sentences("en", [("S1", 1, 10), ("S1", 2, 5)])
    assert "only one store" in text[0]
    assert "strongest" in text[1]


def test_many_stores_single_month_keeps_stores():
    text = aggregate_sentences("en", [("S1", 3, 10), ("S2", 3, 5)])
    assert "sold the most" in text[0]
    assert "only one month" in text[1]


def test_multiple_unchanged():
    stores, season = aggregate_sentences("en", MULTI)
    assert "Store S1 sold the most (150, 75.0% of all sales) and S2 the least (50)" in stores
    assert "Month 1 is the strongest (130) and month 2 the weakest (70)" in season
    assert "year-end growth" in season
