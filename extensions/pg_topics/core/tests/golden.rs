use std::fs;

#[test]
fn every_row_matches_the_kafka_reference() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/murmur2_golden.tsv");
    let text = fs::read_to_string(path).expect("read core/testdata/murmur2_golden.tsv");
    let mut rows = 0;

    for (line_no, line) in text.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let mut cols = line.split('\t');
        let key = cols
            .next()
            .unwrap_or_else(|| panic!("line {}: missing key column", line_no + 1));
        let band_count: u32 = cols
            .next()
            .unwrap_or_else(|| panic!("line {}: missing band_count column", line_no + 1))
            .parse()
            .unwrap_or_else(|e| panic!("line {}: bad band_count: {e}", line_no + 1));
        let expected: u32 = cols
            .next()
            .unwrap_or_else(|| panic!("line {}: missing band column", line_no + 1))
            .parse()
            .unwrap_or_else(|e| panic!("line {}: bad band: {e}", line_no + 1));

        let actual = pgt::murmur2::band_for(key.as_bytes(), band_count);
        assert_eq!(
            actual,
            expected,
            "line {}: key={key:?} band_count={band_count}",
            line_no + 1
        );
        rows += 1;
    }

    assert!(
        rows >= 200,
        "golden file has only {rows} rows, expected at least 200"
    );
}
