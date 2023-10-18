use super::*;

#[tokio::test]
async fn select_with_datetime() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("o_orderdate", DataType::Date32, false),
    ]));
    let filename = format!("partition.{}", "csv");
    let file_path = tmp_dir.path().join(filename);
    let mut file = File::create(file_path)?;

    // generate some data
    for index in 1..10 {
        let text1 = format!("id{index:}");
        let text2 = format!("1996-10-0{index:}");
        let data = format!("\"{text1}\",\"{text2}\"\r\n");
        file.write_all(data.as_bytes())?;
    }

    ctx.register_csv(
        "orders",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new()
            .schema(&schema)
            .has_header(false)
            .escape(b'\\'),
    )
    .await?;

    let results =
        plan_and_collect(&ctx, "SELECT * from orders where o_orderdate = '19961006'")
            .await?;

    let expected = vec![
        "+-----+---------+",
        "| c1  | c2      |",
        "+-----+---------+",
        "| id1 | value\"1 |",
        "| id2 | value\"2 |",
        "| id3 | value\"3 |",
        "| id4 | value\"4 |",
        "| id5 | value\"5 |",
        "| id6 | value\"6 |",
        "| id7 | value\"7 |",
        "| id8 | value\"8 |",
        "| id9 | value\"9 |",
        "+-----+---------+",
    ];

    assert_batches_sorted_eq!(expected, &results);
    Ok(())
}
