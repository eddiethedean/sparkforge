# Fixed pipeline - add source_silvers to silver_second

.add_silver_transform(
    name="silver_second",
    source_bronze="bronze_main",
    transform=silver_transform2,
    rules=silver_rules2,
    table_name="sof_loss_silver_2",
    watermark_col="source_date",
    source_silvers=["silver_main"],  # ← ADD THIS LINE! Declares dependency on silver_main
)
