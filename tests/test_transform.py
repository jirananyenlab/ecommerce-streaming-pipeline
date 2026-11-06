            process_test_silver_batch(layer, batch_df)

        def gold_batch(batch_df, batch_id):
            merge_daily_sales(batch_df, batch_id, silver_table, gold_table)

        silver_query = (
            self.spark.readStream.table(self.bronze_table)
            .writeStream.foreachBatch(silver_batch)
            .option("checkpointLocation", f"{self.checkpoint}/silver")
            .trigger(availableNow=True)
            .start()
        )
        silver_query.awaitTermination()

        gold_query = (
            self.spark.readStream
            .option("readChangeFeed", "true")
            .table(self.silver_table)
            .writeStream.foreachBatch(gold_batch)
            .option("checkpointLocation", f"{self.checkpoint}/gold")
            .trigger(availableNow=True)
            .start()
        )
        gold_query.awaitTermination()

    def assert_gold(self, category, order_count, units, revenue):
        expected_schema = StructType([
            StructField("order_date", DateType(), True),
            StructField("country", StringType(), True),
            StructField("category", StringType(), True),
            StructField("order_count", LongType(), True),
            StructField("units_sold", LongType(), True),
            StructField("revenue_usd", DecimalType(28, 2), True),
        ])
        expected = self.spark.createDataFrame(
            [(date(2026, 11, 5), "Japan", category, order_count, units, Decimal(revenue))],
            expected_schema,
        ).collect()
        actual = self.spark.table(self.gold_table).collect()
        self.assertEqual(actual, expected)

    def test_scd1_cdf_group_move_and_retry(self):
        old_time = datetime(2026, 11, 5, 9, 45)
        new_time = datetime(2026, 11, 5, 9, 48)

        # Round 1: insert one order.
        first_batch = self.spark.createDataFrame([
            ("1", "Japan", "Books", "10", "2", "11/5/2026", "20",
             "round1.csv", old_time, None, None),
        ], BRONZE_SCHEMA)
        first_batch.write.mode("append").saveAsTable(self.bronze_table)
        self.run_streams()
        self.assert_gold("Books", 1, 2, "20.00")

        # Round 2: update the order, move its group, and reject an invalid order.
        second_batch = self.spark.createDataFrame([
            ("1", "Japan", "Sports", "10", "3", "11/5/2026", "30",
             "round2.csv", new_time, None, None),
            ("2", "Japan", "Books", "10", "0", "11/5/2026", "0",
             "round2.csv", new_time, None, None),
        ], BRONZE_SCHEMA)
        second_batch.write.mode("append").saveAsTable(self.bronze_table)
        self.run_streams()
        self.assert_gold("Sports", 1, 3, "30.00")
        self.assertEqual(self.spark.table(self.silver_table).count(), 1)
        self.assertEqual(self.spark.table(self.bad_table).count(), 1)

        # Force a replay of the same Silver batch after its MERGE already committed.
        process_test_silver_batch(self.layer, second_batch)
        self.assertEqual(self.spark.table(self.silver_table).count(), 1)
        self.assertEqual(self.spark.table(self.bad_table).count(), 1)

        # Replay actual CDF, including the old and new group images.
        changes = (
            self.spark.read
            .option("readChangeFeed", "true")
            .option("startingVersion", 0)
            .table(self.silver_table)
        )
        change_types = {row._change_type for row in changes.select("_change_type").distinct().collect()}
        self.assertIn("update_preimage", change_types)
        self.assertIn("update_postimage", change_types)
        merge_daily_sales(changes, 999, self.silver_table, self.gold_table)
        merge_daily_sales(changes, 999, self.silver_table, self.gold_table)
        self.assert_gold("Sports", 1, 3, "30.00")

        # An older processing timestamp cannot overwrite the newer order.
        first_batch.write.mode("append").saveAsTable(self.bronze_table)
        self.run_streams()
        self.assert_gold("Sports", 1, 3, "30.00")
        self.assertEqual(
            self.spark.table(self.silver_table).first()._processing_time,
            new_time,
        )

        # Starting both streams again with the same checkpoints adds nothing.
        self.run_streams()
        self.assert_gold("Sports", 1, 3, "30.00")

suite = unittest.TestSuite()
suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(TestSilverTransform))
suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(TestStreamingPipeline))
result = unittest.TextTestRunner(verbosity=2).run(suite)

if not result.wasSuccessful():
    raise AssertionError("Pipeline tests failed. Read the unittest output above.")