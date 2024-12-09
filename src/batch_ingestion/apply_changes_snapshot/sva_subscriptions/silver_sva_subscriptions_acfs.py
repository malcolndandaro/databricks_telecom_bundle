# Databricks notebook source
import dlt

dlt.apply_changes_from_snapshot(
  target = "sva_subscriptions",
  source = f"aux_bronze_sva_subscriptions",
  keys = ["msisdn", "productid", "data_contratacao"],
  track_history_except_column_list=["silver_ts"],
  stored_as_scd_type = "2")

