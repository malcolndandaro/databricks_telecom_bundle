# Databricks notebook source
import dlt

dlt.apply_changes_from_snapshot(
  target = "product_subscriptions",
  source = f"aux_bronze_product_subscriptions",
  keys = ["nu_tlfn", "nu_doct", "user_id", "id_prdt", "dt_prmr_atvc_lnha"],
  track_history_except_column_list=["silver_ts"],
  stored_as_scd_type = "2")

