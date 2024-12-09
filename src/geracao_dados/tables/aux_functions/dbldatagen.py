# Databricks notebook source
import dbldatagen as dg
table = "malcoln.m2c.product_subscriptions"
dfSource = spark.read.table(table)
analyzer = dg.DataAnalyzer(df=dfSource)
analyzer.scriptDataGeneratorFromSchema(schema=dfSource.schema)
