# Databricks notebook source
dbutils.widgets.text("p_catalog", "dev")
dbutils.widgets.text("p_schema", "misc")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG $p_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS $p_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE $p_schema.aux_tbl_servicos AS
# MAGIC SELECT productid, productname, spname, productaccountinggroup, servicetype, grossvalue, company, cast(taxpis as decimal(7,2)) taxpis, cast(taxcofins as decimal(7,2)) taxcofins, cast(taxiss as decimal(7,2)) FROM (
# MAGIC   VALUES
# MAGIC     ('PROD001', 'Seguro Celular Premium', 'SeguroTech', 'Seguros', 'Avulso', 99, 'TelecomCorp', 1.65, 7.60, 5.00),
# MAGIC     ('PROD002', 'Pacote de Canais Esportivos', 'SportTV', 'Entretenimento', 'Bundle', 119, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD003', 'Antivírus Mobile', 'CyberSafe', 'Segurança', 'Avulso', 10, 'TelecomCorp', 1.65, 7.60, 2.00),
# MAGIC     ('PROD004', 'Streaming de Música Premium', 'MusicStream', 'Entretenimento', 'Bundle', 29, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD005', 'Backup em Nuvem 100GB', 'CloudStore', 'Armazenamento', 'Avulso', 10, 'TelecomCorp', 1.65, 7.60, 2.50),
# MAGIC     ('PROD006', 'Pacote de Filmes On Demand', 'CineMax', 'Entretenimento', 'Bundle', 99, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD007', 'Assistência Técnica 24h', 'TechSupport', 'Suporte', 'Avulso', 10, 'TelecomCorp', 1.65, 7.60, 5.00),
# MAGIC     ('PROD008', 'VPN Segura', 'SecureNet', 'Segurança', 'Avulso', 15, 'TelecomCorp', 1.65, 7.60, 2.00),
# MAGIC     ('PROD009', 'Pacote de Jogos Mobile', 'GameZone', 'Entretenimento', 'Bundle', 40, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD010', 'Controle Parental', 'KidSafe', 'Segurança', 'Avulso', 10, 'TelecomCorp', 1.65, 7.60, 2.00),
# MAGIC     ('PROD011', 'Assinatura de Revistas Digitais', 'DigiMags', 'Conteúdo', 'Bundle', 19, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD012', 'Seguro Viagem Internacional', 'TravelSafe', 'Seguros', 'Avulso', 99, 'TelecomCorp', 1.65, 7.60, 5.00),
# MAGIC     ('PROD013', 'Pacote de Canais Infantis', 'KidsTV', 'Entretenimento', 'Bundle', 49, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD014', 'Gerenciador de Senhas', 'PassKeeper', 'Segurança', 'Avulso', 10, 'TelecomCorp', 1.65, 7.60, 2.00),
# MAGIC     ('PROD015', 'Curso de Idiomas Online', 'LinguaLearn', 'Educação', 'Bundle', 20, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD016', 'Backup em Nuvem 500GB', 'CloudStore', 'Armazenamento', 'Avulso', 40, 'TelecomCorp', 1.65, 7.60, 2.50),
# MAGIC     ('PROD017', 'Pacote de Notícias Premium', 'NewsNow', 'Conteúdo', 'Bundle', 10, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD018', 'Seguro Roubo e Furto', 'SecureDevice', 'Seguros', 'Avulso', 79, 'TelecomCorp', 1.65, 7.60, 5.00),
# MAGIC     ('PROD019', 'Pacote de Documentários', 'DocWorld', 'Entretenimento', 'Bundle', 20, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD020', 'Filtro de Spam e Malware', 'CleanMail', 'Segurança', 'Avulso', 10, 'TelecomCorp', 1.65, 7.60, 2.00),
# MAGIC     ('PROD021', 'Assinatura de Audiolivros', 'AudioBooks', 'Conteúdo', 'Bundle', 5, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD022', 'Seguro Quebra Acidental', 'OopsProtect', 'Seguros', 'Avulso', 79, 'TelecomCorp', 1.65, 7.60, 5.00),
# MAGIC     ('PROD023', 'Pacote de Séries Exclusivas', 'SeriesMax', 'Entretenimento', 'Bundle', 49, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD024', 'Monitoramento de Crédito', 'CreditWatch', 'Segurança', 'Avulso', 10, 'TelecomCorp', 1.65, 7.60, 2.00),
# MAGIC     ('PROD025', 'Curso de Programação Online', 'CodeAcademy', 'Educação', 'Bundle', 10, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD026', 'Backup em Nuvem 1TB', 'CloudStore', 'Armazenamento', 'Avulso', 80, 'TelecomCorp', 1.65, 7.60, 2.50),
# MAGIC     ('PROD027', 'Pacote de Fitness Digital', 'FitLife', 'Saúde', 'Bundle', 20, 'TelecomCorp', 1.65, 7.60, 3.00),
# MAGIC     ('PROD028', 'Seguro Extensão de Garantia', 'ExtraWarranty', 'Seguros', 'Avulso', 40, 'TelecomCorp', 1.65, 7.60, 5.00),
# MAGIC     ('PROD030', 'Assistente Virtual Premium', 'AIAssist', 'Produtividade', 'Avulso', 20, 'TelecomCorp', 1.65, 7.60, 2.00)
# MAGIC ) AS products(productid, productname, spname, productaccountinggroup, servicetype, grossvalue, company, taxpis, taxcofins, taxiss);
