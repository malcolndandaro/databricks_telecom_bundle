# Databricks notebook source
dbutils.widgets.text("p_catalog", "dev")
dbutils.widgets.text("p_schema", "misc")
dbutils.widgets.text("p_table", "aux_tbl_clientes")

# COMMAND ----------

p_catalog = dbutils.widgets.get("p_catalog")
p_schema = dbutils.widgets.get("p_schema")
p_table = dbutils.widgets.get("p_table")

# COMMAND ----------

import random

"""
IMEI validation (CRC check) and fake IMEI generation.

You can find more documentation about IMEI here:

http://en.wikipedia.org/wiki/International_Mobile_Station_Equipment_Identity
"""

class ImeiSupport:
    """
    Class for IMEI validation and fake IMEIs generation (by known IMEI). Can check that 15-digit IMEI (std.
    length of IMEI) is valid. Or can generate needful amount of fake IMEI by any known IMEI.
    """

    IMEI_LENGTH = 15

    @staticmethod
    def next(imei):
        """
        Generates next IMEI for the given IMEI. So, using this method you can can generate needful amount of fake IMEIs
        (for example, for testing purpose).

        :param imei: string or integer representation of 15-digit IMEI (std. length of IMEI)

        :return: next value for IMEI
        """
        imei = int(imei)

        imei += 10

        imei //= 10
        check = ImeiSupport.checksum(imei)

        return imei * 10 + check

    @staticmethod
    def checksum(imei):
        """
        Generates check sum for IMEI

        :param imei: string or integer representation of 15-digit IMEI or 14-digits IMEI without CRC.

        :return: checksum for IMEI. If you entered 15-digit (std. IMEI) and IMEI is correct it's will be the same as last
        15-th digit
        """
        imei = int(imei)

        if len(str(imei)) == ImeiSupport.IMEI_LENGTH:
            imei //= 10

        sum = [0] * (ImeiSupport.IMEI_LENGTH-1)

        mod = 10
        for i in range(1, ImeiSupport.IMEI_LENGTH):
            index = i - 1
            sum[index] = int(imei % mod)

            if i % 2 != 0:
                sum[index] *= 2

            if sum[index] >= 10:
                sum[index] = int(sum[index] % 10 + (sum[index] / 10))

            imei /= mod

        check = 0

        for i in range(len(sum)):
            check += sum[i]

        return (check * 9) % 10

    @staticmethod
    def isValid(imei):
        """
        Check that IMEI is valid.

        :param imei: string or integer representation of 15-digit IMEI (std. IMEI)

        :return: True when IMEI is correct, False otherwise
        """

        imei = int(imei)
        crc = ImeiSupport.checksum(imei // 10)

        return crc == (imei % 10)

    @staticmethod
    def generateNew():
        """
        Generates random IMEI number

        :return: fake IMEI number
        """
        imei = 0

        qty = ImeiSupport.IMEI_LENGTH
        while qty > 0:

            if qty != ImeiSupport.IMEI_LENGTH:
                imei = imei * 10 + random.randint(0, 9)
            else:
                imei = random.randint(1, 9)

            qty -= 1

        crc = ImeiSupport.checksum(imei)

        imei = imei * 10 + crc

        assert ImeiSupport.isValid(imei) == True
        assert ImeiSupport.isValid(str(imei)) == True

        return str(imei)

    @staticmethod
    def test():
        """
        Makes tests for this class

        :return: True if everything is OK
        """

        check = ImeiSupport.checksum(35806501910426)
        assert check == 5

        check = ImeiSupport.checksum("35806501910426")
        assert check == 5

        check = ImeiSupport.checksum(358065019104265)
        assert check == 5

        check = ImeiSupport.checksum("358065019104265")
        assert check == 5

        check = ImeiSupport.checksum(35780502398494)
        assert check == 2

        check = ImeiSupport.checksum(35693803564380)
        assert check == 9

        check = ImeiSupport.checksum(49015420323751)
        assert check == 8

        check = ImeiSupport.checksum(490154203237518)
        assert check == 8

        check = ImeiSupport.checksum("49015420323751")
        assert check == 8

        assert ImeiSupport.isValid(358065019104265) == True
        assert ImeiSupport.isValid(357805023984942) == True
        assert ImeiSupport.isValid(356938035643809) == True

        assert ImeiSupport.isValid(358065019104263) == False
        assert ImeiSupport.isValid(357805023984941) == False
        assert ImeiSupport.isValid(356938035643801) == False

        assert ImeiSupport.next(358065019104273) == 358065019104281

        assert ImeiSupport.next(357805023984942) == 357805023984959
        assert ImeiSupport.next(356938035643809) == 356938035643817

        return True

if __name__ == '__main__':
    ImeiSupport.test()
    print("TEST SUCCESSFULLY FINISHED")


# COMMAND ----------

def generate_imei():
  return ImeiSupport.generateNew()
spark.udf.register(f"generate_imei", generate_imei)

def generate_imei_description(imei: str):
    smartphone_brands = {
    '1': "Apple",
    '2': "Samsung",
    '3': "Xiaomi",
    '4': "OnePlus",
    '5': "Google",
    '6': "Motorola",
    '7': "Lenovo",
    '8': "Sony",
    '9': "Huawei"}

    return smartphone_brands[imei[0]]
spark.udf.register(f"generate_brand", generate_imei_description)


# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog $p_catalog

# COMMAND ----------

import dbldatagen as dg
from dbldatagen import FakerTextFactory, DataGenerator, fakerText, DataAnalyzer
from faker.providers import internet
from pyspark.sql.types import FloatType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def generate_cpf():
    import random

    def calculate_digit(digits):
        s = sum([int(digit) * (len(digits) + 1 - idx) for idx, digit in enumerate(digits)])
        remainder = s % 11
        return '0' if remainder < 2 else str(11 - remainder)

    digits = [str(random.randint(0, 9)) for _ in range(9)]
    digits.append(calculate_digit(digits))
    digits.append(calculate_digit(digits))
    cpf = ''.join(digits)
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"

generate_cpf_udf = udf(generate_cpf, StringType())
spark.udf.register(f"generate_cpf_udf", generate_cpf_udf)

# COMMAND ----------

spark.udf.register(f"generate_cpf_udf", generate_cpf_udf)

# COMMAND ----------

FakerTextIT = FakerTextFactory(locale=['pt_BR'])

data_rows = int(6000000/10)
generation_spec = (
    dg.DataGenerator(
                    name='clientes', 
                    sparkSession=spark,
                     rows=data_rows,
                     random=False
                     )
    .withColumn('nu_tlfn', 'string', uniqueValues=data_rows, text=FakerTextIT(  "msisdn"))
    .withColumn('nu_doct', 'string', uniqueValues=data_rows, expr="generate_cpf_udf()")#text=FakerTextIT("cpf"))
    .withColumn('user_id', 'string', expr="upper(substring(cast(uuid() as string), 1, 20))", uniqueValues=data_rows)
    .withColumn("nu_imei_aprl", 'string', expr="generate_imei()")
    .withColumn("ds_modl_orig_aprl", 'string', base_column="nu_imei_aprl", expr="generate_brand(nu_imei_aprl)")
    .withColumn('cd_ddd', 'smallint', expr="substr(nu_tlfn, 3,2)")
    .withColumn('uf', 'string', baseColumn="cd_ddd", expr=f"{p_schema}.lookup_uf_by_ddd(cd_ddd)")
    .withColumn('no_lgrd', 'string', text=FakerTextIT("street_address"), random=True)
    .withColumn('no_imovel', 'string', text=FakerTextIT("building_number"), random=True)
    .withColumn('no_brro', 'string', text=FakerTextIT("bairro"), random=True)
    .withColumn('nu_cep', 'string', text=FakerTextIT("postcode"), random=True)
    .withColumn('cd_ibge_mnco', baseColumns=["cd_ddd", "uf"], expr=f"{p_schema}.lookup_random_city_by_ddd_uf(cd_ddd,uf)")
    .withColumn('no_mnco', 'string', baseColumn="cd_ibge_mnco", expr=f"{p_schema}.lookup_city_by_ibge(cd_ibge_mnco)")
    .withColumn("client", 'string', values=["P", "S", "G", "PL", "V"], weights=[0.3, 0.25, 0.2, 0.15, 0.1], random=True)
    )


# COMMAND ----------

df_aux_tbl_clientes = generation_spec.build(withStreaming=False)

# COMMAND ----------


table_full_name = f"{p_catalog}.{p_schema}.{p_table}"
spark.sql(f"drop table if exists {table_full_name}")


# COMMAND ----------
(
    df_aux_tbl_clientes
    .withColumn("last_update", current_timestamp())
    .write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(table_full_name)
)

