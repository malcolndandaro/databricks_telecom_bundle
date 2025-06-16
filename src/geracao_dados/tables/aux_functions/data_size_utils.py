# Databricks notebook source

# COMMAND ----------

def get_data_size_multiplier(data_size: str) -> float:
    """Returns the multiplier for data generation based on the data_size parameter.
    
    Args:
        data_size (str): The data size parameter ('small', 'medium', or 'large')
        
    Returns:
        float: The multiplier to apply to the original data size
    """
    size_multipliers = {
        'small': 0.05,  # 5%
        'medium': 0.25,  # 25%
        'large': 1.0    # 100%
    }
    
    if data_size not in size_multipliers:
        valid_sizes = list(size_multipliers.keys())
        raise ValueError(
            f"Invalid data_size: {data_size}. Must be one of {valid_sizes}"
        )
        
    return size_multipliers[data_size]


def calculate_data_rows(original_rows: int, data_size: str) -> int:
    """Calculates the number of rows to generate based on size parameters.
    
    Args:
        original_rows (int): The original number of rows
        data_size (str): The data size parameter ('small', 'medium', or 'large')
        
    Returns:
        int: The number of rows to generate
    """
    multiplier = get_data_size_multiplier(data_size)
    return int(original_rows * multiplier)

# COMMAND ----------

# Example usage:
# original_rows = 1000000
# data_size = dbutils.widgets.get("data_size")
# rows_to_generate = calculate_data_rows(original_rows, data_size) 