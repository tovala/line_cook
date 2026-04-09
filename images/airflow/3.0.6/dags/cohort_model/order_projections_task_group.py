
initial_values_vector = hook.get_df(sql="SELECT * FROM my_table WHERE date_column >= %s",
    parameters={"date_column": "2023-01-01"},
    df_type="polars",)