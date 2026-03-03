from airflow.sdk import Param

def mealsPerOrderAssumptionsParam(order_type: str, default_val: float):
  return Param(default_val, title=f'Default for {order_type}', section='Meals Per Order Assumptions', type='number')