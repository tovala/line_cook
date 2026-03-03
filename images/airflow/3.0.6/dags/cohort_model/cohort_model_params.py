from typing import List

from airflow.sdk import Param

class mealsPerOrderAssumptionsParam(Param):
  '''
  Class to set meals per order assumption Params in Airflow. Extends Airflow Param.

  :param order_type: Name of the order type for the current assumption value
  :param default: Type float. Default value for the given order type param. (0.000 if not specified)
  '''
  def __init__(
    self,
    order_type: str,
    default: float = 0.000,
    **kwargs
  ):
    super().__init__(
      default,
      title=f'Default for {order_type}',
      section='Meals Per Order Assumptions',
      type='number',
      **kwargs
    )

class sixWeekAttachRateParam(Param):
  '''
  Class to set six week attach rate Params in Airflow. Extends Airflow Param.

  :param order_type: Name of the order type for the current assumption value
  :param default: List of default values for the given order type param.
  '''
  def __init__(
    self,
    order_type: str,
    default: List[float] = [0.000, 0.000, 0.200, 0.050, 0.000, 0.000],
    **kwargs
  ):
    super().__init__(
      default,
      title=f'Default for {order_type}',
      description='Attach rates for first six weeks.',
      section='Six Week Attach Rates',
      type='array',
      **kwargs
    )