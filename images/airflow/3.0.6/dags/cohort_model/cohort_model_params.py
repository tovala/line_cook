from typing import List

from airflow.sdk import Param

def mealsPerOrderAssumptionsParam(order_type: str, default: float = 0.000) -> Param:
  '''
  Fn to set meals per order assumption Params in Airflow.

  :param order_type: Name of the order type for the current assumption value
  :param default: Type float. Default value for the given order type param. (0.000 if not specified)
  '''
  return Param(
    default,
    title=f'Default for {order_type}',
    section='Meals Per Order Assumptions',
    type='number'
  )

def sixWeekAttachRateParam(order_type: str, default: List[float] = [0.000, 0.000, 0.200, 0.050, 0.000, 0.000]) -> Param:
  '''
  Fn to set six week attach rate Params in Airflow.

  :param order_type: Name of the order type for the current assumption value
  :param default: List of default values for the given order type param.
  '''
  return Param(
    default,
    title=f'Default for {order_type}',
    description='Attach rates for first six weeks.',
    section='Six Week Attach Rates',
    type='array',
    maxItems=6,
    minItems=6
  )