# NOTE: This is a workaround to use MultipleCronTriggerTimetable introduced in Airflow 3 while on Airflow 2.11.0. 
# Apache Airflow source code taken from https://airflow.apache.org/docs/apache-airflow/stable/_modules/airflow/timetables/trigger.html#MultipleCronTriggerTimetable
# Once migrated to Airflow 3, we can remove this code and access this same functionality via:
# from airflow.timetables.trigger import MultipleCronTriggerTimetable
from __future__ import annotations

import datetime
import functools
import math
import operator
import time
import pendulum
from typing import Any
from dateutil.relativedelta import relativedelta
from pendulum import DateTime
from pendulum.tz.timezone import FixedTimezone, Timezone

from airflow.timetables.base import DagRunInfo, DataInterval, Timetable, TimeRestriction
from airflow.timetables.trigger import CronTriggerTimetable

from airflow.serialization.decoders import decode_interval, decode_run_immediately
from airflow.serialization.encoders import encode_interval, encode_run_immediately, encode_timezone

from airflow.plugins_manager import AirflowPlugin


class MultipleCronTriggerTimetable(Timetable):
    """
    Timetable that triggers DAG runs according to multiple cron expressions.

    This combines multiple ``CronTriggerTimetable`` instances underneath, and
    triggers a DAG run whenever one of the timetables want to trigger a run.

    Only at most one run is triggered for any given time, even if more than one
    timetable fires at the same time.
    """

    def __init__(
        self,
        *crons: str,
        timezone: str | Timezone | FixedTimezone,
        interval: datetime.timedelta | relativedelta = datetime.timedelta(),
        run_immediately: bool | datetime.timedelta = False,
    ) -> None:
        if not crons:
            raise ValueError("cron expression required")
        self._timetables = [
            CronTriggerTimetable(cron, timezone=timezone, interval=interval, run_immediately=run_immediately)
            for cron in crons
        ]
        self.description = ", ".join(t.description for t in self._timetables)

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
      return cls(
          *data["expressions"],
          timezone=pendulum.timezone(data["timezone"]),
          interval=decode_interval(data["interval"]),
          run_immediately=decode_run_immediately(data["run_immediately"]),
      )

    def serialize(self) -> dict[str, Any]:
      # All timetables share the same timezone, interval, and run_immediately
      # values, so we can just use the first to represent them.
      timetable = self._timetables[0]
      return {
          "expressions": [t._expression for t in self._timetables],
          "timezone": encode_timezone(timetable._timezone),
          "interval": encode_interval(timetable._interval),
          "run_immediately": encode_run_immediately(timetable._run_immediately),
      }

    @property
    def summary(self) -> str:
        return ", ".join(t.summary for t in self._timetables)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return min(
            (t.infer_manual_data_interval(run_after=run_after) for t in self._timetables),
            key=operator.attrgetter("start"),
        )

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        infos = (
            timetable.next_dagrun_info(
                last_automated_data_interval=last_automated_data_interval,
                restriction=restriction,
            )
            for timetable in self._timetables
        )
        if restriction.catchup:
            select_key = self._dagrun_info_sort_key_catchup
        else:
            select_key = functools.partial(self._dagrun_info_sort_key_no_catchup, now=time.time())
        return min(infos, key=select_key)

    @staticmethod
    def _dagrun_info_sort_key_catchup(info: DagRunInfo | None) -> float:
        """
        Sort key for DagRunInfo values when catchup=True.

        This is passed as the sort key to ``min`` in ``next_dagrun_info`` to
        find the next closest run, ordered by logical date.

        The sort is done by simply returning the logical date converted to a
        Unix timestamp. If the input is *None* (no next run), *inf* is returned
        so it's selected last.
        """
        if info is None:
            return math.inf
        return info.logical_date.timestamp()

    @staticmethod
    def _dagrun_info_sort_key_no_catchup(info: DagRunInfo | None, *, now: float) -> float:
        """
        Sort key for DagRunInfo values when catchup=False.

        When catchup is disabled, we want to ignore as many runs as possible
        without going over the current time, but if no runs should happen right
        now, we want to choose the earliest opportunity.

        Combining with the ``min`` sorter in ``next_dagrun_info``, we should
        order values by ``-logical_date`` if they are earlier than or at current
        time, but ``+logical_date`` if later.
        """
        if info is None:
            return math.inf
        if (ts := info.logical_date.timestamp()) <= now:
            return -ts
        return ts


class MultipleCronTriggerTimetablePlugin(AirflowPlugin):
    name = "multiple_cron_trigger_timetable_plugin"
    timetables = [MultipleCronTriggerTimetable]