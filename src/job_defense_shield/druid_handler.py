"""Read stored JS1 payloads back from Apache Druid in bulk.

This is the third storage backend for the per-job summary statistics, alongside
Slurm's AdminComment field and the external MySQL/MariaDB database in
db_handler.py. It exists for sites whose administrators will not allow an
external program to write to slurmdbd at all -- the same motivation as the
external database, taken one step further, since that backend is also declined.

The Jobstats Kafka producer writes each job's opaque `JS1:` blob into a Druid
datasource as the `js1` column. That string is byte-identical to what would have
been stored in AdminComment, sentinels included, so the values returned here
feed the existing get_stats_dict() decode path unchanged.

The interface deliberately mirrors ShieldDBHandler so that both backends drop
into the same merge in job_defense_shield.py.
"""

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
import sys

import pandas as pd
import requests

# The sacct window selects jobs by overlap, but Druid rows are timestamped with
# the job's end time, so a job can be in the sacct result and just outside a
# naive Druid window. Pad both ends. The merge is keyed on job id, so extra rows
# are discarded harmlessly while missing rows would silently drop a job.
WINDOW_PADDING_DAYS = 1

# Bulk scans are far heavier than the single-job point lookups the Jobstats CLI
# performs, so they get their own deadline rather than inheriting that one.
DEFAULT_BULK_TIMEOUT = 120


class ShieldDruidHandler:

    """This class allows for summary statistics to be read from an Apache Druid
       datasource populated by the Jobstats Kafka producer."""

    def __init__(self,
                 conn_params: Dict[str, Any],
                 clusters: str,
                 start_date: datetime,
                 end_date: datetime,
                 verbose: bool) -> None:
        self.conn_params = conn_params
        self.url = conn_params.get("url")
        self.datasource = conn_params.get("datasource", "slurm_jobstats")
        self.timeout = int(conn_params.get("bulk_timeout", DEFAULT_BULK_TIMEOUT))
        self.start_date = start_date
        self.end_date = end_date
        raw = [cluster.strip() for cluster in clusters.split(",")]
        self.cluster_list = None if "all" in raw else raw
        self.verbose = verbose
        self.stats = None

    @staticmethod
    def _to_druid_timestamp(dt: datetime) -> str:
        """Convert a local datetime to the naive UTC string Druid stores.

        Druid timestamps are naive UTC while prepare_datetimes() returns naive
        local ones, so the offset has to be applied explicitly. Without this the
        window is wrong by the UTC offset and quietly returns the wrong jobs.
        A naive datetime is assumed to be local time by astimezone().
        """
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    def _day_windows(self) -> List[tuple]:
        """Split the padded date range into one-day windows.

        A single query over the whole range would return one response holding
        every blob in it, which is hundreds of megabytes on a large cluster.
        Chunking bounds the size of any one response.
        """
        start = self.start_date - timedelta(days=WINDOW_PADDING_DAYS)
        end = self.end_date + timedelta(days=WINDOW_PADDING_DAYS)
        windows = []
        lower = start
        while lower < end:
            upper = min(lower + timedelta(days=1), end)
            windows.append((lower, upper))
            lower = upper
        return windows

    def _build_query(self) -> str:
        """Return the parameterized SQL for one day window."""
        query = (
            "SELECT jobid, cluster, js1 AS admin_comment "
            f'FROM "{self.datasource}" '
            "WHERE __time >= ? AND __time < ? AND js1 IS NOT NULL"
        )
        if self.cluster_list:
            placeholders = ", ".join(["?"] * len(self.cluster_list))
            query += f" AND cluster IN ({placeholders})"
        return query

    def _run_query(self, lower: datetime, upper: datetime) -> List[dict]:
        """Execute one windowed query and return the raw rows."""
        parameters = [{"type": "TIMESTAMP", "value": self._to_druid_timestamp(lower)},
                      {"type": "TIMESTAMP", "value": self._to_druid_timestamp(upper)}]
        if self.cluster_list:
            parameters += [{"type": "VARCHAR", "value": c} for c in self.cluster_list]
        payload = {"query": self._build_query(),
                   "parameters": parameters,
                   # Send the deadline to Druid rather than relying on the HTTP
                   # client timeout alone. Druid keeps executing a query after
                   # the client hangs up, so only this releases broker work.
                   "context": {"timeout": self.timeout * 1000}}
        response = requests.post(self.url,
                                 json=payload,
                                 timeout=self.timeout + 10)
        response.raise_for_status()
        return response.json()

    def get_summary_stats(self) -> Optional[pd.DataFrame]:
        """Return a pandas DataFrame of the summary statistics from Druid."""
        cols = ["jobid", "cluster", "admin_comment"]
        if not self.url:
            print("ERROR: No Druid url found in DRUID_CONFIG.", file=sys.stderr)
            sys.exit(1)
        if self.verbose:
            print(f"INFO: Druid datasource: {self.datasource}")
            print(f"      url={self.url}")
            if self.cluster_list:
                print(f"      clusters={','.join(self.cluster_list)}")
        rows: List[dict] = []
        for lower, upper in self._day_windows():
            try:
                window_rows = self._run_query(lower, upper)
            except Exception as e:
                # Exit rather than returning what was collected so far. A short
                # dataframe is indistinguishable from a quiet week, so a partial
                # result would be reported as "no violations found".
                msg = f"ERROR: Failed to retrieve jobstats from Druid: {e}"
                print(msg, file=sys.stderr)
                sys.exit(1)
            if self.verbose:
                day = lower.strftime("%Y-%m-%d")
                print(f"INFO: Druid rows for {day}: {len(window_rows)}")
            rows.extend(window_rows)
        if not rows:
            self.stats = pd.DataFrame(columns=cols)
        else:
            self.stats = pd.DataFrame(rows)[cols]
            # Druid types jobid as a long while sacct gives strings. The merge
            # is on jobidraw, so both sides have to be strings to match.
            self.stats["jobid"] = self.stats["jobid"].astype("str")
            # The datasource is not rolled up, so a re-emitted job appends a row
            # rather than replacing one. Two rows for a job would duplicate that
            # job in the left join and inflate every downstream aggregate, so
            # collapse to one row per job before returning.
            self.stats = self.stats.drop_duplicates(subset=["jobid", "cluster"],
                                                    keep="last")
            self.stats.reset_index(drop=True, inplace=True)
        if self.verbose:
            print(f"INFO: Number of rows in Druid dataframe: {len(self.stats)}")
            if not self.stats.empty:
                print("INFO: Showing first 5 rows of Druid dataframe below:")
                print(self.stats.head())
        return self.stats
