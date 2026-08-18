from datetime import datetime
from datetime import timezone

import pytest

import druid_handler
from druid_handler import ShieldDruidHandler


conn_params = {"url": "http://druid.institution.edu:8888/druid/v2/sql",
               "datasource": "slurm_jobstats"}


def make_handler(clusters="stellar",
                 start=datetime(2026, 8, 10, 12, 0, 0),
                 end=datetime(2026, 8, 12, 12, 0, 0),
                 verbose=False):
    return ShieldDruidHandler(conn_params, clusters, start, end, verbose)


class FakeResponse:

    """Minimal stand-in for a requests Response."""

    def __init__(self, rows):
        self.rows = rows

    def raise_for_status(self):
        pass

    def json(self):
        return self.rows


def test_timestamp_is_converted_to_utc():
    """A naive local datetime must be shifted by the local UTC offset, since
       Druid stores naive UTC. Comparing against a computed expectation keeps
       the test valid in any timezone."""
    handler = make_handler()
    dt = datetime(2026, 8, 10, 12, 0, 0)
    expected = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    assert handler._to_druid_timestamp(dt) == expected


def test_day_windows_are_padded_and_chunked():
    """A 2-day request becomes 4 one-day windows once padded on both ends."""
    handler = make_handler(start=datetime(2026, 8, 10, 12, 0, 0),
                           end=datetime(2026, 8, 12, 12, 0, 0))
    windows = handler._day_windows()
    assert len(windows) == 4
    assert windows[0][0] == datetime(2026, 8, 9, 12, 0, 0)
    assert windows[-1][1] == datetime(2026, 8, 13, 12, 0, 0)
    # windows must be contiguous and non-overlapping
    for earlier, later in zip(windows, windows[1:]):
        assert earlier[1] == later[0]


def test_query_filters_on_cluster_list():
    handler = make_handler(clusters="stellar,della")
    query = handler._build_query()
    assert "cluster IN (?, ?)" in query
    assert 'FROM "slurm_jobstats"' in query


def test_query_omits_cluster_filter_for_all():
    handler = make_handler(clusters="all")
    assert "cluster IN" not in handler._build_query()


def test_summary_stats_casts_jobid_to_string(mocker):
    """Druid types jobid as a long but the merge is against sacct's jobidraw,
       which is a string. A numeric jobid would match nothing."""
    rows = [{"jobid": 12345, "cluster": "stellar", "admin_comment": "JS1:H4sIA"}]
    mocker.patch("druid_handler.requests.post",
                 return_value=FakeResponse(rows))
    handler = make_handler(start=datetime(2026, 8, 10, 12, 0, 0),
                           end=datetime(2026, 8, 10, 12, 0, 0))
    stats = handler.get_summary_stats()
    assert list(stats.columns) == ["jobid", "cluster", "admin_comment"]
    assert stats.jobid.tolist() == ["12345"]
    assert stats.admin_comment.tolist() == ["JS1:H4sIA"]


def test_summary_stats_accumulates_across_windows(mocker):
    """Each day window is a separate request; the rows are concatenated."""
    windows = [[{"jobid": i, "cluster": "stellar", "admin_comment": "JS1:aaa"}]
               for i in range(4)]
    post = mocker.patch("druid_handler.requests.post",
                        side_effect=[FakeResponse(w) for w in windows])
    handler = make_handler(start=datetime(2026, 8, 10, 12, 0, 0),
                           end=datetime(2026, 8, 12, 12, 0, 0))
    stats = handler.get_summary_stats()
    assert post.call_count == 4
    assert stats.jobid.tolist() == ["0", "1", "2", "3"]


def test_repeated_job_is_collapsed_to_one_row(mocker):
    """slurm_jobstats is not rolled up, so a re-emitted job appends a second
       row. Left-joining that would duplicate the job and inflate aggregates."""
    rows = [{"jobid": 12345, "cluster": "stellar", "admin_comment": "JS1:old"},
            {"jobid": 12345, "cluster": "stellar", "admin_comment": "JS1:new"}]
    mocker.patch("druid_handler.requests.post",
                 return_value=FakeResponse(rows))
    handler = make_handler(start=datetime(2026, 8, 10, 12, 0, 0),
                           end=datetime(2026, 8, 10, 12, 0, 0))
    stats = handler.get_summary_stats()
    assert len(stats) == 1
    assert stats.admin_comment.tolist() == ["JS1:new"]


def test_empty_result_gives_empty_frame_with_columns(mocker):
    mocker.patch("druid_handler.requests.post",
                 return_value=FakeResponse([]))
    handler = make_handler(start=datetime(2026, 8, 10, 12, 0, 0),
                           end=datetime(2026, 8, 10, 12, 0, 0))
    stats = handler.get_summary_stats()
    assert stats.empty
    assert list(stats.columns) == ["jobid", "cluster", "admin_comment"]


def test_failed_query_exits_rather_than_returning_partial(mocker):
    """A partial dataframe is indistinguishable from a quiet week, so it would
       be reported as 'no violations'. The run must stop instead."""
    mocker.patch("druid_handler.requests.post",
                 side_effect=RuntimeError("broker timeout"))
    handler = make_handler()
    with pytest.raises(SystemExit):
        handler.get_summary_stats()


def test_missing_url_exits(mocker):
    handler = ShieldDruidHandler({}, "stellar",
                                 datetime(2026, 8, 10),
                                 datetime(2026, 8, 11),
                                 False)
    with pytest.raises(SystemExit):
        handler.get_summary_stats()


def test_timestamps_are_sent_as_query_parameters(mocker):
    """The window bounds must reach Druid as bound parameters in UTC, ahead of
       any cluster parameters."""
    post = mocker.patch("druid_handler.requests.post",
                        return_value=FakeResponse([]))
    handler = make_handler(clusters="stellar",
                           start=datetime(2026, 8, 10, 12, 0, 0),
                           end=datetime(2026, 8, 10, 12, 0, 0))
    handler.get_summary_stats()
    payload = post.call_args.kwargs["json"]
    params = payload["parameters"]
    assert [p["type"] for p in params] == ["TIMESTAMP", "TIMESTAMP", "VARCHAR"]
    assert params[2]["value"] == "stellar"
    # the server-side deadline must be set, not just the client timeout
    assert payload["context"]["timeout"] == druid_handler.DEFAULT_BULK_TIMEOUT * 1000


# --- gating: which runs actually fetch from Druid ---------------------------

from argparse import Namespace

from utils import needs_stored_stats


def make_args(**flags):
    """An argparse-like namespace with the non-flag options present too."""
    base = {"days": 7, "clusters": "all", "partition": "", "config_file": None,
            "starttime": None, "endtime": None,
            "cancel_zero_gpu_jobs": False, "low_gpu_efficiency": False,
            "zero_util_gpu_hours": False, "usage_overview": False,
            "jobs_overview": False, "email": False, "dump_files": False}
    base.update(flags)
    return Namespace(**base)


def test_cancellation_alert_does_not_fetch():
    """It builds its own statistics from Prometheus for running jobs. This is
       the case that matters: the cancellation cron fires every few minutes."""
    assert not needs_stored_stats(make_args(cancel_zero_gpu_jobs=True))


def test_sacct_only_reports_do_not_fetch():
    assert not needs_stored_stats(make_args(usage_overview=True))
    assert not needs_stored_stats(make_args(jobs_overview=True))


def test_efficiency_alerts_fetch():
    assert needs_stored_stats(make_args(low_gpu_efficiency=True))
    assert needs_stored_stats(make_args(zero_util_gpu_hours=True))


def test_mixed_run_fetches():
    """A weekly run combining both kinds still needs the statistics."""
    assert needs_stored_stats(make_args(usage_overview=True,
                                        low_gpu_efficiency=True))


def test_value_options_are_not_mistaken_for_flags():
    """--days 7 and --clusters bouchet must not count as requested alerts."""
    assert not needs_stored_stats(make_args(days=7, clusters="bouchet"))


def test_email_modifiers_do_not_by_themselves_fetch():
    assert not needs_stored_stats(make_args(cancel_zero_gpu_jobs=True,
                                            email=True, dump_files=True))


def test_unknown_future_alert_defaults_to_fetching():
    """An alert added upstream must fetch rather than silently see no data."""
    assert needs_stored_stats(make_args(some_new_alert=True))
