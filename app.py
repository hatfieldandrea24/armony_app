import os
import inspect
from datetime import date, timedelta
from typing import Any

import altair as alt
import duckdb
import pandas as pd
import streamlit as st


st.set_page_config(page_title="Armony Client Intelligence", layout="wide")

TARGET_SEGMENTS = ("commercial_target", "res_target")
ALTAIR_ACCEPTS_WIDTH = "width" in inspect.signature(st.altair_chart).parameters


def _lower_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df


def _render_altair_chart(chart: alt.Chart) -> None:
    if ALTAIR_ACCEPTS_WIDTH:
        st.altair_chart(chart, width="stretch")
    else:
        st.altair_chart(chart, use_container_width=True)


def _in_filter(
    column_sql: str,
    values: tuple[str, ...],
    *,
    lower_input: bool = False,
    lower_column: bool = False,
) -> tuple[str, list[Any]]:
    clean_values = [v for v in values if v]
    if not clean_values:
        return "", []
    if lower_input:
        clean_values = [v.lower() for v in clean_values]
    placeholders = ",".join("?" for _ in clean_values)
    col = f"lower({column_sql})" if lower_column else column_sql
    return f" and {col} in ({placeholders})", clean_values


LATEST_PERMIT_CTE = """
with typed as (
    select
        permit_number,
        coalesce(county, '') as county,
        coalesce(jurisdiction, '') as jurisdiction,
        coalesce(city, '') as city,
        street_no,
        street,
        zipcode,
        upper(coalesce(status, '')) as status,
        type,
        subtype,
        description,
        try_cast(nullif(file_date, '\\N') as date) as file_date,
        try_cast(nullif(issue_date, '\\N') as date) as issue_date,
        try_cast(nullif(job_value, '\\N') as double) as job_value,
        owner_name,
        nullif(owner_email, '\\N') as owner_email,
        nullif(owner_phone, '\\N') as owner_phone,
        applicant_name,
        nullif(applicant_email, '\\N') as applicant_email,
        nullif(applicant_phone, '\\N') as applicant_phone,
        _meta_source_file_name
    from main.combined_data
),
ranked as (
    select
        permit_number,
        county,
        jurisdiction,
        city,
        street_no,
        street,
        zipcode,
        status,
        type,
        subtype,
        description,
        file_date,
        issue_date,
        greatest(
            coalesce(file_date, date '1900-01-01'),
            coalesce(issue_date, date '1900-01-01')
        ) as activity_date,
        job_value,
        owner_name,
        owner_email,
        owner_phone,
        applicant_name,
        applicant_email,
        applicant_phone,
        row_number() over (
            partition by permit_number
            order by greatest(
                coalesce(file_date, date '1900-01-01'),
                coalesce(issue_date, date '1900-01-01')
            ) desc,
            coalesce(_meta_source_file_name, '') desc
        ) as rn
    from typed
),
permit_latest as (
    select
        permit_number,
        county,
        jurisdiction,
        city,
        street_no,
        street,
        zipcode,
        status,
        type,
        subtype,
        description,
        file_date,
        issue_date,
        activity_date,
        job_value,
        owner_name,
        owner_email,
        owner_phone,
        applicant_name,
        applicant_email,
        applicant_phone
    from ranked
    where rn = 1
)
"""


@st.cache_resource(show_spinner=False)
def get_connection(conn_str: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(conn_str)
    try:
        lower_conn = conn_str.strip().lower()
        generic_md = lower_conn in {"md", "md:"} or lower_conn.startswith("md:?")
        if generic_md:
            dbs = conn.execute("pragma database_list").fetchdf()
            if "name" in dbs.columns and "reporting_db" in dbs["name"].str.lower().tolist():
                current = conn.execute("select current_database()").fetchone()[0].lower()
                if current != "reporting_db":
                    conn.execute("use reporting_db")
    except Exception:
        pass
    return conn


@st.cache_data(show_spinner=False)
def connection_info(conn_str: str) -> dict[str, Any]:
    conn = get_connection(conn_str)
    current_db = conn.execute("select current_database()").fetchone()[0]
    db_list = conn.execute("pragma database_list").fetchdf()
    return {"current_database": current_db, "database_list": db_list}


@st.cache_data(show_spinner=False)
def reporting_views(conn_str: str) -> set[str]:
    conn = get_connection(conn_str)
    rows = conn.execute(
        """
        select lower(table_name) as table_name
        from information_schema.views
        where lower(table_catalog) = 'reporting_db'
          and lower(table_schema) = 'main'
        """
    ).fetchall()
    return {r[0] for r in rows}


@st.cache_data(show_spinner=False)
def list_counties(conn_str: str) -> list[str]:
    conn = get_connection(conn_str)
    rows = conn.execute(
        LATEST_PERMIT_CTE
        + """
        select distinct county
        from permit_latest
        where county <> ''
        order by county
        """
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(show_spinner=False)
def list_target_jurisdictions(conn_str: str) -> list[str]:
    conn = get_connection(conn_str)
    rows = conn.execute(
        LATEST_PERMIT_CTE
        + """
        select distinct l.jurisdiction
        from main.permit_segments p
        join permit_latest l using (permit_number)
        where p.segment in ('commercial_target', 'res_target')
          and l.jurisdiction <> ''
        order by 1
        """
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(show_spinner=False)
def list_target_statuses(conn_str: str) -> list[str]:
    conn = get_connection(conn_str)
    rows = conn.execute(
        LATEST_PERMIT_CTE
        + """
        select distinct l.status
        from main.permit_segments p
        join permit_latest l using (permit_number)
        where p.segment in ('commercial_target', 'res_target')
          and l.status <> ''
        order by 1
        """
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(show_spinner=False)
def fetch_target_activity_bounds(conn_str: str) -> tuple[date, date]:
    conn = get_connection(conn_str)
    row = conn.execute(
        LATEST_PERMIT_CTE
        + """
        select
            min(l.activity_date) as min_activity_date,
            max(l.activity_date) as max_activity_date
        from main.permit_segments p
        join permit_latest l using (permit_number)
        where p.segment in ('commercial_target', 'res_target')
          and l.activity_date > date '1900-01-01'
        """
    ).fetchone()
    today = date.today()
    if not row or row[0] is None or row[1] is None:
        return today - timedelta(days=365), today
    min_d = row[0] if isinstance(row[0], date) else pd.to_datetime(row[0]).date()
    max_d = row[1] if isinstance(row[1], date) else pd.to_datetime(row[1]).date()
    return min_d, max_d


@st.cache_data(show_spinner=False)
def fetch_all_activity_bounds(conn_str: str) -> tuple[date, date]:
    conn = get_connection(conn_str)
    row = conn.execute(
        LATEST_PERMIT_CTE
        + """
        select
            min(activity_date) as min_activity_date,
            max(activity_date) as max_activity_date
        from permit_latest
        where activity_date > date '1900-01-01'
        """
    ).fetchone()
    today = date.today()
    if not row or row[0] is None or row[1] is None:
        return today - timedelta(days=365), today
    min_d = row[0] if isinstance(row[0], date) else pd.to_datetime(row[0]).date()
    max_d = row[1] if isinstance(row[1], date) else pd.to_datetime(row[1]).date()
    return min_d, max_d


def _search_mode_sql_and_params(search_mode: str, like_term: str) -> tuple[str, list[Any]]:
    if search_mode == "Address":
        return (
            "concat_ws(' ', cast(l.street_no as varchar), l.street, l.city, l.jurisdiction, cast(l.zipcode as varchar)) ilike ?",
            [like_term],
        )
    if search_mode == "Applicant":
        return (
            "(coalesce(l.applicant_name, '') ilike ? or coalesce(l.applicant_email, '') ilike ? or coalesce(l.applicant_phone, '') ilike ?)",
            [like_term, like_term, like_term],
        )
    if search_mode == "Owner":
        return (
            "(coalesce(l.owner_name, '') ilike ? or coalesce(l.owner_email, '') ilike ? or coalesce(l.owner_phone, '') ilike ?)",
            [like_term, like_term, like_term],
        )
    if search_mode == "Permit #":
        return "coalesce(l.permit_number, '') ilike ?", [like_term]
    return (
        "("
        "coalesce(l.permit_number, '') ilike ?"
        " or concat_ws(' ', cast(l.street_no as varchar), l.street, l.city, l.jurisdiction, cast(l.zipcode as varchar)) ilike ?"
        " or coalesce(l.owner_name, '') ilike ?"
        " or coalesce(l.owner_email, '') ilike ?"
        " or coalesce(l.applicant_name, '') ilike ?"
        " or coalesce(l.applicant_email, '') ilike ?"
        " or coalesce(l.description, '') ilike ?"
        " or coalesce(l.type, '') ilike ?"
        " or coalesce(l.subtype, '') ilike ?"
        ")",
        [like_term] * 9,
    )


@st.cache_data(show_spinner=False, ttl=300)
def fetch_global_search_results(
    conn_str: str,
    search_text: str,
    search_mode: str,
    start_date: str,
    end_date: str,
    result_limit: int,
) -> pd.DataFrame:
    term = search_text.strip()
    if not term:
        return pd.DataFrame()

    conn = get_connection(conn_str)
    mode_sql, mode_params = _search_mode_sql_and_params(search_mode, f"%{term}%")
    params: list[Any] = [start_date, end_date] + mode_params + [result_limit]
    sql = (
        LATEST_PERMIT_CTE
        + f"""
        select
            l.permit_number,
            p.segment,
            p.lead_priority,
            l.activity_date,
            l.file_date,
            l.issue_date,
            l.county,
            l.jurisdiction,
            l.city,
            concat_ws(' ', cast(l.street_no as varchar), l.street) as address,
            cast(l.zipcode as varchar) as zipcode,
            l.status,
            l.type,
            l.subtype,
            l.description,
            l.job_value,
            p.contractor_id,
            p.contractor_group_id,
            p.contractor_is_trade,
            l.owner_name,
            l.owner_email,
            l.owner_phone,
            l.applicant_name,
            l.applicant_email,
            l.applicant_phone
        from permit_latest l
        left join main.permit_segments p using (permit_number)
        where l.activity_date between ? and ?
          and {mode_sql}
        order by l.activity_date desc nulls last, l.file_date desc nulls last
        limit ?
        """
    )
    return _lower_cols(conn.execute(sql, params).fetchdf())


def _default_date_range(min_d: date, max_d: date, days: int = 180) -> tuple[date, date]:
    start = max(min_d, max_d - timedelta(days=days))
    return start, max_d


@st.cache_data(show_spinner=False)
def fetch_overview_metrics(conn_str: str, start_date: str, end_date: str) -> dict[str, Any]:
    conn = get_connection(conn_str)
    sql = (
        LATEST_PERMIT_CTE
        + """
        select
            count(*) as permits_total,
            sum(case when p.segment in ('commercial_target', 'res_target') then 1 else 0 end) as target_permits,
            sum(case when p.segment in ('commercial_target', 'res_target') and p.lead_priority = 'High' then 1 else 0 end) as high_priority_target,
            sum(case when p.segment in ('commercial_target', 'res_target')
                      and (p.contractor_id is null or coalesce(p.contractor_is_trade, false))
                     then 1 else 0 end) as gc_missing_proxy,
            sum(case when p.segment in ('commercial_target', 'res_target')
                      and l.activity_date between ? and ?
                     then 1 else 0 end) as active_target_lookback,
            count(distinct case when l.county <> '' then l.county end) as counties_covered
        from main.permit_segments p
        join permit_latest l using (permit_number)
        """
    )
    row = conn.execute(sql, [start_date, end_date]).fetchone()
    keys = [
        "permits_total",
        "target_permits",
        "high_priority_target",
        "gc_missing_proxy",
        "active_target_lookback",
        "counties_covered",
    ]
    return dict(zip(keys, row))


@st.cache_data(show_spinner=False)
def fetch_data_quality_metrics(conn_str: str) -> dict[str, Any]:
    conn = get_connection(conn_str)
    quality_row = conn.execute(
        LATEST_PERMIT_CTE
        + """
        select
            round(100.0 * sum(case when job_value is not null then 1 else 0 end) / nullif(count(*), 0), 2) as pct_job_value_present,
            round(100.0 * sum(case when owner_email ilike '%@%' then 1 else 0 end) / nullif(count(*), 0), 2) as pct_owner_email_usable,
            round(100.0 * sum(case when applicant_email ilike '%@%' then 1 else 0 end) / nullif(count(*), 0), 2) as pct_applicant_email_usable,
            round(100.0 * sum(case when length(regexp_replace(coalesce(owner_phone, ''), '[^0-9]', '', 'g')) >= 10 then 1 else 0 end) / nullif(count(*), 0), 2) as pct_owner_phone_usable,
            round(100.0 * sum(case when length(regexp_replace(coalesce(applicant_phone, ''), '[^0-9]', '', 'g')) >= 10 then 1 else 0 end) / nullif(count(*), 0), 2) as pct_applicant_phone_usable
        from permit_latest
        """
    ).fetchone()
    snapshot_row = conn.execute(
        """
        select
            min(cast(file_month_start_date as date)) as min_snapshot_month,
            max(cast(file_month_start_date as date)) as max_snapshot_month,
            count(distinct cast(file_month_start_date as date)) as snapshot_months
        from main.combined_data
        where file_month_start_date is not null
          and file_month_start_date <> ''
          and file_month_start_date <> '\\N'
        """
    ).fetchone()
    return {
        "pct_job_value_present": quality_row[0],
        "pct_owner_email_usable": quality_row[1],
        "pct_applicant_email_usable": quality_row[2],
        "pct_owner_phone_usable": quality_row[3],
        "pct_applicant_phone_usable": quality_row[4],
        "min_snapshot_month": snapshot_row[0],
        "max_snapshot_month": snapshot_row[1],
        "snapshot_months": snapshot_row[2],
    }


@st.cache_data(show_spinner=False)
def fetch_activity_series(conn_str: str) -> pd.DataFrame:
    conn = get_connection(conn_str)
    df = conn.execute(
        LATEST_PERMIT_CTE
        + """
        select
            date_trunc('month', l.activity_date) as month,
            count(*) as permits_total,
            sum(case when p.segment in ('commercial_target', 'res_target') then 1 else 0 end) as target_permits,
            sum(case when p.segment in ('commercial_target', 'res_target') and p.lead_priority = 'High' then 1 else 0 end) as high_priority_target
        from main.permit_segments p
        join permit_latest l using (permit_number)
        where l.activity_date > date '1900-01-01'
        group by 1
        order by 1
        """
    ).fetchdf()
    return _lower_cols(df)


@st.cache_data(show_spinner=False)
def fetch_segment_mix(conn_str: str) -> pd.DataFrame:
    conn = get_connection(conn_str)
    df = conn.execute(
        """
        select
            segment,
            count(*) as permits,
            round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
        from main.permit_segments
        group by 1
        order by permits desc
        """
    ).fetchdf()
    return _lower_cols(df)


@st.cache_data(show_spinner=False)
def fetch_top_counties(conn_str: str) -> pd.DataFrame:
    conn = get_connection(conn_str)
    df = conn.execute(
        LATEST_PERMIT_CTE
        + """
        select
            l.county,
            count(*) as permits
        from main.permit_segments p
        join permit_latest l using (permit_number)
        where l.county <> ''
        group by 1
        order by permits desc
        """
    ).fetchdf()
    return _lower_cols(df)


@st.cache_data(show_spinner=False)
def fetch_opportunities(
    conn_str: str,
    counties: tuple[str, ...],
    jurisdictions: tuple[str, ...],
    statuses: tuple[str, ...],
    start_date: str,
    end_date: str,
    min_job_value: int,
    include_unknown_job_value: bool,
) -> pd.DataFrame:
    conn = get_connection(conn_str)
    county_sql, county_params = _in_filter("l.county", counties, lower_input=True, lower_column=True)
    juris_sql, juris_params = _in_filter(
        "l.jurisdiction", jurisdictions, lower_input=True, lower_column=True
    )
    status_sql, status_params = _in_filter("l.status", statuses)

    value_sql = ""
    params: list[Any] = [start_date, end_date] + county_params + juris_params + status_params
    if min_job_value > 0:
        if include_unknown_job_value:
            value_sql = " and (l.job_value is null or l.job_value >= ?)"
        else:
            value_sql = " and l.job_value >= ?"
        params.append(min_job_value)

    sql = (
        LATEST_PERMIT_CTE
        + f"""
        select
            p.permit_number,
            p.segment,
            p.lead_priority,
            case
                when l.status like '%PLAN%' or l.status like '%REVIEW%' then 'Plan / Review'
                when l.status like '%SUBMIT%' then 'Submitted'
                when l.status like '%ACTIVE%' then 'Active'
                when l.status like '%FINAL%' then 'Final'
                else 'Other'
            end as stage,
            l.status,
            l.activity_date,
            l.file_date,
            l.issue_date,
            l.county,
            l.jurisdiction,
            l.city,
            concat_ws(' ', cast(l.street_no as varchar), l.street) as address,
            l.zipcode,
            l.job_value,
            l.type,
            l.subtype,
            l.description,
            p.contractor_id,
            p.contractor_group_id,
            p.contractor_is_trade,
            l.owner_name,
            l.owner_email,
            l.owner_phone,
            l.applicant_name,
            l.applicant_email,
            l.applicant_phone
        from main.permit_segments p
        join permit_latest l using (permit_number)
        where p.segment in ('commercial_target', 'res_target')
          and l.activity_date between ? and ?
          {county_sql}
          {juris_sql}
          {status_sql}
          {value_sql}
        order by l.activity_date desc
        limit 5000
        """
    )
    df = conn.execute(sql, params).fetchdf()
    return _lower_cols(df)


@st.cache_data(show_spinner=False)
def fetch_competitor_pulse(
    conn_str: str,
    counties: tuple[str, ...],
    start_date: str,
    end_date: str,
    include_dormant: bool,
) -> pd.DataFrame:
    conn = get_connection(conn_str)
    county_sql, params = _in_filter("l.county", counties, lower_input=True, lower_column=True)

    base_sql = (
        LATEST_PERMIT_CTE
        + f"""
        ,recent as (
            select
                p.contractor_group_id,
                count(*) as recent_target_permits,
                max(l.activity_date) as last_activity
            from main.permit_segments p
            join permit_latest l using (permit_number)
            where p.segment in ('commercial_target', 'res_target')
              and p.contractor_group_id is not null
              and coalesce(p.contractor_is_trade, false) = false
              and l.activity_date between ? and ?
              {county_sql}
            group by 1
        )
        """
    )
    params = [start_date, end_date] + params

    if include_dormant:
        sql = (
            base_sql
            + """
            select
                coalesce(w.contractor_group_id, r.contractor_group_id) as contractor_group_id,
                coalesce(w.contractor_name, w.biz_name, w.biz_name_2, r.contractor_group_id) as contractor,
                coalesce(w.biz_name, w.biz_name_2, w.contractor_name) as biz_name,
                w.permit_count,
                w.top_permit_type,
                w.top_jurisdiction,
                w.top_city,
                w.primary_email,
                w.primary_phone,
                w.website,
                coalesce(r.recent_target_permits, 0) as recent_target_permits,
                r.last_activity
            from main.watchlist_gc w
            left join recent r using (contractor_group_id)
            order by recent_target_permits desc, w.permit_count desc
            """
        )
    else:
        sql = (
            base_sql
            + """
            select
                coalesce(w.contractor_group_id, r.contractor_group_id) as contractor_group_id,
                coalesce(w.contractor_name, w.biz_name, w.biz_name_2, r.contractor_group_id) as contractor,
                coalesce(w.biz_name, w.biz_name_2, w.contractor_name) as biz_name,
                w.permit_count,
                w.top_permit_type,
                w.top_jurisdiction,
                w.top_city,
                w.primary_email,
                w.primary_phone,
                w.website,
                r.recent_target_permits,
                r.last_activity
            from recent r
            left join main.watchlist_gc w using (contractor_group_id)
            order by r.recent_target_permits desc, w.permit_count desc
            """
        )

    df = conn.execute(sql, params).fetchdf()
    return _lower_cols(df)


@st.cache_data(show_spinner=False)
def fetch_property_summary(conn_str: str) -> dict[str, Any]:
    conn = get_connection(conn_str)
    row = conn.execute(
        """
        select
            count(*) as properties,
            sum(case when permit_count >= 2 then 1 else 0 end) as properties_2plus,
            sum(case when permit_count >= 3 then 1 else 0 end) as properties_3plus,
            sum(case when high_permit_count >= 1 then 1 else 0 end) as properties_with_high,
            round(avg(permit_count), 2) as avg_permits_per_property,
            max(permit_count) as max_permits_per_property
        from main.property_permits
        """
    ).fetchone()
    keys = [
        "properties",
        "properties_2plus",
        "properties_3plus",
        "properties_with_high",
        "avg_permits_per_property",
        "max_permits_per_property",
    ]
    out = dict(zip(keys, row))
    out["pct_2plus"] = round(100.0 * out["properties_2plus"] / max(out["properties"], 1), 2)
    out["pct_3plus"] = round(100.0 * out["properties_3plus"] / max(out["properties"], 1), 2)
    return out


@st.cache_data(show_spinner=False)
def fetch_property_buckets(conn_str: str) -> pd.DataFrame:
    conn = get_connection(conn_str)
    df = conn.execute(
        """
        select
            case
                when permit_count >= 10 then '10+'
                when permit_count >= 5 then '5-9'
                when permit_count = 4 then '4'
                when permit_count = 3 then '3'
                when permit_count = 2 then '2'
                else '1'
            end as permit_bucket,
            case
                when permit_count >= 10 then 6
                when permit_count >= 5 then 5
                when permit_count = 4 then 4
                when permit_count = 3 then 3
                when permit_count = 2 then 2
                else 1
            end as bucket_order,
            count(*) as properties
        from main.property_permits
        group by 1, 2
        order by 2
        """
    ).fetchdf()
    return _lower_cols(df)


@st.cache_data(show_spinner=False)
def fetch_property_jurisdiction_rollup(conn_str: str, top_n: int) -> pd.DataFrame:
    conn = get_connection(conn_str)
    df = conn.execute(
        """
        select
            jurisdiction,
            count(*) as properties,
            sum(permit_count) as permits,
            sum(high_permit_count) as high_permits
        from main.property_permits
        where coalesce(jurisdiction, '') <> ''
        group by 1
        order by permits desc
        limit ?
        """,
        [top_n],
    ).fetchdf()
    return _lower_cols(df)


@st.cache_data(show_spinner=False)
def fetch_top_properties(conn_str: str, top_n: int, min_permits: int) -> pd.DataFrame:
    conn = get_connection(conn_str)
    df = conn.execute(
        """
        select
            property_uid,
            jurisdiction,
            city,
            concat_ws(' ', cast(street_no as varchar), street) as address,
            zipcode,
            permit_count,
            high_permit_count,
            first_file_date,
            last_file_date
        from main.property_permits
        where permit_count >= ?
        order by permit_count desc, last_file_date desc
        limit ?
        """,
        [min_permits, top_n],
    ).fetchdf()
    return _lower_cols(df)


@st.cache_data(show_spinner=False)
def fetch_watchlist(conn_str: str, list_type: str) -> pd.DataFrame:
    conn = get_connection(conn_str)
    if list_type == "gc":
        sql = """
        select
            contractor_group_id,
            contractor_name,
            coalesce(biz_name, biz_name_2, contractor_name) as biz_name,
            permit_count,
            watchlist_top20,
            top_permit_type,
            top_jurisdiction,
            top_city,
            primary_email,
            email,
            primary_phone,
            phone,
            website
        from main.watchlist_gc
        order by permit_count desc
        """
    else:
        sql = """
        with base as (
            select
                contractor_group_id,
                contractor_name,
                coalesce(biz_name, biz_name_2, contractor_name) as biz_name,
                permit_count,
                watchlist_top20,
                top_jurisdiction,
                top_city,
                primary_email,
                email,
                primary_phone,
                phone,
                website,
                lower(
                    concat_ws(
                        ' ',
                        coalesce(contractor_type, ''),
                        coalesce(classification, ''),
                        coalesce(primary_industry, ''),
                        coalesce(naics, ''),
                        coalesce(biz_type, ''),
                        coalesce(contractor_name, ''),
                        coalesce(biz_name, ''),
                        coalesce(biz_name_2, '')
                    )
                ) as trade_text
            from main.watchlist_trade
        )
        select
            contractor_group_id,
            contractor_name,
            biz_name,
            permit_count,
            watchlist_top20,
            null::varchar as top_permit_type,
            top_jurisdiction,
            top_city,
            primary_email,
            email,
            primary_phone,
            phone,
            website,
            case
                when regexp_matches(trade_text, '(plumb|pipefitt|drain|c-?36)') then 'Plumbing'
                when regexp_matches(trade_text, '(electric|electrical|c-?10|low voltage|ev charger)') then 'Electrical'
                when regexp_matches(trade_text, '(hvac|heating|air conditioning|heat pump|ventilation|furnace|c-?20|c-?43)') then 'HVAC'
                when regexp_matches(trade_text, '(roof|roofing|c-?39)') then 'Roofing'
                when regexp_matches(trade_text, '(solar|photovoltaic|\\bpv\\b|c-?46)') then 'Solar'
                when regexp_matches(trade_text, '(paint|coating|c-?33)') then 'Painting'
                when regexp_matches(trade_text, '(concrete|masonry|stucco|c-?8|c-?29|c-?61)') then 'Concrete/Masonry'
                when regexp_matches(trade_text, '(landscape|lawn|tree|arbor|irrigation|c-?27)') then 'Landscaping'
                when regexp_matches(trade_text, '(excavat|grading|earthwork|demolition|c-?12|c-?21)') then 'Excavation/Grading'
                else 'Other/Unknown'
            end as trade_type_group
        from base
        order by permit_count desc
        """
    return _lower_cols(conn.execute(sql).fetchdf())


@st.cache_data(show_spinner=False)
def fetch_watchlist_quality(conn_str: str, list_type: str) -> dict[str, Any]:
    conn = get_connection(conn_str)
    table_name = "main.watchlist_gc" if list_type == "gc" else "main.watchlist_trade"
    row = conn.execute(
        f"""
        select
            count(*) as contractors,
            sum(case when primary_email ilike '%@%' then 1 else 0 end) as email_like,
            sum(case when length(regexp_replace(coalesce(primary_phone, ''), '[^0-9]', '', 'g')) >= 10 then 1 else 0 end) as phone_like,
            sum(case when regexp_matches(lower(coalesce(website, '')), '^[a-z0-9.-]+\\.[a-z]{{2,}}$') then 1 else 0 end) as domain_like
        from {table_name}
        """
    ).fetchone()
    contractors, email_like, phone_like, domain_like = row
    return {
        "contractors": contractors,
        "email_like": email_like,
        "phone_like": phone_like,
        "domain_like": domain_like,
        "pct_email_like": round(100.0 * email_like / max(contractors, 1), 2),
        "pct_phone_like": round(100.0 * phone_like / max(contractors, 1), 2),
        "pct_domain_like": round(100.0 * domain_like / max(contractors, 1), 2),
    }


@st.cache_data(show_spinner=False)
def fetch_mom_change_summary(conn_str: str) -> pd.DataFrame:
    conn = get_connection(conn_str)
    return _lower_cols(
        conn.execute(
            """
            select *
            from main.permit_mom_change_counts
            order by curr_month desc
            """
        ).fetchdf()
    )


@st.cache_data(show_spinner=False)
def fetch_mom_change_details(conn_str: str) -> pd.DataFrame:
    conn = get_connection(conn_str)
    df = conn.execute(
        LATEST_PERMIT_CTE
        + """
        select
            m.permit_number,
            m.current_snapshot_month,
            m.prev_snapshot_month,
            l.county,
            l.jurisdiction,
            l.city,
            concat_ws(' ', cast(l.street_no as varchar), l.street) as address,
            l.zipcode,
            l.activity_date,
            m.is_new_this_month,
            m.status_changed,
            m.job_value_changed,
            m.area_changed,
            m.contractor_changed,
            m.date_filled,
            m.current_status,
            m.prev_status,
            m.current_job_value,
            m.prev_job_value,
            m.current_issue_date,
            m.prev_issue_date,
            concat_ws(
                '; ',
                case when m.is_new_this_month then 'New permit' end,
                case when m.status_changed then 'Status changed' end,
                case when m.job_value_changed then 'Job value changed' end,
                case when m.area_changed then 'Area changed' end,
                case when m.contractor_changed then 'Contractor changed' end,
                case when m.date_filled then 'Issue date filled' end
            ) as change_reason
        from main.permit_mom_changes m
        left join permit_latest l using (permit_number)
        order by l.activity_date desc nulls last, m.permit_number
        """
    ).fetchdf()
    return _lower_cols(df)


def render_watchlist_tab(conn_str: str, list_type: str, title: str, download_name: str) -> None:
    quality = fetch_watchlist_quality(conn_str, list_type)
    data = fetch_watchlist(conn_str, list_type)

    st.markdown(
        f"""
        **How to use this section**
        - Use search to quickly isolate contractor groups by name, city, jurisdiction, or permit type.
        - Use Top N for chart focus, then work from the full table and export CSV for outreach.
        """
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"{title} Contractors", f"{quality['contractors']:,}")
    c2.metric("Email coverage", f"{quality['pct_email_like']:.1f}%")
    c3.metric("Phone coverage", f"{quality['pct_phone_like']:.1f}%")
    c4.metric("Website domain coverage", f"{quality['pct_domain_like']:.1f}%")

    if list_type == "trade" and "trade_type_group" in data.columns:
        trade_options = sorted([t for t in data["trade_type_group"].dropna().unique().tolist() if t])
        selected_trade_types = st.multiselect(
            "Trade type filter",
            options=trade_options,
            default=[],
            key=f"{list_type}_trade_types",
        )
        if selected_trade_types:
            data = data[data["trade_type_group"].isin(selected_trade_types)]

    search = st.text_input("Search contractors", key=f"{list_type}_search")
    top_n = st.slider("Show top N in chart", 5, 50, 20, 1, key=f"{list_type}_topn")

    if search.strip():
        mask = (
            data["contractor_name"].str.contains(search, case=False, na=False)
            | data["biz_name"].str.contains(search, case=False, na=False)
            | data["top_city"].str.contains(search, case=False, na=False)
            | data["top_jurisdiction"].str.contains(search, case=False, na=False)
            | data["top_permit_type"].str.contains(search, case=False, na=False)
        )
        data = data[mask]

    view_df = data.head(top_n)
    if view_df.empty:
        st.info("No contractors match the current filter.")
    else:
        chart = (
            alt.Chart(view_df)
            .mark_bar()
            .encode(
                y=alt.Y("contractor_name:N", sort="-x", title="Contractor"),
                x=alt.X("permit_count:Q", title="Permits"),
                tooltip=[
                    "contractor_name",
                    "biz_name",
                    "permit_count",
                    "top_permit_type",
                    "top_jurisdiction",
                    "top_city",
                ],
            )
            .properties(height=420, title=f"{title} by permit volume")
        )
        _render_altair_chart(chart)

    column_config: dict[str, Any] = {
        "permit_count": st.column_config.NumberColumn("Permit count", format="%d"),
        "watchlist_top20": st.column_config.CheckboxColumn("Top 20"),
    }
    if list_type == "trade" and "trade_type_group" in data.columns:
        column_config["trade_type_group"] = st.column_config.TextColumn("Trade type")

    st.dataframe(data, width="stretch", column_config=column_config)
    st.download_button(
        f"Download {title} CSV",
        data.to_csv(index=False).encode(),
        file_name=download_name,
    )


st.title("Armony Client Intelligence")
st.write(
    "Client-ready permit intelligence built from `reporting_db`: opportunities, competitor motion, property hotspots, and watchlists."
)

env_md_token = os.environ.get("MOTHERDUCK_TOKEN", "").strip()
env_conn = os.environ.get("MOTHERDUCK_DSN", "").strip()
default_conn = env_conn or (f"md:?motherduck_token={env_md_token}" if env_md_token else ":memory:")

with st.sidebar:
    st.header("Connection")
    conn_str = st.text_input("Warehouse connection string", value=default_conn)
    if st.button("Refresh cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

if not conn_str:
    st.stop()

try:
    _ = get_connection(conn_str)
except Exception as exc:  # pragma: no cover
    st.error(f"Could not connect to the warehouse: {exc}")
    st.stop()

info = connection_info(conn_str)
st.success("Connected")
st.caption(f"Current database: `{info['current_database']}`")

required_views = {
    "combined_data",
    "permit_segments",
    "property_permits",
    "watchlist_gc",
    "watchlist_trade",
    "permit_mom_change_counts",
}
available_views = reporting_views(conn_str)
missing_views = sorted(required_views - available_views)
if missing_views:
    st.error(
        "Missing required reporting views in `reporting_db.main`: "
        + ", ".join(missing_views)
        + ". Update the connection or rebuild the reporting views."
    )
    st.stop()

county_options = list_counties(conn_str)
default_counties = tuple(c for c in county_options if c.lower() in {"marin", "sonoma", "napa"}) or tuple(
    county_options
)

overview_tab, opp_tab, search_tab, comp_tab, property_tab, watch_tab, mom_tab = st.tabs(
    [
        "Overview",
        "Opportunities",
        "Global Search",
        "Competitor Pulse",
        "Property Hotspots",
        "Watchlists",
        "Market Change",
    ]
)

with overview_tab:
    st.markdown(
        """
        **How to use this tab**
        - Start here for the broad funnel: total permits, target share, high-priority volume, and active recent opportunities.
        - Use the trend and mix charts to align sales focus on where current permit activity is concentrated.
        """
    )

    min_activity_date, max_activity_date = fetch_target_activity_bounds(conn_str)
    ov_default = _default_date_range(min_activity_date, max_activity_date, days=180)
    ov_start_date, ov_end_date = st.slider(
        "Target activity date range",
        min_value=min_activity_date,
        max_value=max_activity_date,
        value=ov_default,
        format="YYYY-MM-DD",
        key="ov_date_range",
    )
    metrics = fetch_overview_metrics(conn_str, ov_start_date.isoformat(), ov_end_date.isoformat())
    quality = fetch_data_quality_metrics(conn_str)

    m1, m2, m3, m4, m5 = st.columns(5)
    pct_target = 100.0 * metrics["target_permits"] / max(metrics["permits_total"], 1)
    m1.metric("Permits (deduped)", f"{metrics['permits_total']:,}")
    m2.metric("Target permits", f"{metrics['target_permits']:,}", delta=f"{pct_target:.1f}% of all")
    m3.metric("High-priority target", f"{metrics['high_priority_target']:,}")
    m4.metric("Active in range", f"{metrics['active_target_lookback']:,}")
    m5.metric("Counties covered", f"{metrics['counties_covered']:,}")
    st.markdown(
        """
        **Metric definitions**
        - `permits_total`: Total deduplicated permits (one row per permit number).
        - `target_permits`: Permits classified as `commercial_target` or `res_target`.
        - `high_priority_target`: Subset of `target_permits` (the permits we care about) where `lead_priority = 'High'`.
        """
    )

    q1, q2, q3 = st.columns(3)
    q1.metric("Job value coverage", f"{quality['pct_job_value_present']:.1f}%")
    q2.metric("Owner email usable", f"{quality['pct_owner_email_usable']:.1f}%")
    q3.metric("Applicant email usable", f"{quality['pct_applicant_email_usable']:.1f}%")
    st.caption(
        f"Selected target range: {ov_start_date.isoformat()} to {ov_end_date.isoformat()} | "
        f"Snapshot months available: {quality['snapshot_months']} "
        f"({quality['min_snapshot_month']} to {quality['max_snapshot_month']})"
    )

    activity_df = fetch_activity_series(conn_str)
    if not activity_df.empty:
        trend_df = activity_df.melt(
            id_vars=["month"],
            value_vars=["permits_total", "target_permits", "high_priority_target"],
            var_name="series",
            value_name="permits",
        )
        trend_chart = (
            alt.Chart(trend_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("permits:Q", title="Permits"),
                color=alt.Color("series:N", title="Series"),
                tooltip=["month:T", "series:N", "permits:Q"],
            )
            .properties(height=360, title="Permit trend (deduped)")
        )
        _render_altair_chart(trend_chart)

    left, right = st.columns(2)
    with left:
        seg_df = fetch_segment_mix(conn_str)
        seg_chart = (
            alt.Chart(seg_df)
            .mark_bar()
            .encode(
                y=alt.Y("segment:N", sort="-x", title="Segment"),
                x=alt.X("permits:Q", title="Permits"),
                tooltip=["segment", "permits", "pct"],
            )
            .properties(height=360, title="Segment mix")
        )
        _render_altair_chart(seg_chart)

    with right:
        county_df = fetch_top_counties(conn_str).head(10)
        county_chart = (
            alt.Chart(county_df)
            .mark_bar(color="#4b8bff")
            .encode(
                y=alt.Y("county:N", sort="-x", title="County"),
                x=alt.X("permits:Q", title="Permits"),
                tooltip=["county", "permits"],
            )
            .properties(height=360, title="Top counties by permits")
        )
        _render_altair_chart(county_chart)


with opp_tab:
    st.markdown(
        """
        **How to use this tab**
        - Filter to the exact target market window your client team is selling into.
        - Use the stage/jurisdiction/type charts for prioritization, then export the table for outreach.
        """
    )

    jurisdictions = list_target_jurisdictions(conn_str)
    statuses = list_target_statuses(conn_str)
    min_activity_date, max_activity_date = fetch_target_activity_bounds(conn_str)
    opp_default = _default_date_range(min_activity_date, max_activity_date, days=180)

    f1, f2, f3 = st.columns([1.6, 1.2, 1])
    with f1:
        sel_counties = st.multiselect("Counties", county_options, default=list(default_counties))
        sel_jurisdictions = st.multiselect("Jurisdictions", jurisdictions, default=[])
    with f2:
        sel_statuses = st.multiselect(
            "Statuses",
            statuses,
            default=[s for s in statuses if s in {"ACTIVE", "IN_REVIEW", "PLAN REVIEW", "SUBMITTED"}],
        )
        opp_start_date, opp_end_date = st.slider(
            "Activity date range",
            min_value=min_activity_date,
            max_value=max_activity_date,
            value=opp_default,
            format="YYYY-MM-DD",
            key="opp_date_range",
        )
    with f3:
        min_job_value = st.number_input("Min job value", min_value=0, value=150000, step=25000)
        include_unknown_value = st.toggle("Include missing job value", value=True)

    opp_df = fetch_opportunities(
        conn_str,
        tuple(sel_counties),
        tuple(sel_jurisdictions),
        tuple(sel_statuses),
        opp_start_date.isoformat(),
        opp_end_date.isoformat(),
        int(min_job_value),
        bool(include_unknown_value),
    )

    st.write(f"{len(opp_df):,} opportunities")
    if opp_df.empty:
        st.info("No opportunities match current filters.")
    else:
        contractor_group_series = opp_df["contractor_group_id"]
        trade_series = opp_df["contractor_is_trade"].fillna(False)
        o1, o2, o3, o4 = st.columns(4)
        o1.metric("High priority", f"{(opp_df['lead_priority'] == 'High').sum():,}")
        o2.metric("Plan/Review or Submitted", f"{opp_df['stage'].isin(['Plan / Review', 'Submitted']).sum():,}")
        o3.metric("GC missing proxy", f"{(contractor_group_series.isna() | trade_series).sum():,}")
        o4.metric("With job value", f"{opp_df['job_value'].notna().sum():,}")

        left, mid, right = st.columns(3)
        with left:
            stage_df = opp_df.groupby("stage", as_index=False)["permit_number"].count()
            stage_chart = (
                alt.Chart(stage_df)
                .mark_bar()
                .encode(
                    x=alt.X("stage:N", sort="-y", title="Stage"),
                    y=alt.Y("permit_number:Q", title="Permits"),
                    tooltip=["stage", "permit_number"],
                )
                .properties(height=300, title="Stage mix")
            )
            _render_altair_chart(stage_chart)
        with mid:
            juris_df = (
                opp_df.groupby("jurisdiction", as_index=False)["permit_number"]
                .count()
                .sort_values("permit_number", ascending=False)
                .head(10)
            )
            juris_chart = (
                alt.Chart(juris_df)
                .mark_bar(color="#4b8bff")
                .encode(
                    y=alt.Y("jurisdiction:N", sort="-x", title="Jurisdiction"),
                    x=alt.X("permit_number:Q", title="Permits"),
                    tooltip=["jurisdiction", "permit_number"],
                )
                .properties(height=300, title="Top jurisdictions")
            )
            _render_altair_chart(juris_chart)
        with right:
            type_df = (
                opp_df.groupby("type", as_index=False)["permit_number"]
                .count()
                .sort_values("permit_number", ascending=False)
                .head(10)
            )
            type_chart = (
                alt.Chart(type_df)
                .mark_bar(color="#2ca58d")
                .encode(
                    y=alt.Y("type:N", sort="-x", title="Type"),
                    x=alt.X("permit_number:Q", title="Permits"),
                    tooltip=["type", "permit_number"],
                )
                .properties(height=300, title="Top permit types")
            )
            _render_altair_chart(type_chart)

        st.dataframe(
            opp_df,
            width="stretch",
            column_config={
                "job_value": st.column_config.NumberColumn("Job value", format="dollar", step=1),
                "activity_date": st.column_config.DateColumn("Activity date"),
                "file_date": st.column_config.DateColumn("File date"),
                "issue_date": st.column_config.DateColumn("Issue date"),
                "permit_number": st.column_config.TextColumn("Permit #"),
            },
        )
        st.download_button(
            "Download opportunities CSV",
            opp_df.to_csv(index=False).encode(),
            file_name="client_opportunities.csv",
        )


with search_tab:
    st.markdown(
        """
        **How to use this tab**
        - Use one search bar to find permits by address, applicant, owner, or permit number.
        - Apply the activity date range for time-boxed lookups, then export matching rows to CSV.
        """
    )
    search_min_date, search_max_date = fetch_all_activity_bounds(conn_str)
    search_default = _default_date_range(search_min_date, search_max_date, days=365)
    if "global_search_request" not in st.session_state:
        st.session_state["global_search_request"] = None

    with st.form("global_search_form"):
        s1, s2 = st.columns([1.8, 1])
        with s1:
            search_text = st.text_input(
                "Search",
                placeholder="Try an address, applicant name, owner name, or permit number",
            )
        with s2:
            search_mode = st.selectbox(
                "Search scope",
                options=["All fields", "Address", "Applicant", "Owner", "Permit #"],
                index=0,
            )

        search_start_date, search_end_date = st.slider(
            "Activity date range",
            min_value=search_min_date,
            max_value=search_max_date,
            value=search_default,
            format="YYYY-MM-DD",
            key="global_search_date_range",
        )
        result_limit = st.slider("Max rows", 50, 5000, 500, 50, key="global_search_limit")
        run_search = st.form_submit_button("Search permits", use_container_width=True)

    if run_search:
        term = search_text.strip()
        if len(term) < 2:
            st.warning("Enter at least 2 characters to run search.")
            st.session_state["global_search_request"] = None
        else:
            st.session_state["global_search_request"] = {
                "search_text": term,
                "search_mode": search_mode,
                "start_date": search_start_date.isoformat(),
                "end_date": search_end_date.isoformat(),
                "result_limit": int(result_limit),
            }

    request = st.session_state.get("global_search_request")
    if request:
        search_df = fetch_global_search_results(
            conn_str,
            request["search_text"],
            request["search_mode"],
            request["start_date"],
            request["end_date"],
            int(request["result_limit"]),
        )
        st.write(f"{len(search_df):,} permits matched")
        if search_df.empty:
            st.info("No permits match the current search and date range.")
        else:
            g1, g2, g3 = st.columns(3)
            g1.metric("Unique jurisdictions", f"{search_df['jurisdiction'].nunique(dropna=True):,}")
            g2.metric("With applicant email", f"{search_df['applicant_email'].str.contains('@', na=False).sum():,}")
            g3.metric("With owner email", f"{search_df['owner_email'].str.contains('@', na=False).sum():,}")

            st.dataframe(
                search_df,
                width="stretch",
                column_config={
                    "permit_number": st.column_config.TextColumn("Permit #"),
                    "activity_date": st.column_config.DateColumn("Activity date"),
                    "file_date": st.column_config.DateColumn("File date"),
                    "issue_date": st.column_config.DateColumn("Issue date"),
                    "job_value": st.column_config.NumberColumn("Job value", format="dollar", step=1),
                    "contractor_is_trade": st.column_config.CheckboxColumn("Trade"),
                },
            )
            st.download_button(
                "Download search results CSV",
                search_df.to_csv(index=False).encode(),
                file_name="permit_search_results.csv",
            )
    else:
        st.caption("Run a search to view permit matches.")


with comp_tab:
    st.markdown(
        """
        **How to use this tab**
        - Track which GC groups are most active on target permits in your selected window.
        - Compare recent motion against each contractor's all-time permit footprint.
        """
    )

    min_activity_date, max_activity_date = fetch_target_activity_bounds(conn_str)
    comp_default = _default_date_range(min_activity_date, max_activity_date, days=180)
    c1, c2, c3 = st.columns([1.5, 1.2, 1])
    with c1:
        comp_counties = st.multiselect("Counties", county_options, default=list(default_counties), key="comp_counties")
    with c2:
        comp_start_date, comp_end_date = st.slider(
            "Activity date range",
            min_value=min_activity_date,
            max_value=max_activity_date,
            value=comp_default,
            format="YYYY-MM-DD",
            key="comp_date_range",
        )
    with c3:
        include_dormant = st.toggle("Include dormant watchlist groups", value=False)

    comp_df = fetch_competitor_pulse(
        conn_str,
        tuple(comp_counties),
        comp_start_date.isoformat(),
        comp_end_date.isoformat(),
        bool(include_dormant),
    )
    active_comp_df = comp_df[comp_df["recent_target_permits"] > 0]

    m1, m2, m3 = st.columns(3)
    m1.metric("Groups shown", f"{len(comp_df):,}")
    m2.metric("Active in window", f"{len(active_comp_df):,}")
    top_recent = active_comp_df.iloc[0]["contractor"] if not active_comp_df.empty else "None"
    m3.metric("Top recent contractor", str(top_recent))

    top_n = st.slider("Show top N in charts", 5, 40, 15, 1, key="comp_topn")
    chart_df = comp_df.nlargest(top_n, "recent_target_permits") if not comp_df.empty else comp_df
    if chart_df.empty:
        st.info("No competitor rows match current filters.")
    else:
        left, right = st.columns(2)
        with left:
            recent_chart = (
                alt.Chart(chart_df)
                .mark_bar()
                .encode(
                    y=alt.Y("contractor:N", sort="-x", title="Contractor"),
                    x=alt.X(
                        "recent_target_permits:Q",
                        title=f"Target permits ({comp_start_date.isoformat()} to {comp_end_date.isoformat()})",
                    ),
                    tooltip=[
                        "contractor",
                        "biz_name",
                        "recent_target_permits",
                        "permit_count",
                        "top_permit_type",
                        "top_jurisdiction",
                    ],
                )
                .properties(height=420, title="Recent target activity")
            )
            _render_altair_chart(recent_chart)
        with right:
            total_chart = (
                alt.Chart(chart_df)
                .mark_bar(color="#4b8bff")
                .encode(
                    y=alt.Y("contractor:N", sort="-x", title="Contractor"),
                    x=alt.X("permit_count:Q", title="All-time permits"),
                    tooltip=[
                        "contractor",
                        "biz_name",
                        "permit_count",
                        "top_permit_type",
                        "top_jurisdiction",
                    ],
                )
                .properties(height=420, title="Overall permit footprint")
            )
            _render_altair_chart(total_chart)

    st.dataframe(
        comp_df,
        width="stretch",
        column_config={
            "last_activity": st.column_config.DateColumn("Last activity"),
            "permit_count": st.column_config.NumberColumn("All-time permits", format="%d"),
            "recent_target_permits": st.column_config.NumberColumn("Recent target permits", format="%d"),
        },
    )
    st.download_button(
        "Download competitor pulse CSV",
        comp_df.to_csv(index=False).encode(),
        file_name="competitor_pulse.csv",
    )


with property_tab:
    st.markdown(
        """
        **How to use this tab**
        - Focus on repeat-permit properties and jurisdictions with concentrated permit recurrence.
        - Use top property export to feed account-based field strategy.
        """
    )

    prop_summary = fetch_property_summary(conn_str)
    p1, p2, p3, p4, p5 = st.columns(5)
    p1.metric("Properties", f"{prop_summary['properties']:,}")
    p2.metric("2+ permits", f"{prop_summary['properties_2plus']:,}", delta=f"{prop_summary['pct_2plus']:.1f}%")
    p3.metric("3+ permits", f"{prop_summary['properties_3plus']:,}", delta=f"{prop_summary['pct_3plus']:.1f}%")
    p4.metric("Avg permits/property", f"{prop_summary['avg_permits_per_property']:.2f}")
    p5.metric("Max permits/property", f"{prop_summary['max_permits_per_property']:,}")

    left, right = st.columns(2)
    with left:
        bucket_df = fetch_property_buckets(conn_str)
        bucket_chart = (
            alt.Chart(bucket_df)
            .mark_bar()
            .encode(
                x=alt.X(
                    "permit_bucket:N",
                    sort=["1", "2", "3", "4", "5-9", "10+"],
                    title="Permits per property",
                ),
                y=alt.Y("properties:Q", title="Properties"),
                tooltip=["permit_bucket", "properties"],
            )
            .properties(height=320, title="Property repeat-permit distribution")
        )
        _render_altair_chart(bucket_chart)
    with right:
        top_n_j = st.slider("Top jurisdictions", 5, 20, 10, 1)
        prop_j_df = fetch_property_jurisdiction_rollup(conn_str, top_n_j)
        prop_j_chart = (
            alt.Chart(prop_j_df)
            .mark_bar(color="#2ca58d")
            .encode(
                y=alt.Y("jurisdiction:N", sort="-x", title="Jurisdiction"),
                x=alt.X("permits:Q", title="Permits"),
                tooltip=["jurisdiction", "permits", "properties", "high_permits"],
            )
            .properties(height=320, title="Top jurisdictions by repeat permits")
        )
        _render_altair_chart(prop_j_chart)

    t1, t2 = st.columns(2)
    with t1:
        min_permits = st.slider("Minimum permits/property", 1, 10, 3, 1)
    with t2:
        top_n_props = st.slider("Top properties to show", 10, 100, 40, 5)
    top_props_df = fetch_top_properties(conn_str, int(top_n_props), int(min_permits))

    st.dataframe(
        top_props_df,
        width="stretch",
        column_config={
            "address": st.column_config.TextColumn("Address"),
            "zipcode": st.column_config.TextColumn("Zip"),
            "permit_count": st.column_config.NumberColumn("Permit count", format="%d"),
            "high_permit_count": st.column_config.NumberColumn("High-priority permits", format="%d"),
            "first_file_date": st.column_config.DateColumn("First file date"),
            "last_file_date": st.column_config.DateColumn("Last file date"),
        },
    )
    st.download_button(
        "Download top properties CSV",
        top_props_df.to_csv(index=False).encode(),
        file_name="property_hotspots.csv",
    )


with watch_tab:
    st.markdown(
        """
        **How to use this tab**
        - **GC Watchlist**: General contractors (non-trade focus) with top permit type context for competitive positioning.
        - **Trade Watchlist**: Specialty trade contractors (HVAC, roofing, solar, plumbing, electrical, etc.) for partner/sub and trade-competition tracking.
        - Work both lists separately: use chart for concentration, table for contacts, then CSV for execution.
        """
    )
    gc_watch, trade_watch = st.tabs(["GC Watchlist", "Trade Watchlist"])
    with gc_watch:
        render_watchlist_tab(conn_str, "gc", "GC Watchlist", "watchlist_gc.csv")
    with trade_watch:
        render_watchlist_tab(conn_str, "trade", "Trade Watchlist", "watchlist_trade.csv")


with mom_tab:
    st.markdown(
        """
        **How to use this tab**
        - Treat this as top-line change telemetry between the two latest snapshot months.
        - Metrics compare the latest snapshot month to the prior month using deduped permit rows.
        """
    )
    quality = fetch_data_quality_metrics(conn_str)
    if quality["snapshot_months"] < 2:
        st.warning(
            "Month-over-month analysis needs at least 2 snapshot months. "
            f"Current snapshot coverage: {quality['snapshot_months']} month(s) "
            f"({quality['min_snapshot_month']} to {quality['max_snapshot_month']})."
        )
    else:
        st.info("Month-over-month values are based on latest-vs-prior snapshot comparison in reporting views.")

        mom_df = fetch_mom_change_summary(conn_str)
        if mom_df.empty:
            st.info("No month-over-month change rows available.")
        else:
            row = mom_df.iloc[0]
            st.caption(f"Current month: {row['curr_month']} | Previous month: {row['prev_month']}")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Current permit count", f"{int(row['curr_count']):,}")
            m2.metric("Previous permit count", f"{int(row['prev_count']):,}")
            m3.metric("New permits", f"{int(row['new_permits']):,}")
            m4.metric("Missing permits", f"{int(row['missing_permits']):,}")

            change_fields = [
                "status_changed",
                "type_changed",
                "subtype_changed",
                "description_changed",
                "issue_date_changed",
                "final_date_changed",
                "start_date_changed",
                "job_value_changed",
                "property_building_area_changed",
                "property_type_changed",
                "property_type_detail_changed",
                "property_story_count_changed",
                "property_unit_count_changed",
                "contractor_id_changed",
                "contractor_group_id_changed",
                "primary_email_changed",
                "primary_phone_changed",
            ]
            change_df = pd.DataFrame(
                {"field": change_fields, "count": [int(row[f]) for f in change_fields]}
            ).sort_values("count", ascending=False)

            change_chart = (
                alt.Chart(change_df.head(12))
                .mark_bar(color="#4b8bff")
                .encode(
                    y=alt.Y("field:N", sort="-x", title="Changed field"),
                    x=alt.X("count:Q", title="Permits changed"),
                    tooltip=["field", "count"],
                )
                .properties(height=420, title="Top changed fields (latest MoM)")
            )
            _render_altair_chart(change_chart)
            st.dataframe(mom_df, width="stretch")

            st.markdown("**Permit-level detail**")
            detail_df = fetch_mom_change_details(conn_str)
            detail_options = [
                "New permit",
                "Status changed",
                "Job value changed",
                "Area changed",
                "Contractor changed",
                "Issue date filled",
            ]
            selected_detail_types = st.multiselect(
                "Detail filter (any selected type)",
                options=detail_options,
                default=["New permit", "Status changed", "Contractor changed"],
            )
            detail_limit = st.slider("Detail rows to display", 50, 5000, 500, 50)
            detail_search = st.text_input("Search permit / address / jurisdiction", key="mom_detail_search")

            if not detail_df.empty:
                masks = []
                if "New permit" in selected_detail_types:
                    masks.append(detail_df["is_new_this_month"])
                if "Status changed" in selected_detail_types:
                    masks.append(detail_df["status_changed"])
                if "Job value changed" in selected_detail_types:
                    masks.append(detail_df["job_value_changed"])
                if "Area changed" in selected_detail_types:
                    masks.append(detail_df["area_changed"])
                if "Contractor changed" in selected_detail_types:
                    masks.append(detail_df["contractor_changed"])
                if "Issue date filled" in selected_detail_types:
                    masks.append(detail_df["date_filled"])
                if masks:
                    detail_mask = masks[0]
                    for m in masks[1:]:
                        detail_mask = detail_mask | m
                    detail_df = detail_df[detail_mask]

                if detail_search.strip():
                    s = detail_search.strip()
                    text_mask = (
                        detail_df["permit_number"].str.contains(s, case=False, na=False)
                        | detail_df["address"].str.contains(s, case=False, na=False)
                        | detail_df["jurisdiction"].str.contains(s, case=False, na=False)
                        | detail_df["change_reason"].str.contains(s, case=False, na=False)
                    )
                    detail_df = detail_df[text_mask]

                detail_df = detail_df.head(detail_limit)

            st.write(f"{len(detail_df):,} detail rows")
            st.dataframe(
                detail_df,
                width="stretch",
                column_config={
                    "activity_date": st.column_config.DateColumn("Activity date"),
                    "current_snapshot_month": st.column_config.DateColumn("Current snapshot"),
                    "prev_snapshot_month": st.column_config.DateColumn("Previous snapshot"),
                    "current_job_value": st.column_config.NumberColumn(
                        "Current job value",
                        format="dollar",
                        step=1,
                    ),
                    "prev_job_value": st.column_config.NumberColumn(
                        "Previous job value",
                        format="dollar",
                        step=1,
                    ),
                    "current_issue_date": st.column_config.DateColumn("Current issue date"),
                    "prev_issue_date": st.column_config.DateColumn("Previous issue date"),
                },
            )
            st.download_button(
                "Download MoM detail CSV",
                detail_df.to_csv(index=False).encode(),
                file_name="permit_mom_changes_detail.csv",
            )
