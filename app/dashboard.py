"""NJ Transit Reliability — interactive console over the gold marts.

Mirrors the pipeline's own logic end to end:
  * silver grain is one row per trip-stop (``stg_stop_performance``)
  * ``fct_stop_performance`` derives ``weather_condition`` from
    precipitation / wind exactly as the dbt model does
    (precip >= 2.5mm -> rain, precip > 0 -> light_precip,
     wind >= 40 km/h -> high_wind, else clear)
  * a stop is on time when ``delay_seconds <= 300`` (the
    ``ON_TIME_THRESHOLD_SECONDS`` business rule in ``common/config.py``)
  * gold marts are ``mart_ontime_by_line`` (line x date) and
    ``mart_ontime_by_weather`` (line x weather_condition)

The demo data is generated deterministically (fixed seed) from those
schemas so the console runs standalone anywhere — no S3, Snowflake,
Airflow or live GTFS-RT feed required.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="NJ Transit Reliability", page_icon="🚆", layout="wide")

# ----------------------------------------------------------------------------
# Custom CSS — dark glass / gradient console styling (flowscope pattern)
# ----------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .stApp {
        background: radial-gradient(1200px 600px at 15% -10%, #7a1f1f 0%, transparent 55%),
                    radial-gradient(1000px 500px at 90% 0%, #14315e 0%, transparent 50%),
                    #0e1117;
    }
    .njt-hero {
        padding: 1.1rem 1.4rem; border-radius: 16px; margin-bottom: 0.6rem;
        background: linear-gradient(120deg, rgba(160,30,30,.55), rgba(20,60,140,.50));
        border: 1px solid rgba(255,150,150,.35);
        box-shadow: 0 8px 32px rgba(0,0,0,.45);
    }
    .njt-hero h1 {
        margin: 0; font-size: 2.1rem; letter-spacing: .5px;
        background: linear-gradient(90deg,#ffb3b3,#ffd98a,#9fd0ff);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .njt-hero p { margin: .25rem 0 0 0; color: #cfd8e3; font-size: .95rem; }
    div[data-testid="stMetric"] {
        background: rgba(22,27,34,.85); border: 1px solid rgba(255,140,140,.35);
        border-radius: 12px; padding: .6rem .8rem;
        box-shadow: 0 4px 18px rgba(0,0,0,.35);
    }
    div[data-testid="stMetric"]:hover { border-color: rgba(255,217,138,.7); }
    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] {
        background: rgba(22,27,34,.8); border-radius: 10px 10px 0 0; padding: 6px 14px;
    }
    section[data-testid="stSidebar"] { background: rgba(14,17,23,.92); }
    </style>
    <div class="njt-hero">
      <h1>🚆 NJ Transit Reliability — On-Time Console</h1>
      <p>Schedule vs. actual, by line, hour and weather — the published GTFS schedule
         joined against live GTFS-RT arrivals. Drag the 3D delay space, press play on
         the animated charts, slice everything from the sidebar.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ----------------------------------------------------------------------------
# Deterministic demo data — derived from the pipeline's own schemas
# ----------------------------------------------------------------------------
ON_TIME_THRESHOLD_S = 300  # common/config.py: ON_TIME_THRESHOLD_SECONDS

LINES = {
    # line_name: (route_id, base_delay_min, reliability 0..1)
    "Northeast Corridor": ("NEC", 1.2, 0.90),
    "North Jersey Coast": ("NJCL", 1.9, 0.84),
    "Montclair-Boonton": ("MOBO", 1.5, 0.86),
    "Morris & Essex": ("M&E", 1.4, 0.88),
    "Raritan Valley": ("RVL", 2.1, 0.82),
    "Atlantic City": ("ACL", 0.9, 0.92),
}
LINE_STOPS = {
    "Northeast Corridor": [("NP", "Newark Penn", "Newark Penn"), ("NB", "New Brunswick", "New Brunswick"), ("TR", "Trenton", "Trenton")],
    "North Jersey Coast": [("HB", "Hoboken", "Hoboken"), ("SC", "Secaucus Jct", "Secaucus Jct"), ("NP", "Newark Penn", "Newark Penn")],
    "Montclair-Boonton": [("SU", "Summit", "Summit"), ("NP", "Newark Penn", "Newark Penn"), ("HB", "Hoboken", "Hoboken")],
    "Morris & Essex": [("SU", "Summit", "Summit"), ("HB", "Hoboken", "Hoboken"), ("SC", "Secaucus Jct", "Secaucus Jct")],
    "Raritan Valley": [("NP", "Newark Penn", "Newark Penn"), ("NB", "New Brunswick", "New Brunswick"), ("SC", "Secaucus Jct", "Secaucus Jct")],
    "Atlantic City": [("TR", "Trenton", "Trenton"), ("NP", "Newark Penn", "Newark Penn")],
}
STATIONS = ["Newark Penn", "Secaucus Jct", "Hoboken", "New Brunswick", "Trenton", "Summit"]


def _weather_condition(precip: float, wind: float) -> str:
    """Exact port of dbt/models/marts/fct_stop_performance.sql."""
    if precip >= 2.5:
        return "rain"
    if precip > 0:
        return "light_precip"
    if wind >= 40:
        return "high_wind"
    return "clear"


@st.cache_data
def load_demo() -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(42)
    dates = pd.date_range("2026-06-19", "2026-08-02", freq="D")  # 45 service days
    hours = list(range(5, 23))  # 05:00–22:00 service window

    # One weather regime per (date, station): storm days hit all stations.
    storm_day = {d: rng.random() < 0.22 for d in dates}
    rows: list[dict] = []
    for d in dates:
        dow = d.weekday()
        weekend = dow >= 5
        for line, (route_id, base_delay, _rel) in LINES.items():
            stops = LINE_STOPS[line]
            for h in hours:
                rush = 1.0 if h in (7, 8, 17, 18) else (0.45 if h in (6, 9, 16, 19) else 0.0)
                direction = str(int(rng.integers(0, 2)))
                # 2–3 stop observations per line-hour (one trip, several stops)
                for si, (stop_id, stop_name, station) in enumerate(stops):
                    if rng.random() < 0.18:
                        continue  # not every train stops everywhere
                    storm = storm_day[d] and rng.random() < 0.8
                    u = rng.random()
                    if storm:
                        precip = float(rng.uniform(2.5, 9.0)) if u < 0.7 else float(rng.uniform(0.1, 2.4))
                        wind = float(rng.uniform(18, 38))
                        temp = float(rng.uniform(17, 24))
                        wcode = 63 if precip >= 2.5 else 61
                    elif u < 0.10:
                        precip, wind, temp, wcode = 0.0, float(rng.uniform(40, 58)), float(rng.uniform(19, 27)), 2
                    elif u < 0.28:
                        precip = float(rng.uniform(0.1, 2.4)); wind = float(rng.uniform(8, 22))
                        temp = float(rng.uniform(20, 29)); wcode = 51
                    else:
                        precip, wind, temp, wcode = 0.0, float(rng.uniform(4, 24)), float(rng.uniform(21, 31)), 1
                    cond = _weather_condition(precip, wind)
                    wx_penalty = {"rain": 3.4, "light_precip": 1.0, "high_wind": 2.2, "clear": 0.0}[cond]
                    mean_delay = base_delay + rush * 1.4 + wx_penalty + si * 0.35 - (1.1 if weekend else 0.0)
                    delay_min = float(np.clip(rng.normal(mean_delay, 2.2), -3.0, 28.0))
                    delay_s = int(round(delay_min * 60))
                    sched_min = h * 60 + int(rng.integers(0, 55))
                    rows.append(
                        {
                            "trip_id": f"{route_id}_{d:%m%d}_{h:02d}{direction}",
                            "route_id": route_id,
                            "line_name": line,
                            "direction_id": direction,
                            "stop_id": stop_id,
                            "stop_name": stop_name,
                            "stop_sequence": si + 1,
                            "delay_seconds": delay_s,
                            "delay_min": delay_s / 60.0,
                            "on_time_flag": int(delay_s <= ON_TIME_THRESHOLD_S),
                            "service_date": d,
                            "hour": h,
                            "weather_station": station,
                            "temperature_c": round(temp, 1),
                            "precipitation_mm": round(precip, 1),
                            "wind_speed_kmh": round(wind, 1),
                            "weather_code": wcode,
                            "weather_condition": cond,
                            "scheduled_min": sched_min,
                            "actual_min": sched_min + delay_min,
                            "is_weekend": weekend,
                        }
                    )
    stops = pd.DataFrame(rows)

    # Quarantine / quality ledger — one row per date x layer, Great Expectations
    # gates at bronze / silver / gold (quality/validate.py). Rain days spike
    # failures: stale feeds, out-of-bounds delays, unmatched trips.
    qrows: list[dict] = []
    for d in dates:
        day = stops[stops["service_date"] == d]
        rain_share = float((day["weather_condition"] == "rain").mean()) if len(day) else 0.0
        for layer, base_rate in (("bronze", 0.006), ("silver", 0.011), ("gold", 0.003)):
            checked = int(max(len(day) * (3 if layer == "bronze" else 1), 40))
            rate = base_rate + rain_share * 0.055 + float(rng.uniform(0, 0.004))
            quarantined = int(round(checked * rate))
            qrows.append(
                {
                    "service_date": d,
                    "layer": layer,
                    "checked": checked,
                    "quarantined": quarantined,
                    "passed": checked - quarantined,
                    "quarantine_rate": quarantined / checked if checked else 0.0,
                    "rain_share": rain_share,
                }
            )
    quarantine = pd.DataFrame(qrows)
    return stops, quarantine


stops, quarantine = load_demo()

# ----------------------------------------------------------------------------
# Sidebar — global dynamic filters (every chart below reads filtered data)
# ----------------------------------------------------------------------------
st.sidebar.header("🎛️ Live filters")
dmin, dmax = stops["service_date"].min(), stops["service_date"].max()
picked = st.sidebar.date_input(
    "Service date range", value=(dmin.date(), dmax.date()),
    min_value=dmin.date(), max_value=dmax.date(),
)
if isinstance(picked, (list, tuple)) and len(picked) == 2:
    start, end = pd.Timestamp(picked[0]), pd.Timestamp(picked[1])
else:
    start, end = dmin, dmax
all_lines = sorted(stops["line_name"].unique())
sel_lines = st.sidebar.multiselect("Lines", all_lines, default=all_lines)
all_wx = ["clear", "light_precip", "rain", "high_wind"]
sel_wx = st.sidebar.multiselect("Weather conditions", all_wx, default=all_wx)
all_stations = sorted(stops["weather_station"].unique())
sel_stations = st.sidebar.multiselect("Weather stations", all_stations, default=all_stations)
hour_range = st.sidebar.slider("Hour of day", 5, 22, (5, 22))
sel_dir = st.sidebar.multiselect("Direction", ["0", "1"], default=["0", "1"],
                                 format_func=lambda x: "Inbound (0)" if x == "0" else "Outbound (1)")
only_late = st.sidebar.toggle("Only late trains (> 5 min)", value=False)
st.sidebar.caption("On-time = delay ≤ 300s — the pipeline's `ON_TIME_THRESHOLD_SECONDS` rule. "
                   "Weather buckets follow `fct_stop_performance` exactly.")

f = stops[
    (stops["service_date"] >= start) & (stops["service_date"] <= end)
    & stops["line_name"].isin(sel_lines or all_lines)
    & stops["weather_condition"].isin(sel_wx or all_wx)
    & stops["weather_station"].isin(sel_stations or all_stations)
    & stops["hour"].between(hour_range[0], hour_range[1])
    & stops["direction_id"].isin(sel_dir or ["0", "1"])
].copy()
if only_late:
    f = f[f["on_time_flag"] == 0]
fq = quarantine[(quarantine["service_date"] >= start) & (quarantine["service_date"] <= end)]

# Gold-mart equivalents, computed live from the filtered silver grain —
# the same GROUP BYs as mart_ontime_by_line / mart_ontime_by_weather.
by_line = (
    f.groupby(["line_name", "service_date"], as_index=False)
    .agg(trips=("trip_id", "count"), on_time_pct=("on_time_flag", lambda s: 100.0 * s.mean()),
         avg_delay_min=("delay_min", "mean"))
    if not f.empty else f
)
by_weather = (
    f.groupby(["line_name", "weather_condition"], as_index=False)
    .agg(trips=("trip_id", "count"), on_time_pct=("on_time_flag", lambda s: 100.0 * s.mean()),
         avg_delay_min=("delay_min", "mean"))
    if not f.empty else f
)

# ----------------------------------------------------------------------------
# KPI row — with deltas vs the prior equal-length period
# ----------------------------------------------------------------------------
if not f.empty:
    otp = 100.0 * float(f["on_time_flag"].mean())
    span_days = max((f["service_date"].max() - f["service_date"].min()).days, 1)
    prior_end = f["service_date"].min() - pd.Timedelta(days=1)
    prior_start = prior_end - pd.Timedelta(days=span_days)
    prior = stops[(stops["service_date"] >= prior_start) & (stops["service_date"] <= prior_end)
                  & stops["line_name"].isin(sel_lines or all_lines)]
    prior_otp = 100.0 * float(prior["on_time_flag"].mean()) if not prior.empty else otp
    line_otp = f.groupby("line_name")["on_time_flag"].mean().sort_values()
    worst_line = line_otp.index[0] if len(line_otp) else "—"
    q_rate = float(fq["quarantined"].sum() / fq["checked"].sum() * 100) if fq["checked"].sum() else 0.0
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("On-time % (filtered)", f"{otp:.1f}%",
              delta=f"{otp - prior_otp:+.1f} pts vs prior period" if not prior.empty else None)
    c2.metric("Avg delay", f"{f['delay_min'].mean():.1f} min",
              delta=f"{f['delay_min'].mean() - prior['delay_min'].mean():+.1f} min vs prior" if not prior.empty else None,
              delta_color="inverse")
    c3.metric("Stop observations", f"{len(f):,}", delta=f"{f['trip_id'].nunique():,} trips · {f['service_date'].nunique()} days")
    c4.metric("Worst line right now", str(worst_line),
              delta=f"{100.0 * float(line_otp.iloc[0]):.1f}% on time" if len(line_otp) else None, delta_color="inverse")
    c5.metric("Quarantined share", f"{q_rate:.2f}%", delta=f"{int(fq['quarantined'].sum()):,} rows held back")

tab_line, tab_anim, tab_3d, tab_sched, tab_wx, tab_hour, tab_q = st.tabs(
    ["🚉 By line", "🎬 Animated reliability", "🧊 3D delay space", "🕐 Schedule vs actual",
     "🌧️ Weather impact", "⏰ Hour of day", "🛡️ Quarantine & quality"]
)

with tab_line:
    st.subheader("On-time performance by line — the `mart_ontime_by_line` view")
    if f.empty:
        st.info("No stop observations in the current filter.")
    else:
        daily_line = by_line.copy()
        fig = px.line(daily_line.sort_values("service_date"), x="service_date", y="on_time_pct",
                      color="line_name", markers=True,
                      labels={"on_time_pct": "On-time %", "service_date": "Service date", "line_name": "Line"},
                      template="plotly_dark", title="Daily on-time % by line (5-minute rule)")
        fig.add_hline(y=85, line_dash="dash", line_color="#ffd98a", annotation_text="85% target")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch")
        rank = (f.groupby("line_name", as_index=False)
                .agg(trips=("trip_id", "count"), on_time_pct=("on_time_flag", lambda s: 100.0 * s.mean()),
                     avg_delay_min=("delay_min", "mean"), p90_delay_min=("delay_min", lambda s: float(s.quantile(0.9))))
                .sort_values("on_time_pct"))
        fig_r = px.bar(rank, x="on_time_pct", y="line_name", orientation="h", color="avg_delay_min",
                       color_continuous_scale="RdYlGn_r",
                       labels={"on_time_pct": "On-time %", "line_name": "Line", "avg_delay_min": "Avg delay (min)"},
                       template="plotly_dark", title="Line ranking — colour is average delay")
        fig_r.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_r, width="stretch")
        st.dataframe(rank, width="stretch")

with tab_anim:
    st.subheader("Press play — reliability evolving day by day")
    if by_line.empty:
        st.info("No rows in the current filter.")
    else:
        daily = by_line.copy()
        daily["frame"] = daily["service_date"].dt.strftime("%Y-%m-%d")
        fig_a = px.bar(daily.sort_values(["service_date", "line_name"]), x="line_name", y="on_time_pct",
                       color="line_name", animation_frame="frame", range_y=[0, 100],
                       labels={"on_time_pct": "On-time %", "line_name": "Line"},
                       template="plotly_dark", title="Daily on-time % by line — animated")
        fig_a.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
        if fig_a.layout.updatemenus:
            fig_a.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 300
        st.plotly_chart(fig_a, width="stretch")

        race = daily.sort_values(["line_name", "service_date"]).copy()
        race["cum_delay"] = race.groupby("line_name")["avg_delay_min"].cumsum()
        race["frame"] = race["service_date"].dt.strftime("%Y-%m-%d")
        fig_c = px.line(race, x="service_date", y="cum_delay", color="line_name", animation_frame="frame",
                        labels={"cum_delay": "Cumulative avg delay (min)", "service_date": "Service date"},
                        template="plotly_dark", title="Cumulative delay race — animated")
        fig_c.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_c.layout.updatemenus:
            fig_c.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 300
        st.plotly_chart(fig_c, width="stretch")
    st.caption("Animations are native Plotly `animation_frame` charts — hit ▶ Play on each chart.")

with tab_3d:
    st.subheader("🧊 3D delay space — drag to rotate, scroll to zoom")
    if f.empty:
        st.info("No rows in the current filter.")
    else:
        d3 = f.copy()
        d3["size"] = np.clip(d3["delay_min"].abs(), 1, None)
        fig3 = px.scatter_3d(d3.sort_values("service_date"), x="hour", y="temperature_c", z="delay_min",
                             color="line_name", size="size", size_max=13, opacity=0.82,
                             hover_data=["stop_name", "weather_condition", "service_date"],
                             labels={"hour": "Hour of day", "temperature_c": "Temperature (°C)", "delay_min": "Delay (min)"},
                             template="plotly_dark",
                             title="Every stop observation in hour × temperature × delay space")
        fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                           scene=dict(xaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                                      yaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                                      zaxis=dict(backgroundcolor="rgba(20,24,32,.9)")),
                           margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig3, width="stretch")

        st.subheader("On-time surface — line × hour terrain")
        surf = (f.pivot_table(index="line_name", columns="hour", values="on_time_flag", aggfunc="mean") * 100).fillna(0)
        if surf.shape[0] > 1 and surf.shape[1] > 2:
            fig_s = go.Figure(data=[go.Surface(z=surf.to_numpy(), x=[str(c) for c in surf.columns],
                                               y=list(surf.index), colorscale="RdYlGn",
                                               cmin=40, cmax=100, colorbar=dict(title="On-time %"))])
            fig_s.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                                scene=dict(xaxis_title="Hour of day", yaxis_title="Line", zaxis_title="On-time %",
                                           camera=dict(eye=dict(x=1.5, y=-1.4, z=0.9))),
                                margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_s, width="stretch")
        else:
            st.info("Not enough line/hour coverage in the current filter for a surface.")

with tab_sched:
    st.subheader("Schedule vs. actual — where the minutes go")
    if f.empty:
        st.info("No rows in the current filter.")
    else:
        sample = f.sample(n=min(1500, len(f)), random_state=7)
        fig = px.scatter(sample, x="scheduled_min", y="actual_min", color="weather_condition",
                         hover_data=["line_name", "stop_name"],
                         labels={"scheduled_min": "Scheduled (min after midnight)", "actual_min": "Actual (min after midnight)"},
                         template="plotly_dark", title="Actual vs. scheduled arrival — points above the diagonal are late")
        lims = [sample["scheduled_min"].min() - 20, sample["actual_min"].max() + 20]
        fig.add_shape(type="line", x0=lims[0], y0=lims[0], x1=lims[1], y1=lims[1],
                      line=dict(color="#ffd98a", dash="dash"))
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch")
        fig_h = px.histogram(f, x="delay_min", color="weather_condition", nbins=42, barmode="overlay", opacity=0.75,
                             labels={"delay_min": "Delay (min)"}, template="plotly_dark",
                             title="Delay distribution by weather — the rain tail is the story")
        fig_h.add_vline(x=5, line_dash="dash", line_color="#ffd98a", annotation_text="5-min on-time rule")
        fig_h.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_h, width="stretch")

with tab_wx:
    st.subheader("Does weather explain the delays? — the `mart_ontime_by_weather` view")
    if by_weather.empty:
        st.info("No rows in the current filter.")
    else:
        fig = px.bar(by_weather.sort_values("on_time_pct"), x="weather_condition", y="on_time_pct",
                     color="weather_condition", facet_col="line_name", facet_col_wrap=3,
                     category_orders={"weather_condition": ["clear", "light_precip", "rain", "high_wind"]},
                     labels={"on_time_pct": "On-time %", "weather_condition": "Weather"},
                     template="plotly_dark", title="On-time % by line and weather condition")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
        st.plotly_chart(fig, width="stretch")
        fig2 = px.scatter(f.sample(n=min(1200, len(f)), random_state=3), x="precipitation_mm", y="delay_min",
                          color="line_name", opacity=0.7,
                          labels={"precipitation_mm": "Precipitation (mm)", "delay_min": "Delay (min)"},
                          template="plotly_dark", title="Delay vs. precipitation — delays climb as rain gets heavier")
        fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig2, width="stretch")
        st.dataframe(by_weather.sort_values(["line_name", "on_time_pct"]), width="stretch")

with tab_hour:
    st.subheader("On-time % by hour of day — the rush-hour dip")
    if f.empty:
        st.info("No rows in the current filter.")
    else:
        hourly = (f.groupby(["hour", "line_name"], as_index=False)
                  .agg(on_time_pct=("on_time_flag", lambda s: 100.0 * s.mean()), avg_delay_min=("delay_min", "mean"),
                       trips=("trip_id", "count")))
        pivot = hourly.pivot_table(index="line_name", columns="hour", values="on_time_pct")
        fig = px.imshow(pivot, aspect="auto", color_continuous_scale="RdYlGn", zmin=40, zmax=100,
                        labels={"color": "On-time %", "x": "Hour", "y": "Line"}, template="plotly_dark",
                        title="Line × hour on-time heatmap")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch")
        fig_l = px.line(hourly, x="hour", y="avg_delay_min", color="line_name", markers=True,
                        labels={"avg_delay_min": "Avg delay (min)", "hour": "Hour of day"},
                        template="plotly_dark", title="Average delay by hour — peaks at 08:00 and 17:00–18:00")
        fig_l.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_l, width="stretch")

with tab_q:
    st.subheader("Quarantine & quality — bad data is held back, never silently loaded")
    st.markdown(
        "Great Expectations gates run at **bronze** (schema + feed freshness), "
        "**silver** (referential integrity to the static schedule, delay bounds ±6h) and "
        "**gold** (mart bounds). A failed batch is routed to the quarantine prefix and "
        "downstream Airflow tasks are skipped — this tab shows that ledger."
    )
    if fq.empty:
        st.info("No quality rows in the current date range.")
    else:
        daily_q = fq.groupby("service_date", as_index=False).agg(checked=("checked", "sum"), quarantined=("quarantined", "sum"))
        daily_q["quarantine_rate_pct"] = 100.0 * daily_q["quarantined"] / daily_q["checked"]
        fig = px.bar(daily_q, x="service_date", y="quarantined", color_discrete_sequence=["#e4572e"],
                     labels={"quarantined": "Quarantined rows", "service_date": "Service date"},
                     template="plotly_dark", title="Quarantined rows per day — spikes land on storm days")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch")
        by_layer = fq.groupby("layer", as_index=False).agg(checked=("checked", "sum"), quarantined=("quarantined", "sum"))
        by_layer["pass_rate"] = 100.0 * (1 - by_layer["quarantined"] / by_layer["checked"])
        fig2 = px.bar(by_layer, x="layer", y="pass_rate", color="layer",
                      category_orders={"layer": ["bronze", "silver", "gold"]}, range_y=[90, 100],
                      labels={"pass_rate": "Pass rate %", "layer": "Layer"},
                      template="plotly_dark", title="Expectation pass rate by medallion layer")
        fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
        st.plotly_chart(fig2, width="stretch")
        st.dataframe(fq.sort_values(["service_date", "layer"]), width="stretch")
