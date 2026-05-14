#!/usr/bin/env python3
"""
NetSec — Dark Gradio Intelligence Dashboard
=================================================
A stateless, dark-themed web GUI for the ARP Anomaly Detection system.
Renders real-time threat gauges, temporal timelines, network charts,
and full detection tables using Plotly + Gradio.
"""

import os
import tempfile
import shutil
from typing import Dict, Optional, Tuple

import gradio as gr
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pyspark.sql.functions import col, count, countDistinct, date_format

# Import our refactored ARP modules
from config import ARPConfig

from session import create_spark_session
from data_source import ARPDataSource
from enrichment import enrich_arp_data
from detectors import (
    detect_arp_scanning,
    detect_garp_activity,
    detect_arp_spoofing,
    detect_request_flood,
    detect_unsolicited_replies,
    detect_mac_impersonation,
    detect_arp_conflicts,
)

_cfg = ARPConfig()  # instantiate once

# =============================================================================
# DARK THEME CSS
# =============================================================================
_CSS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "styles.css")
with open(_CSS_PATH, "r", encoding="utf-8") as f:
    CUSTOM_CSS = f.read()

# =============================================================================
# SPARK PROCESSING
# =============================================================================


def process_arp_capture(file_path: str) -> Tuple[Optional[Dict], Optional[str]]:
    """Run the full PySpark pipeline and return serializable results."""
    if not file_path or not os.path.exists(file_path):
        return None, "Upload a valid CSV capture file."

    temp_dir = tempfile.mkdtemp()
    try:
        # Spark reads directories; copy file to a temp folder
        dest = os.path.join(temp_dir, "capture.csv")
        shutil.copy(file_path, dest)

        config = ARPConfig()
        config.CSV_INPUT_DIR = temp_dir

        spark = create_spark_session(streaming=False)

        source = ARPDataSource(spark, config)
        raw_df = source.read_csv_batch(temp_dir)
        total_raw = raw_df.count()

        if total_raw == 0:
            return None, "No ARP packets found in the uploaded file."

        enriched = enrich_arp_data(raw_df, config)
        enriched.cache()

        # ---- Summary metrics -------------------------------------------------
        total_req = enriched.filter(col("is_request")).count()
        total_rep = enriched.filter(col("is_reply")).count()
        total_garp = enriched.filter(col("is_gratuitous")).count()

        # ---- Temporal data ---------------------------------------------------
        timeline_df = enriched.select(
            date_format(col("event_ts"), "yyyy-MM-dd HH:mm:ss").alias("second"),
            col("is_request").cast("int"),
            col("is_reply").cast("int"),
            col("is_gratuitous").cast("int"),
        ).toPandas()

        # ---- Top talkers -----------------------------------------------------
        top_talkers_df = (
            enriched.groupBy("eth_src")
            .agg(
                count("*").alias("packet_count"),
                countDistinct("arp_dst_ip").alias("unique_targets"),
            )
            .orderBy(col("packet_count").desc())
            .limit(10)
            .toPandas()
        )

        # ---- Opcode distribution ---------------------------------------------
        opcode_df = enriched.groupBy("opcode").count().toPandas()
        opcode_map = {1: "Request", 2: "Reply"}
        opcode_df["type"] = opcode_df["opcode"].map(opcode_map).fillna("Other")

        # ---- Run all detectors -----------------------------------------------
        scanning = detect_arp_scanning(enriched, config).toPandas()
        garp = detect_garp_activity(enriched, config).toPandas()
        reply_mismatch, mac_flipping, ip_flipping = detect_arp_spoofing(
            enriched, config
        )
        reply_mismatch = reply_mismatch.toPandas()
        mac_flipping = mac_flipping.toPandas()
        ip_flipping = ip_flipping.toPandas()
        flood = detect_request_flood(enriched, config).toPandas()
        unsolicited = detect_unsolicited_replies(enriched, config).toPandas()
        impersonation = detect_mac_impersonation(enriched, config).toPandas()
        conflict = detect_arp_conflicts(enriched, config).toPandas()

        enriched.unpersist()

        return {
            "total": total_raw,
            "requests": total_req,
            "replies": total_rep,
            "garps": total_garp,
            "timeline": timeline_df,
            "top_talkers": top_talkers_df,
            "opcode": opcode_df,
            "scanning": scanning,
            "garp": garp,
            "reply_mismatch": reply_mismatch,
            "mac_flipping": mac_flipping,
            "ip_flipping": ip_flipping,
            "flood": flood,
            "unsolicited": unsolicited,
            "impersonation": impersonation,
            "conflict": conflict,
        }, None

    except Exception as exc:
        return None, f"Processing error: {str(exc)}"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        try:
            spark.stop()
        except Exception:
            pass


# =============================================================================
# PLOTLY CHART BUILDERS (all dark-themed)
# =============================================================================


def _dark_layout(fig: go.Figure, title: str, height: int = 320) -> go.Figure:
    fig.update_layout(
        title=dict(text=title, font=dict(color="#f1f5f9", size=15)),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#cbd5e1", family="Inter, sans-serif"),
        margin=dict(l=40, r=20, t=50, b=30),
        height=height,
        xaxis=dict(gridcolor="rgba(148,163,184,0.1)", zeroline=False),
        yaxis=dict(gridcolor="rgba(148,163,184,0.1)", zeroline=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def build_threat_gauge(data: Dict) -> go.Figure:
    score = 0
    weights = _cfg.THREAT_WEIGHTS
    for key, w in weights.items():
        if len(data[key]) > 0:
            score += w
    score = min(score, 100)

    if score < 25:
        color, status = "#10b981", "LOW"
    elif score < 60:
        color, status = "#f59e0b", "ELEVATED"
    else:
        color, status = "#f43f5e", "CRITICAL"

    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=score,
            number=dict(font=dict(size=42, color=color), suffix="/100"),
            title=dict(
                text=f"Threat Level<br><span style='font-size:0.75em;color:{color}'>{status}</span>",
                font=dict(size=18, color="#f8fafc"),
            ),
            gauge=dict(
                axis=dict(range=[0, 100], tickwidth=1, tickcolor="#94a3b8"),
                bar=dict(color=color, thickness=0.75),
                bgcolor="rgba(0,0,0,0)",
                borderwidth=2,
                bordercolor="rgba(255,255,255,0.08)",
                steps=[
                    dict(range=[0, 25], color="rgba(16,185,129,0.12)"),
                    dict(range=[25, 60], color="rgba(245,158,11,0.12)"),
                    dict(range=[60, 100], color="rgba(244,63,94,0.12)"),
                ],
                threshold=dict(
                    line=dict(color="white", width=3), thickness=0.8, value=score
                ),
            ),
        )
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"),
        margin=dict(l=20, r=20, t=60, b=20),
        height=300,
    )
    return fig


def build_timeline(tdf: pd.DataFrame) -> go.Figure:
    if tdf.empty:
        fig = go.Figure()
        return _dark_layout(fig, "Traffic Timeline — No Data")

    tdf["second"] = pd.to_datetime(tdf["second"])
    # FIX: pandas 2.x requires lowercase 's' for seconds
    tdf = tdf.set_index("second").resample("1s").sum().reset_index()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=tdf["second"],
            y=tdf["is_request"],
            mode="lines",
            name="Requests",
            line=dict(color="#38bdf8", width=2),
            fill="tozeroy",
            fillcolor="rgba(56,189,248,0.08)",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=tdf["second"],
            y=tdf["is_reply"],
            mode="lines",
            name="Replies",
            line=dict(color="#a78bfa", width=2),
            fill="tozeroy",
            fillcolor="rgba(167,139,250,0.08)",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=tdf["second"],
            y=tdf["is_gratuitous"],
            mode="lines",
            name="GARP",
            line=dict(color="#f472b6", width=2),
            fill="tozeroy",
            fillcolor="rgba(244,114,182,0.08)",
        )
    )
    return _dark_layout(fig, "ARP Traffic Timeline", height=340)


def build_top_talkers(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        return _dark_layout(fig, "Top Talkers — No Data")

    fig = go.Figure(
        go.Bar(
            x=df["packet_count"],
            y=df["eth_src"],
            orientation="h",
            marker=dict(
                color=df["packet_count"],
                colorscale="Teal",
                line=dict(color="rgba(255,255,255,0.2)", width=1),
            ),
            text=df["packet_count"],
            textposition="outside",
            textfont=dict(color="#e2e8f0"),
        )
    )
    fig.update_layout(yaxis=dict(autorange="reversed"))
    return _dark_layout(fig, "Top Talkers (Source MAC)", height=340)


def build_opcode_pie(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        return _dark_layout(fig, "Opcode Distribution — No Data")

    colors = ["#38bdf8", "#a78bfa", "#f472b6"]
    fig = go.Figure(
        go.Pie(
            labels=df["type"],
            values=df["count"],
            hole=0.55,
            marker=dict(colors=colors, line=dict(color="rgba(15,23,42,0.8)", width=2)),
            textinfo="label+percent",
            textfont=dict(color="#f1f5f9", size=12),
            rotation=90,
        )
    )
    fig.update_layout(
        showlegend=False,
        annotations=[
            dict(
                text="ARP",
                x=0.5,
                y=0.5,
                font_size=18,
                showarrow=False,
                font_color="#94a3b8",
            )
        ],
        margin=dict(l=20, r=20, t=40, b=20),
        height=300,
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def build_scanning_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        return _dark_layout(fig, "Scanning — No Alerts")

    fig = px.scatter(
        df,
        x="unique_targets",
        y="total_requests",
        size="targeted_ip_count",
        color="scanner_mac",
        hover_data=["window_start", "window_end"],
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    fig.update_traces(marker=dict(line=dict(width=1, color="white"), opacity=0.9))
    fig.update_layout(
        xaxis_title="Unique Targets",
        yaxis_title="Total Requests",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#cbd5e1",
        height=340,
        margin=dict(l=40, r=20, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
    )
    return fig


def build_flood_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        return _dark_layout(fig, "Flood — No Alerts")

    fig = go.Figure(
        go.Scatter(
            x=df["second"],
            y=df["requests_per_sec"],
            mode="markers+lines",
            marker=dict(size=12, color="#f59e0b", line=dict(width=1, color="white")),
            line=dict(color="#f59e0b", width=2),
            fill="tozeroy",
            fillcolor="rgba(245,158,11,0.1)",
        )
    )
    fig.update_layout(xaxis_title="Time", yaxis_title="Requests / Second")
    return _dark_layout(fig, "ARP Request Flood", height=300)


def build_garp_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        return _dark_layout(fig, "GARP — No Activity")

    fig = go.Figure(
        go.Bar(
            x=df["mac"],
            y=df["garp_count"],
            marker=dict(color="#f472b6", line=dict(color="white", width=1)),
            text=df["claimed_ip_count"],
            textposition="auto",
            textfont=dict(color="white"),
        )
    )
    fig.update_layout(xaxis_title="MAC Address", yaxis_title="GARP Count")
    return _dark_layout(fig, "GARP Activity by MAC", height=300)


def build_conflict_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        return _dark_layout(fig, "Conflicts — No Alerts")

    fig = go.Figure(
        go.Bar(
            x=df["contested_ip"],
            y=df["claiming_mac_count"],
            marker=dict(
                color=df["claim_count"],
                colorscale="Reds",
                line=dict(color="white", width=1),
            ),
            text=df["claim_count"],
            textposition="auto",
            textfont=dict(color="white"),
        )
    )
    fig.update_layout(xaxis_title="Contested IP", yaxis_title="Claiming MACs")
    return _dark_layout(fig, "IP Conflicts (Multi-MAC)", height=300)


def build_spoofing_summary(data: Dict) -> go.Figure:
    cats = ["Reply Mismatch", "MAC Flipping", "IP Flipping"]
    vals = [
        len(data["reply_mismatch"]),
        len(data["mac_flipping"]),
        len(data["ip_flipping"]),
    ]
    colors = ["#f43f5e", "#f59e0b", "#a78bfa"]

    fig = go.Figure(
        go.Bar(
            x=cats,
            y=vals,
            marker=dict(color=colors, line=dict(color="white", width=1)),
            text=vals,
            textposition="auto",
            textfont=dict(color="white", size=14),
        )
    )
    fig.update_layout(xaxis_title="Heuristic", yaxis_title="Event Count")
    return _dark_layout(fig, "Spoofing Heuristics", height=300)


# =============================================================================
# GRADIO UI
# =============================================================================


def build_interface() -> gr.Blocks:
    # FIX: Gradio 6.0 — move theme/css to launch(), not Blocks()
    with gr.Blocks(title="NetSec") as demo:
        # Header
        gr.HTML("""
        <div style="text-align:center; padding: 24px 0 8px;">
            <h1 style="margin:0; font-size:2.4rem; font-weight:700; color:#38bdf8; letter-spacing:-0.03em;">
                NetSec
            </h1>
            <p style="margin:6px 0 0; font-size:0.95rem; color:#94a3b8;">
                Stateless Anomaly Detection & Threat Intelligence Dashboard
            </p>
        </div>
        """)

        # Upload row
        with gr.Row():
            with gr.Column(scale=3):
                file_input = gr.File(
                    label="Upload Wireshark / tshark CSV Export",
                    file_types=[".csv"],
                    elem_classes=["upload-box"],
                )
            with gr.Column(scale=1):
                analyze_btn = gr.Button(
                    "🔍  Analyze Capture",
                    variant="primary",
                    size="lg",
                    elem_classes=["metric-card"],
                )
                status_msg = gr.Textbox(
                    label="Status",
                    interactive=False,
                    visible=False,
                )

        # ---------------------------------------------------------------------
        # TAB 1 — Executive Dashboard
        # ---------------------------------------------------------------------
        with gr.Tab("Executive Dashboard"):
            with gr.Row():
                total_card = gr.HTML(
                    '<div class="metric-card"><div class="metric-label">Total Packets</div>'
                    '<div class="metric-value" style="color:#e2e8f0;">—</div></div>'
                )
                req_card = gr.HTML(
                    '<div class="metric-card"><div class="metric-label">Requests</div>'
                    '<div class="metric-value" style="color:#38bdf8;">—</div></div>'
                )
                rep_card = gr.HTML(
                    '<div class="metric-card"><div class="metric-label">Replies</div>'
                    '<div class="metric-value" style="color:#a78bfa;">—</div></div>'
                )
                garp_card = gr.HTML(
                    '<div class="metric-card"><div class="metric-label">GARPs</div>'
                    '<div class="metric-value" style="color:#f472b6;">—</div></div>'
                )

            with gr.Row():
                with gr.Column(scale=3):
                    threat_plot = gr.Plot(label="Threat Assessment")
                with gr.Column(scale=2):
                    detection_json = gr.JSON(label="Detection Summary")

        # ---------------------------------------------------------------------
        # TAB 2 — Traffic Analysis
        # ---------------------------------------------------------------------
        with gr.Tab("Traffic Analysis"):
            with gr.Row():
                timeline_plot = gr.Plot(label="Timeline")
                opcode_plot = gr.Plot(label="Distribution")
            with gr.Row():
                talkers_plot = gr.Plot(label="Top Talkers")

        # ---------------------------------------------------------------------
        # TAB 3 — Threat Intelligence
        # ---------------------------------------------------------------------
        with gr.Tab("Threat Intelligence"):
            with gr.Row():
                scanning_plot = gr.Plot(label="Scanning Detection")
                flood_plot = gr.Plot(label="Flood Detection")
            with gr.Row():
                garp_plot = gr.Plot(label="GARP Activity")
                conflict_plot = gr.Plot(label="IP Conflicts")

        # ---------------------------------------------------------------------
        # TAB 4 — Spoofing & Attacks
        # ---------------------------------------------------------------------
        with gr.Tab("Spoofing & Attacks"):
            with gr.Row():
                spoof_summary = gr.Plot(label="Spoofing Overview")
            with gr.Row():
                rm_table = gr.DataFrame(label="Reply MAC Mismatch")
                mf_table = gr.DataFrame(label="MAC Flipping")
            with gr.Row():
                if_table = gr.DataFrame(label="IP Flipping")
                unsol_table = gr.DataFrame(label="Unsolicited Replies")

        # ---------------------------------------------------------------------
        # TAB 5 — Raw Detections
        # ---------------------------------------------------------------------
        with gr.Tab("Raw Detections"):
            with gr.Row():
                scan_table = gr.DataFrame(label="Scanning Events")
                imp_table = gr.DataFrame(label="MAC Impersonation")
            with gr.Row():
                conflict_table = gr.DataFrame(label="ARP Conflicts")
                garp_table = gr.DataFrame(label="GARP Events")

        # =================================================================
        # Event Handler
        # =================================================================

        def on_analyze(file_obj):
            if file_obj is None:
                return {
                    status_msg: gr.update(value="Please upload a CSV.", visible=True)
                }

            path = file_obj.name if hasattr(file_obj, "name") else str(file_obj)
            data, err = process_arp_capture(path)

            if err:
                return {status_msg: gr.update(value=err, visible=True)}

            # Detection summary dict
            detections = {
                "Scanning": len(data["scanning"]),
                "GARP": len(data["garp"]),
                "Reply Mismatch": len(data["reply_mismatch"]),
                "MAC Flipping": len(data["mac_flipping"]),
                "IP Flipping": len(data["ip_flipping"]),
                "Flood": len(data["flood"]),
                "Unsolicited": len(data["unsolicited"]),
                "Impersonation": len(data["impersonation"]),
                "Conflict": len(data["conflict"]),
            }

            return {
                status_msg: gr.update(
                    value=f"✅ Processed {data['total']:,} packets", visible=True
                ),
                total_card: (
                    f'<div class="metric-card"><div class="metric-label">Total Packets</div>'
                    f'<div class="metric-value" style="color:#e2e8f0;">{data["total"]:,}</div></div>'
                ),
                req_card: (
                    f'<div class="metric-card"><div class="metric-label">Requests</div>'
                    f'<div class="metric-value" style="color:#38bdf8;">{data["requests"]:,}</div></div>'
                ),
                rep_card: (
                    f'<div class="metric-card"><div class="metric-label">Replies</div>'
                    f'<div class="metric-value" style="color:#a78bfa;">{data["replies"]:,}</div></div>'
                ),
                garp_card: (
                    f'<div class="metric-card"><div class="metric-label">GARPs</div>'
                    f'<div class="metric-value" style="color:#f472b6;">{data["garps"]:,}</div></div>'
                ),
                threat_plot: build_threat_gauge(data),
                detection_json: detections,
                timeline_plot: build_timeline(data["timeline"]),
                opcode_plot: build_opcode_pie(data["opcode"]),
                talkers_plot: build_top_talkers(data["top_talkers"]),
                scanning_plot: build_scanning_chart(data["scanning"]),
                flood_plot: build_flood_chart(data["flood"]),
                garp_plot: build_garp_chart(data["garp"]),
                conflict_plot: build_conflict_chart(data["conflict"]),
                spoof_summary: build_spoofing_summary(data),
                rm_table: data["reply_mismatch"],
                mf_table: data["mac_flipping"],
                if_table: data["ip_flipping"],
                unsol_table: data["unsolicited"],
                scan_table: data["scanning"],
                imp_table: data["impersonation"],
                conflict_table: data["conflict"],
                garp_table: data["garp"],
            }

        analyze_btn.click(
            fn=on_analyze,
            inputs=[file_input],
            outputs=[
                status_msg,
                total_card,
                req_card,
                rep_card,
                garp_card,
                threat_plot,
                detection_json,
                timeline_plot,
                opcode_plot,
                talkers_plot,
                scanning_plot,
                flood_plot,
                garp_plot,
                conflict_plot,
                spoof_summary,
                rm_table,
                mf_table,
                if_table,
                unsol_table,
                scan_table,
                imp_table,
                conflict_table,
                garp_table,
            ],
        )

    return demo


if __name__ == "__main__":
    app = build_interface()
    # FIX: Gradio 6.0 — pass theme/css to launch()
    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        theme=gr.themes.Soft(),
        css=CUSTOM_CSS,
    )
