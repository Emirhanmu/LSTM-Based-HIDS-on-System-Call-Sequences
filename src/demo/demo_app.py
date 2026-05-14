import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from src.demo.demo_utils import (
    uploaded_file_to_windows_df,
    run_selected_models,
    build_comparison_table,
)

from src.demo.explanation_utils import (
    explain_stide_window,
    explain_lstm_syscall_window,
    explain_lstm_context_window,
)

def _shorten_text(text: str, max_len: int = 120) -> str:
    text = str(text)
    return text if len(text) <= max_len else text[:max_len] + " ..."


st.set_page_config(page_title="Linux Trace Anomaly Detection Demo", layout="wide")
if "analysis_done" not in st.session_state:
    st.session_state.analysis_done = False

if "analysis_results" not in st.session_state:
    st.session_state.analysis_results = None

if "comparison_df" not in st.session_state:
    st.session_state.comparison_df = None

if "trace_name" not in st.session_state:
    st.session_state.trace_name = None

if "num_windows" not in st.session_state:
    st.session_state.num_windows = None

if "input_mode_used" not in st.session_state:
    st.session_state.input_mode_used = None
st.title("Linux Trace Anomaly Detection Demo")
st.caption("Offline trace-level inference dashboard for STIDE, LSTM Syscall, and LSTM Context")

with st.sidebar:
    st.header("Input")
    input_mode = st.radio(
        "Input Mode",
        ["Raw Trace (.zip / .sc)", "Prepared Trace Windows CSV"],
        index=0,
    )

    uploaded_file = st.file_uploader(
        "Upload Trace File",
        type=["zip", "sc", "csv"],
    )

    st.header("Analysis Settings")
    selected_models = st.multiselect(
        "Models",
        ["STIDE", "LSTM Syscall", "LSTM Context"],
        default=["LSTM Syscall"],
    )

    threshold_policy = st.selectbox(
        "Threshold Policy",
        ["q95", "q90", "original"],
        index=0,
    )

    run_button = st.button("Run Analysis", type="primary", use_container_width=True)

st.markdown(
    """
    **What this demo does**
    1. Reads a single trace  
    2. Converts it into thread-based sliding windows  
    3. Runs the selected model(s)  
    4. Aggregates window scores into a single trace score using **top-10% mean pooling**  
    5. Compares the trace score with the selected threshold policy  
    """
)

if run_button:
    if uploaded_file is None:
        st.error("Please upload a trace file first.")
        st.stop()

    if not selected_models:
        st.error("Please select at least one model.")
        st.stop()

    try:
        with st.spinner("Parsing input and building windows..."):
            windows_df = uploaded_file_to_windows_df(uploaded_file, input_mode)

        with st.spinner("Running selected model(s)..."):
            results = run_selected_models(
                windows_df=windows_df,
                selected_models=selected_models,
                threshold_policy=threshold_policy,
            )

        comparison_df = build_comparison_table(results)

        # Save everything into session state
        st.session_state.analysis_done = True
        st.session_state.analysis_results = results
        st.session_state.comparison_df = comparison_df
        st.session_state.trace_name = str(windows_df["trace_name"].iloc[0])
        st.session_state.num_windows = len(windows_df)
        st.session_state.input_mode_used = input_mode
        st.session_state.windows_preview_df = windows_df.head(20).copy()

        st.success("Analysis completed successfully.")

    except Exception as e:
        st.exception(e)

if st.session_state.analysis_done and st.session_state.analysis_results is not None:
    results = st.session_state.analysis_results
    comparison_df = st.session_state.comparison_df
    trace_name = st.session_state.trace_name
    num_windows = st.session_state.num_windows
    input_mode_used = st.session_state.input_mode_used
    windows_preview_df = st.session_state.windows_preview_df

    c1, c2, c3 = st.columns(3)
    c1.metric("Trace Name", trace_name)
    c2.metric("Input Mode", input_mode_used)
    c3.metric("Number of Windows", f"{num_windows:,}")

    with st.expander("Preview Parsed Windows", expanded=False):
        st.dataframe(windows_preview_df, use_container_width=True)

    st.subheader("Trace-Level Comparison")
    st.dataframe(comparison_df, use_container_width=True)

    st.subheader("Model Cards")
    cols = st.columns(len(results))
    for col, res in zip(cols, results):
        status_emoji = "🚨" if res.prediction == "ANOMALY" else "✅"
        col.markdown(f"### {status_emoji} {res.model_name}")
        col.write(f"**Trace Score:** {res.trace_score:.6f}")
        col.write(f"**Threshold:** {res.threshold:.6f}")
        col.write(f"**Decision:** {res.prediction}")
        col.write(f"**Windows:** {res.num_windows:,}")
        col.write(f"**Top-k used:** {res.top_k_used:,}")

    st.subheader("Detailed Model View")
    detail_model = st.selectbox(
        "Select model for detailed plot",
        [r.model_name for r in results],
        key="detail_model_select",
    )
    chosen = next(r for r in results if r.model_name == detail_model)

    plot_df = chosen.window_scores_df.sort_values(["window_start_idx", "window_id"]).reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.plot(plot_df.index, plot_df["window_score"], linewidth=1.2, label="Window score")
    ax.axhline(chosen.threshold, linestyle="--", linewidth=1.4, label=f"{chosen.threshold_policy} threshold")
    ax.axhline(chosen.trace_score, linestyle=":", linewidth=1.4, label="Trace score")
    ax.set_title(f"{chosen.model_name} - Window Score Profile")
    ax.set_xlabel("Window index")
    ax.set_ylabel("Score")
    ax.legend()
    st.pyplot(fig, use_container_width=True)
    
    suspicious_cols = [
        c for c in [
            "trace_name",
            "window_id",
            "window_start_idx",
            "window_score",
            "window_syscalls",
            "window_process_names",
            "window_return_status",
        ]
        if c in chosen.window_scores_df.columns
    ]

    if chosen.model_name == "STIDE":
        st.subheader("Pattern-Level Explanation (STIDE)")

        top_window = chosen.window_scores_df.sort_values("window_score", ascending=False).iloc[0]
        stide_expl = explain_stide_window(top_window["window_syscalls"])

        st.markdown(stide_expl["window_reason"])

        if stide_expl["top_unseen_ngrams"]:
            st.markdown("**Top unseen syscall patterns in the most suspicious window**")
            st.dataframe(pd.DataFrame(stide_expl["top_unseen_ngrams"]), use_container_width=True)

    elif chosen.model_name == "LSTM Syscall":
        st.subheader("Pattern-Level Explanation (LSTM Syscall)")

        top_window = chosen.window_scores_df.sort_values("window_score", ascending=False).iloc[0]
        lstm_expl = explain_lstm_syscall_window(top_window["window_syscalls"])

        st.markdown(lstm_expl["window_reason"])

        rows = []
        for item in lstm_expl["important_positions"]:
            pred_text = ", ".join(
                f"{p['predicted_syscall']}"
                for p in item["top_predicted_next_syscalls"]
            )
            rows.append({
                "position": item["position"],
                "observed_transition": item["observed_transition"],
                "token_nll": item["token_nll"],
                "model_expected_more_likely": pred_text,
            })

        if rows:
            st.markdown("**Most influential syscall transitions in the most suspicious window**")
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

    elif chosen.model_name == "LSTM Context":
        st.subheader("Pattern-Level Explanation (LSTM Context)")

        top_window = chosen.window_scores_df.sort_values("window_score", ascending=False).iloc[0]
        ctx_expl = explain_lstm_context_window(top_window)

        st.markdown(ctx_expl["window_reason"])

        rows = []
        for item in ctx_expl["important_positions"]:
            pred_text = ", ".join(
                f"{p['predicted_context_token']}"
                for p in item["top_predicted_next_contexts"]
            )
            rows.append({
                "position": item["position"],
                "observed_context_transition": item["observed_context_transition"],
                "token_nll": item["token_nll"],
                "model_expected_more_likely": pred_text,
            })

        if rows:
            st.markdown("**Most influential context transitions in the most suspicious window**")
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

        st.caption(f"Context formatter match score: {ctx_expl['formatter_match_score']:.2f}")

    st.dataframe(
        chosen.window_scores_df[suspicious_cols].head(10),
        use_container_width=True,
    )

    st.info(
        "This dashboard is an offline inference demo. It does not retrain models and does not represent a real-time production deployment."
    )