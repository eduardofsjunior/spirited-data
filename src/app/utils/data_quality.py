"""
Data Quality Warning Components for Frontend

Provides UI components to warn users about film-language combinations
with subtitle data extending beyond film runtime + 10-minute buffer.

[Source: Epic 3.6.5 - Data Quality Validation Layer UX Enhancement]
"""

import streamlit as st
from typing import Optional, Dict


def render_data_quality_warning(validation_data: Optional[Dict[str, any]]) -> None:
    """
    Display data quality warning banner if film-language combination has FAIL status.

    Shows warning when subtitle data extends beyond expected film duration,
    indicating potential data quality issues that may bias emotion analysis.

    Args:
        validation_data: Dict from get_validation_status() containing:
                        - validation_status ('PASS'/'FAIL'/'UNKNOWN')
                        - overrun_minutes (float)
                        - film_title (str)
                        - max_minute_offset (float)
                        - expected_duration_minutes (float)
                        None if no validation data available

    [Source: Epic 3.6.5 - Frontend Data Quality Warnings]
    """
    if validation_data is None:
        return

    validation_status = validation_data.get("validation_status")
    overrun_minutes = validation_data.get("overrun_minutes")
    film_title = validation_data.get("film_title", "this film")
    max_minute = validation_data.get("max_minute_offset")
    expected_duration = validation_data.get("expected_duration_minutes")

    if validation_status == "FAIL" and overrun_minutes is not None:
        # Determine severity level based on overrun magnitude
        if overrun_minutes > 50:
            severity = "CRITICAL"
            emoji = "🔴"
            color = "#ff4b4b"  # Red
        elif overrun_minutes > 20:
            severity = "SEVERE"
            emoji = "🟠"
            color = "#ff8c00"  # Orange
        else:
            severity = "MODERATE"
            emoji = "🟡"
            color = "#ffd700"  # Yellow

        # Render warning banner
        st.markdown(
            f"""
            <div style="
                background-color: {color}15;
                border-left: 5px solid {color};
                padding: 15px 20px;
                margin: 20px 0;
                border-radius: 5px;
            ">
                <h4 style="margin: 0 0 10px 0; color: {color};">
                    {emoji} Data Quality Warning - {severity}
                </h4>
                <p style="margin: 0; line-height: 1.6;">
                    <strong>Subtitle data extends beyond expected film runtime:</strong><br/>
                    • Film "{film_title}" expected duration: <strong>{expected_duration:.0f} minutes</strong><br/>
                    • Emotion data extends to: <strong>minute {max_minute:.0f}</strong><br/>
                    • Overrun: <strong>{overrun_minutes:.0f} minutes beyond 10-minute buffer</strong>
                </p>
                <p style="margin: 10px 0 0 0; font-size: 0.9em; line-height: 1.6;">
                    ⚠️ <em>This combination uses subtitle data that exceeds the film's runtime by over 10 minutes.
                    Emotion analysis may be biased by content that doesn't exist in other languages for this film.
                    Results should be interpreted with caution and may not be comparable across languages.</em>
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )

    elif validation_status == "UNKNOWN":
        # Info banner for unknown status (no runtime data available)
        st.info(
            f"ℹ️ **Data Quality Status:** Runtime validation unavailable for \"{film_title}\". "
            "This film may not be in the Kaggle dataset used for runtime verification."
        )


def get_language_warning_suffix(validation_status: Optional[str]) -> str:
    """
    Get warning suffix to append to language selector options.

    Args:
        validation_status: 'PASS', 'FAIL', or 'UNKNOWN'

    Returns:
        String suffix to append to language name (e.g., " ⚠️ DATA ISSUE")

    [Source: Epic 3.6.5 - Frontend Data Quality Warnings]
    """
    if validation_status == "FAIL":
        return " ⚠️ DATA ISSUE"
    elif validation_status == "UNKNOWN":
        return " ℹ️ UNVALIDATED"
    else:
        return ""
