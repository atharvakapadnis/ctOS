"""
Dashboard Tab - Database Statistics and System Overview
"""

import streamlit as st
from pathlib import Path
from ...common.service_factory import ServiceFactory
import sys
import time

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from ..components import display_section_header
from ..data_loader import get_database_statistics, clear_cache


def display_dashboard_tab():
    """Display dashboard with database statistics"""

    st.markdown("### Database Statistics")
    st.caption("Real-time overview of product processing status")

    try:
        stats = get_database_statistics()

        # Section A: Database Statistics
        st.markdown("#### Product Overview")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Products", stats["total_products"])
        with col2:
            st.metric("Processed", stats["processed_count"])
        with col3:
            st.metric("Unprocessed", stats["unprocessed_count"])

        st.markdown("---")

        # Section B: Confidence Distribution
        st.markdown("#### Confidence Distribution")

        confidence_dist = stats["confidence_distribution"]
        high_count = confidence_dist.get("High", 0)
        medium_count = confidence_dist.get("Medium", 0)
        low_count = confidence_dist.get("Low", 0)

        total_processed = (
            stats["processed_count"] if stats["processed_count"] > 0 else 1
        )

        dist_col1, dist_col2, dist_col3 = st.columns(3)

        with dist_col1:
            high_pct = (
                (high_count / total_processed * 100)
                if stats["processed_count"] > 0
                else 0
            )
            st.metric("High Confidence", high_count, f"{high_pct:.1f}%")

        with dist_col2:
            medium_pct = (
                (medium_count / total_processed * 100)
                if stats["processed_count"] > 0
                else 0
            )
            st.metric("Medium Confidence", medium_count, f"{medium_pct:.1f}%")

        with dist_col3:
            low_pct = (
                (low_count / total_processed * 100)
                if stats["processed_count"] > 0
                else 0
            )
            st.metric("Low Confidence", low_count, f"{low_pct:.1f}%")

        st.markdown("---")

        # Section C: Additional Stats
        st.markdown("#### Additional Statistics")

        add_col1, add_col2, add_col3 = st.columns(3)

        with add_col1:
            st.metric("Unique HTS Codes", stats["unique_hts_codes"])

        with add_col2:
            avg_score = stats.get("average_confidence_score", 0.0)
            if avg_score:
                st.metric("Avg Confidence Score", f"{avg_score:.2f}")
            else:
                st.metric("Avg Confidence Score", "N/A")

        with add_col3:
            pass_dist = stats.get("pass_distribution", {})
            total_passes = sum(pass_dist.values())
            st.metric("Total Passes", total_passes)

        # Pass distribution breakdown
        if pass_dist:
            st.markdown("**Pass Distribution:**")
            pass_text = ", ".join(
                [f"Pass {k}: {v}" for k, v in sorted(pass_dist.items())]
            )
            st.text(pass_text)

        st.markdown("---")

        # Section D: Actions
        st.markdown("#### Actions")

        col1, col2 = st.columns([1, 3])

        with col1:
            if st.button("Refresh Statistics", type="primary"):
                clear_cache()
                st.rerun()

        with col2:
            timestamp = stats.get("timestamp", "Unknown")
            st.caption(f"Last updated: {timestamp}")

        st.markdown("---")

        # Section E: System Actions
        st.markdown("#### System Actions")
        st.caption("Administrative tools for cache management and troubleshooting")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("Clear All Caches", type="secondary", width="stretch"):
                try:
                    # Clear ServiceFactory Cache
                    ServiceFactory.clear_cache()

                    # Clear Streamlit Cache
                    st.cache_data.clear()
                    st.cache_resource.clear()

                    st.success("All caches cleared successfully!")
                    st.info("Page will reload to apply changes...")

                    # Wait briefly for user to see message
                    time.sleep(1)

                    # Reload page
                    st.rerun()

                except Exception as e:
                    st.error(f"Failed to clear caches: {str(e)}")

        with col2:
            if st.button("Realod Rules", type="secondary", width="stretch"):
                try:

                    # Reload Rules
                    ServiceFactory.reload_rules()

                    st.success("Rules reloaded successfully!")

                    # Wait Briefly
                    time.sleep(0.5)

                    st.rerun()

                except Exception as e:
                    st.error(f"Failed to reload rules: {str(e)}")

        # Show cache statistics (for developers/ debugging)
        with st.expander("Cache Statistics (Debug Info)"):
            try:
                stats = ServiceFactory.get_cache_stats()
                st.json(stats)
                st.caption(
                    "Cached instances are reused across operations to improve performance."
                )

            except Exception as e:
                st.error(f"Failed to get cache statistics: {str(e)}")

    except Exception as e:
        st.error(f"Error loading statistics: {str(e)}")
        if st.button("Retry"):
            clear_cache()
            st.rerun()
