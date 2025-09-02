"""Pluggable data providers for market‑maker holdings.

This package contains optional clients for Arkham Intelligence and Nansen
Smart Money APIs.  They are only used when the corresponding configuration
section is present in ``settings.json``.  When not configured, the app falls
back to explicit address aggregation using public blockchain APIs.
"""

