#!/usr/bin/env python3
"""Adaptive rate limiter with 429/403 feedback."""

import time
import random
from collections import deque
from typing import Optional

class AdaptiveRPS:
    """Adaptive rate limiter that adjusts based on HTTP response codes."""
    
    def __init__(self, rps: float = 0.5, min_rps: float = 0.1, max_rps: float = 1.0):
        """Initialize adaptive rate limiter.
        
        Args:
            rps: Initial requests per second
            min_rps: Minimum allowed RPS
            max_rps: Maximum allowed RPS
        """
        self.rps = rps
        self.min_rps = min_rps
        self.max_rps = max_rps
        self.last_request = 0.0
        self.error_history = deque(maxlen=20)  # Track last 20 requests
        
    def wait(self):
        """Wait for the appropriate time before next request."""
        now = time.time()
        delay = max(0.0, (1.0 / self.rps) - (now - self.last_request))
        
        # Add random jitter to avoid thundering herd
        jitter = random.uniform(0.25, 1.0)
        time.sleep(delay + jitter)
        
        self.last_request = time.time()
    
    def feedback(self, status_code: int):
        """Provide feedback on HTTP response code.
        
        Args:
            status_code: HTTP status code from response
        """
        is_error = status_code in (403, 429, 503)
        self.error_history.append(is_error)
        
        # If we have 3+ errors in recent history, slow down
        if sum(self.error_history) >= 3:
            self.rps = max(self.min_rps, self.rps / 2.0)
            print(f"⚠️ Rate limiting: reducing to {self.rps:.2f} RPS due to errors")
        
        # If no errors in recent history, speed up gradually
        elif sum(self.error_history) == 0 and len(self.error_history) == self.error_history.maxlen:
            self.rps = min(self.max_rps, self.rps * 1.2)
            print(f"✅ Rate limiting: increasing to {self.rps:.2f} RPS (no recent errors)")
    
    def get_status(self) -> dict:
        """Get current rate limiter status."""
        return {
            "current_rps": self.rps,
            "min_rps": self.min_rps,
            "max_rps": self.max_rps,
            "recent_errors": sum(self.error_history),
            "total_requests": len(self.error_history)
        }
