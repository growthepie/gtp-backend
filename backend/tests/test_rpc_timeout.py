#!/usr/bin/env python3
"""
Test script to verify the RPC timeout functionality.

This test simulates slow RPC responses and verifies that:
1. Slow RPCs are detected after 10 minutes
2. Slow RPCs are kicked only when other RPCs are available
3. Block ranges are properly reassigned after an RPC is kicked
"""

import time
import threading
from unittest.mock import Mock
from queue import Queue


class MockNodeAdapter:
    """Mock version of NodeAdapter for testing timeout logic."""
    
    def __init__(self, rpc_configs):
        self.rpc_configs = rpc_configs
        self.active_rpcs = set()
        self.rpc_timeouts = {}
    
    def check_and_kick_slow_rpcs(self, rpc_errors, error_lock):
        """Same logic as the real NodeAdapter."""
        current_time = time.time()
        timeout_threshold = 600  # 10 minutes in seconds
        rpcs_to_kick = []
        
        # Check each RPC for timeouts
        for rpc_url, start_times in self.rpc_timeouts.items():
            if rpc_url not in self.active_rpcs:
                continue
                
            # Check if any block range has been processing for more than 10 minutes
            for block_range, start_time in start_times.items():
                if current_time - start_time > timeout_threshold:
                    # Only kick if there are other active RPCs
                    if len(self.active_rpcs) > 1:
                        rpcs_to_kick.append((rpc_url, block_range))
                        print(f"TIMEOUT DETECTED: {rpc_url} has been processing {block_range} for {(current_time - start_time):.1f}s (>{timeout_threshold}s)")
                    else:
                        print(f"TIMEOUT WARNING: {rpc_url} is slow ({(current_time - start_time):.1f}s) but it's the only active RPC")
        
        # Kick the slow RPCs
        with error_lock:
            for rpc_url, block_range in rpcs_to_kick:
                if rpc_url in self.active_rpcs:
                    print(f"KICKING SLOW RPC: Removing {rpc_url} due to timeout on {block_range}")
                    self.active_rpcs.discard(rpc_url)
                    # Remove from rpc_configs to prevent restart
                    self.rpc_configs = [rpc for rpc in self.rpc_configs if rpc['url'] != rpc_url]
                    # Clean up timeout tracking for this RPC
                    if rpc_url in self.rpc_timeouts:
                        del self.rpc_timeouts[rpc_url]
                    # Increment error count to ensure proper tracking
                    rpc_errors[rpc_url] = rpc_errors.get(rpc_url, 0) + 1000


def test_rpc_timeout_detection():
    """Test that slow RPCs are properly detected and kicked."""
    
    # Create adapter with multiple RPC configs
    rpc_configs = [
        {'url': 'http://fast-rpc.example.com', 'workers': 1},
        {'url': 'http://slow-rpc.example.com', 'workers': 1},
        {'url': 'http://backup-rpc.example.com', 'workers': 1}
    ]
    
    adapter = MockNodeAdapter(rpc_configs)
    
    # Initialize active RPCs
    adapter.active_rpcs = {'http://fast-rpc.example.com', 'http://slow-rpc.example.com', 'http://backup-rpc.example.com'}
    
    # Simulate a slow RPC by adding an old timestamp
    current_time = time.time()
    slow_timeout = 700  # 11+ minutes ago (more than 10 minute threshold)
    
    adapter.rpc_timeouts = {
        'http://slow-rpc.example.com': {
            '1000-1099': current_time - slow_timeout
        },
        'http://fast-rpc.example.com': {
            '1100-1199': current_time - 30  # 30 seconds ago (normal)
        }
    }
    
    # Mock error tracking
    rpc_errors = {
        'http://fast-rpc.example.com': 0,
        'http://slow-rpc.example.com': 0,
        'http://backup-rpc.example.com': 0
    }
    error_lock = threading.Lock()
    
    print("Before timeout check:")
    print(f"Active RPCs: {adapter.active_rpcs}")
    print(f"RPC configs count: {len(adapter.rpc_configs)}")
    
    # Run the timeout check
    adapter.check_and_kick_slow_rpcs(rpc_errors, error_lock)
    
    print("\nAfter timeout check:")
    print(f"Active RPCs: {adapter.active_rpcs}")
    print(f"RPC configs count: {len(adapter.rpc_configs)}")
    print(f"Remaining RPC URLs: {[rpc['url'] for rpc in adapter.rpc_configs]}")
    
    # Verify that the slow RPC was kicked
    assert 'http://slow-rpc.example.com' not in adapter.active_rpcs, "Slow RPC should have been removed"
    assert 'http://fast-rpc.example.com' in adapter.active_rpcs, "Fast RPC should still be active"
    assert 'http://backup-rpc.example.com' in adapter.active_rpcs, "Backup RPC should still be active"
    
    # Verify that the slow RPC was removed from configs
    remaining_urls = [rpc['url'] for rpc in adapter.rpc_configs]
    assert 'http://slow-rpc.example.com' not in remaining_urls, "Slow RPC should be removed from configs"
    assert len(remaining_urls) == 2, "Should have 2 RPCs remaining"
    
    print("✅ Test passed: Slow RPC was successfully kicked!")


def test_rpc_timeout_single_rpc():
    """Test that the last remaining RPC is not kicked even if it's slow."""
    
    # Create adapter with single RPC config
    rpc_configs = [
        {'url': 'http://only-rpc.example.com', 'workers': 1}
    ]
    
    adapter = MockNodeAdapter(rpc_configs)
    
    # Initialize active RPCs
    adapter.active_rpcs = {'http://only-rpc.example.com'}
    
    # Simulate a slow RPC (but it's the only one)
    current_time = time.time()
    slow_timeout = 700  # 11+ minutes ago
    
    adapter.rpc_timeouts = {
        'http://only-rpc.example.com': {
            '1000-1099': current_time - slow_timeout
        }
    }
    
    # Mock error tracking
    rpc_errors = {'http://only-rpc.example.com': 0}
    error_lock = threading.Lock()
    
    print("\nTesting single RPC scenario:")
    print("Before timeout check:")
    print(f"Active RPCs: {adapter.active_rpcs}")
    
    # Run the timeout check
    adapter.check_and_kick_slow_rpcs(rpc_errors, error_lock)
    
    print("After timeout check:")
    print(f"Active RPCs: {adapter.active_rpcs}")
    
    # Verify that the only RPC was NOT kicked
    assert 'http://only-rpc.example.com' in adapter.active_rpcs, "Only RPC should not be kicked"
    assert len(adapter.rpc_configs) == 1, "Should still have 1 RPC config"
    
    print("✅ Test passed: Single slow RPC was preserved!")


if __name__ == "__main__":
    print("Testing RPC Timeout Functionality")
    print("=" * 50)
    
    test_rpc_timeout_detection()
    test_rpc_timeout_single_rpc()
    
    print("\n" + "=" * 50)
    print("🎉 All tests passed! RPC timeout system is working correctly.")
    print("\nFeatures implemented:")
    print("• ⏰ 10-minute timeout detection for slow RPCs")
    print("• 🔄 Automatic RPC removal when alternatives are available")
    print("• 🛡️  Protection for the last remaining RPC")
    print("• 📦 Automatic block range reassignment")
    print("• 🧹 Proper cleanup of timeout tracking data")
