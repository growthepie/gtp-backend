from src.realtime.rpc_agg import RtBackend
import asyncio
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("rt_trigger")


async def run_backend():
    """Run the real-time backend monitoring."""
    logger.info("Starting RT Backend from trigger script")
    
    backend = RtBackend()
    
    try:
        # Initialize async components (Redis, HTTP session, ETH price)
        await backend.initialize_async()
        
        # Check which clients were successfully initialized
        successful_chains = list(backend.blockchain_clients.keys())
        failed_chains = [name for name in backend.RPC_ENDPOINTS.keys() if name not in successful_chains]
        
        if failed_chains:
            logger.warning(f"Failed to initialize clients for: {', '.join(failed_chains)}")
        
        if not successful_chains:
            logger.error("No chains successfully initialized! Exiting.")
            return
        
        # Create tasks for successfully initialized chains
        tasks = []
        for chain_name in successful_chains:
            task = asyncio.create_task(
                backend.process_chain(chain_name),
                name=f"process_{chain_name}"
            )
            tasks.append(task)
        
        logger.info(f"Started monitoring {len(tasks)} chains: {', '.join(successful_chains)}")
        
        # Print initial stream info
        streams_info = await backend.get_chain_streams_info()
        chain_types = {}
        for chain_name, info in streams_info.items():
            if chain_name in successful_chains:
                chain_type = info.get("chain_type", "unknown")
                if chain_type not in chain_types:
                    chain_types[chain_type] = []
                chain_types[chain_type].append(chain_name)
        
        logger.info("Successfully initialized chains by type:")
        for chain_type, chains in chain_types.items():
            logger.info(f"  {chain_type}: {len(chains)} chains - {', '.join(chains)}")
        
        # Run all tasks concurrently
        # Re-read sys_rpc_config periodically; starts chains that had no RPCs at startup
        tasks.append(asyncio.create_task(backend.rpc_refresh_loop(), name="rpc_refresh"))

        await asyncio.gather(*tasks)
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
    finally:
        # Clean up resources
        await backend.close()
        logger.info("Backend shutdown complete")


# Entry point options:

# Option 1: Run directly if this script is executed
if __name__ == "__main__":
    try:
        asyncio.run(run_backend())
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        exit(1)


# Option 2: Function to call from other scripts
def start_monitoring():
    """Start the monitoring from another script."""
    try:
        asyncio.run(run_backend())
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        raise


# Option 3: Async function to call from other async contexts
async def start_monitoring_async():
    """Start the monitoring from another async context."""
    await run_backend()