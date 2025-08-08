import os
import sys
import getpass

from src.misc.ai_twitter_generator import TwitterPostGenerator
from src.misc.gtp_analyst import GTPAnalyst, convert_timestamps
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from dotenv import load_dotenv
load_dotenv(override=True)  # Force reload environment variables

import pandas as pd
from datetime import datetime
import json
import asyncio
from playwright.async_api import async_playwright
import time

class ChartImageGenerator:
    def __init__(self):
        """Initialize chart image generator with GrowthePie configuration"""
        # Map our metrics to GrowthePie chart paths
        self.metric_to_chart_path = {
            'daa': 'fundamentals/daily-active-addresses',
            'txcount': 'fundamentals/transaction-count', 
            'market_cap_usd': 'fundamentals/market-cap',
            'gas_per_second': 'fundamentals/throughput',
            'tvl': 'fundamentals/total-value-locked',
            'stables_mcap': 'fundamentals/stablecoin-market-cap',
            'rent_paid_usd': 'fundamentals/rent-paid',
            'profit_usd': 'fundamentals/profit'
        }
        
        # Base URL and fixed parameters
        self.base_url = "https://www.growthepie.com/embed"
        self.fixed_params = {
            'showUsd': 'true',
            'theme': 'dark', 
            'timespan': '180d',
            'scale': 'stacked',
            'interval': 'daily',
            'showMainnet': 'true',
            'zoomed': 'false',
            'startTimestamp': '',
            'endTimestamp': '175080960000'
        }
        
        # Create images directory if it doesn't exist
        os.makedirs('generated_images', exist_ok=True)

    def generate_chart_url(self, metric, chain):
        """Generate GrowthePie embed URL for given metric and chain"""
        chart_path = self.metric_to_chart_path.get(metric)
        if not chart_path:
            print(f"⚠️ No chart mapping found for metric: {metric}")
            return None
        
        # Build URL
        url = f"{self.base_url}/{chart_path}"
        
        # Add query parameters
        params = self.fixed_params.copy()
        params['chains'] = chain.lower()
        
        # Build query string
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        full_url = f"{url}?{query_string}"
        
        return full_url

    async def capture_chart_screenshot(self, url, filename):
        """Capture screenshot of GrowthePie chart using Playwright"""
        try:
            async with async_playwright() as p:
                # Launch browser
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # Set viewport size for high quality charts
                await page.set_viewport_size({"width": 1200, "height": 800})
                
                print(f"📸 Loading chart: {url}")
                await page.goto(url, wait_until="networkidle", timeout=30000)
                
                # Wait a bit more for chart to fully render
                await page.wait_for_timeout(5000)
                
                # Take screenshot
                screenshot_path = f"generated_images/{filename}"
                await page.screenshot(path=screenshot_path, full_page=False)
                
                await browser.close()
                print(f"✅ Screenshot saved: {screenshot_path}")
                return screenshot_path
                
        except Exception as e:
            print(f"❌ Error capturing screenshot for {url}: {e}")
            return None

    async def generate_milestone_charts(self, combined_data):
        """Generate chart images for all relevant metrics and chains from milestone data"""
        generated_images = []
        
        # Get unique combinations of chain and metric from milestone data
        chart_combinations = set()
        
        # Process single-chain milestones
        single_chain = combined_data.get("single_chain_milestones", {})
        for chain, chain_data in single_chain.items():
            for date, metrics in chain_data.items():
                for metric in metrics.keys():
                    chart_combinations.add((chain, metric))
        
        # Process cross-chain milestones  
        cross_chain = combined_data.get("cross_chain_milestones", {})
        for chain, chain_data in cross_chain.items():
            for date, metrics in chain_data.items():
                for metric in metrics.keys():
                    chart_combinations.add((chain, metric))
        
        print(f"📊 Generating charts for {len(chart_combinations)} metric/chain combinations...")
        
        # Generate charts for each combination
        for chain, metric in chart_combinations:
            url = self.generate_chart_url(metric, chain)
            if url:
                # Create descriptive filename
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"chart_{chain}_{metric}_{timestamp}.png"
                
                screenshot_path = await self.capture_chart_screenshot(url, filename)
                if screenshot_path:
                    generated_images.append({
                        'chain': chain,
                        'metric': metric,
                        'path': screenshot_path,
                        'url': url,
                        'filename': filename
                    })
                
                # Small delay between screenshots to be respectful
                await asyncio.sleep(2)
        
        print(f"✅ Generated {len(generated_images)} chart images")
        return generated_images
    
    def cleanup_old_images(self, days_old=7):
        """Clean up chart images older than specified days"""
        import time
        
        try:
            current_time = time.time()
            cutoff_time = current_time - (days_old * 24 * 60 * 60)
            
            images_dir = "generated_images"
            if not os.path.exists(images_dir):
                return
            
            deleted_count = 0
            for filename in os.listdir(images_dir):
                if filename.endswith('.png'):
                    file_path = os.path.join(images_dir, filename)
                    if os.path.getmtime(file_path) < cutoff_time:
                        os.remove(file_path)
                        deleted_count += 1
            
            if deleted_count > 0:
                print(f"🧹 Cleaned up {deleted_count} old chart images")
        
        except Exception as e:
            print(f"⚠️ Error during cleanup: {e}")

class SocialMediaAutomation:
    def __init__(self):
        """Initialize social media automation with GTP analyst and Twitter generator"""
        load_dotenv(override=True)
        
        self.gtp_analyst = GTPAnalyst()
        self.twitter_generator = TwitterPostGenerator()
        self.chart_generator = ChartImageGenerator()
        self.webhook_url = os.getenv("GTP_ANALYST_WEBHOOK_URL_LOCAL")
        self.chain_social_handles = {}  # Cache for social handles
        
        # Configuration constants (non-secret)
        self.data_url = "https://api.growthepie.xyz/v1/fundamentals_full.json"
        self.master_url = "https://api.growthepie.xyz/v1/master.json" 
        self.local_filename = "fundamentals_full.json"
        self.master_filename = "master.json"
        
        print(f"🔧 Loaded webhook URL from env: {self.webhook_url[:50]}...{self.webhook_url[-10:] if self.webhook_url else 'None'}")
    
    def fetch_master_data(self, master_url, local_filename="master.json"):
        """Fetch master.json data containing chain social handles"""
        try:
            # Use the same fetch method as fundamentals data
            self.gtp_analyst.fetch_json_data(master_url, local_filename)
            return True
        except Exception as e:
            print(f"❌ Error fetching master data: {e}")
            return False
    
    def extract_social_handles(self, master_filename="master.json"):
        """Extract Twitter/social handles from master.json"""
        social_handles = {}
        unique_chains = {}  # Track unique chains to avoid duplicates
        
        try:
            if not os.path.exists(master_filename):
                print(f"⚠️ Master file {master_filename} not found, continuing without social handles")
                return social_handles
                
            with open(master_filename, 'r') as f:
                master_data = json.load(f)
            
            # Extract social handles from chains
            chains = master_data.get('chains', {})
            
            for chain_key, chain_info in chains.items():
                if isinstance(chain_info, dict):
                    # Extract Twitter handle
                    twitter_handle = chain_info.get('twitter')
                    if twitter_handle:
                        # Clean up the Twitter handle
                        if twitter_handle.startswith('https://twitter.com/'):
                            twitter_handle = twitter_handle.replace('https://twitter.com/', '@')
                        elif twitter_handle.startswith('https://x.com/'):
                            twitter_handle = twitter_handle.replace('https://x.com/', '@')
                        elif not twitter_handle.startswith('@'):
                            twitter_handle = f"@{twitter_handle}"
                        
                        display_name = chain_info.get('name', chain_key.title())
                        
                        # Use display_name as the unique key to avoid duplicates
                        unique_key = display_name.lower()
                        
                        # Only store if we haven't seen this display name before
                        if unique_key not in unique_chains:
                            unique_chains[unique_key] = {
                                'twitter': twitter_handle,
                                'display_name': display_name,
                                'chain_key': chain_key
                            }
                            
                            # Store both chain_key and display_name as lookup keys
                            social_handles[chain_key] = unique_chains[unique_key]
                            social_handles[unique_key] = unique_chains[unique_key]
            
            # Count unique chains (not total entries)
            unique_count = len(unique_chains)
            print(f"✅ Extracted social handles for {unique_count} unique chains")
            
            # Display unique chains only
            for chain_info in unique_chains.values():
                print(f"  - {chain_info['display_name']}: {chain_info['twitter']}")
            
            return social_handles
            
        except Exception as e:
            print(f"❌ Error extracting social handles: {e}")
            return social_handles
    
    def enhance_tweet_with_social_handles(self, tweet_text, chain_names):
        """Add relevant social handles to tweet text"""
        try:
            enhanced_text = tweet_text
            added_handles = []
            
            # Look for matching chains and add their handles
            for chain_name in chain_names:
                chain_lower = chain_name.lower()
                
                # Try exact match first
                if chain_lower in self.chain_social_handles:
                    handle_info = self.chain_social_handles[chain_lower]
                    twitter_handle = handle_info.get('twitter')
                    
                    if twitter_handle and twitter_handle not in enhanced_text and twitter_handle not in added_handles:
                        added_handles.append(twitter_handle)
                
                # Try partial matches for common variations
                elif chain_lower in ['arbitrum', 'arb']:
                    if 'arbitrum' in self.chain_social_handles:
                        handle_info = self.chain_social_handles['arbitrum']
                        twitter_handle = handle_info.get('twitter')
                        if twitter_handle and twitter_handle not in enhanced_text and twitter_handle not in added_handles:
                            added_handles.append(twitter_handle)
            
            # Enhanced strategy: Prioritize fitting handles by removing content if needed
            if added_handles:
                # Try to add handles, shortening tweet if necessary
                for handle in added_handles:
                    test_text = enhanced_text + " " + handle
                    
                    if len(test_text) <= 280:
                        enhanced_text += " " + handle
                    else:
                        # Tweet is too long, try to shorten it
                        # Remove excess content to fit the handle
                        max_base_length = 280 - len(" " + handle)
                        if max_base_length > 100:  # Only shorten if we can keep meaningful content
                            # Try to trim from the end, preserving hashtags
                            words = enhanced_text.split()
                            hashtag_words = [w for w in words if w.startswith('#')]
                            non_hashtag_words = [w for w in words if not w.startswith('#')]
                            
                            # Rebuild text to fit
                            shortened_text = ""
                            for word in non_hashtag_words:
                                test_length = len(shortened_text + " " + word + " " + " ".join(hashtag_words) + " " + handle)
                                if test_length <= 280:
                                    shortened_text += (" " + word if shortened_text else word)
                                else:
                                    break
                            
                            # Add hashtags back
                            if hashtag_words:
                                shortened_text += " " + " ".join(hashtag_words)
                            
                            # Add handle
                            enhanced_text = shortened_text + " " + handle
                            break
            
            return enhanced_text
            
        except Exception as e:
            print(f"⚠️ Error enhancing tweet with social handles: {e}")
            return tweet_text
    
    def format_single_milestone_for_tweets(self, milestone, chain, metric):
        """Convert a single milestone into context for AI tweet generation following new social guidelines"""
        try:
            # Extract milestone data
            milestone_type = milestone.get('milestone', '')
            exact_value = milestone.get('exact_value', '')
            date = milestone.get('date', '')
            importance = milestone.get('total_importance', 0)
            chain_rank = milestone.get('rank', 0)
            
            # Get social handle for this chain
            social_handle = self.chain_social_handles.get(chain.lower(), {}).get('twitter', f'@{chain}')
            
            # Create friendly metric names and references
            metric_names = {
                'daa': 'Daily Active Addresses',
                'txcount': 'Transaction Count', 
                'market_cap_usd': 'Market Cap',
                'gas_per_second': 'Throughput',
                'tvl': 'Total Value Locked'
            }
            friendly_metric = metric_names.get(metric, metric.upper())
            metric_ref = metric.upper() if metric != 'gas_per_second' else 'Throughput'
            
            # Format the exact value appropriately
            formatted_value = exact_value
            if metric == 'market_cap_usd':
                try:
                    value_num = float(exact_value.replace(',', ''))
                    if value_num > 1000000000:
                        formatted_value = f"${value_num/1000000000:.2f}B"
                    else:
                        formatted_value = f"${value_num/1000000:.1f}M"
                except:
                    formatted_value = exact_value
            elif metric in ['daa', 'txcount']:
                try:
                    formatted_value = f"{int(float(exact_value.replace(',', ''))):,}"
                except:
                    formatted_value = exact_value
            elif metric == 'gas_per_second':
                try:
                    formatted_value = f"{float(exact_value):.2f} Mgas/s"
                except:
                    formatted_value = f"{exact_value} Mgas/s"
            
            # Determine milestone achievement type
            ath_indicator = ""
            if milestone_type == 'Chain ATH':
                ath_indicator = "ATH"
            elif milestone_type == '1-Year High':
                ath_indicator = "1-Year High"
            elif 'Up' in milestone_type:
                # Extract percentage if available
                if '%' in exact_value:
                    ath_indicator = f"Growth (+{exact_value})"
                else:
                    ath_indicator = "Strong Growth"
            
            # Create explanation for metric significance
            metric_explanations = {
                'daa': "Active addresses do not = unique users as users can have multiple addresses, but they do help indicate activity per address.",
                'txcount': "Transaction count shows the total number of transactions processed and indicates network usage and activity.",
                'market_cap_usd': "Market cap reflects the total value of all tokens and shows market confidence in the chain.",
                'gas_per_second': "Throughput measures the amount of gas used per second and indicates activity.",
                'tvl': "Total Value Locked shows the amount of capital deployed on the chain and indicates trust and adoption."
            }
            
            return f"""
MILESTONE DATA for {chain.title()} {friendly_metric}:

[social_handle]: {social_handle}
[metric_name]: {friendly_metric}
[metric_ref]: {metric_ref}
[milestone_type]: {milestone_type}
[ath_indicator]: {ath_indicator}
[exact_value]: {formatted_value}
[chain_rank]: Top {chain_rank} if chain_rank <= 10 else None
[increase_interval]: Could include 30-day, 90-day, or 1-year increases if available
[date]: {date}
[importance_score]: {importance}/10

CONTEXT:
Chain: {chain.title()} ({social_handle})
Achievement: {milestone_type} in {friendly_metric}
Value: {formatted_value}
Rank: #{chain_rank}
Date: {date}

METRIC EXPLANATION:
{metric_explanations.get(metric, "This metric represents important network activity and development.")}

Instructions: Create a tweet following the exact content structure specified. Start with 🥧 followed by space and a 12-word or less hook that includes {social_handle} and {metric_ref}. Use bullet points with 🔹 and 🔸 alternating for "Description: Amount" format.
""".strip()
            
        except Exception as e:
            print(f"❌ Error formatting single milestone: {e}")
            return None
    
    def cleanup_generated_images(self, generated_images):
        """Delete generated chart images after they've been sent to Discord"""
        try:
            deleted_count = 0
            for image_data in generated_images:
                image_path = image_data.get('path')
                if image_path and os.path.exists(image_path):
                    os.remove(image_path)
                    deleted_count += 1
                    print(f"🗑️ Deleted: {image_data['filename']}")
            
            if deleted_count > 0:
                print(f"🧹 Cleaned up {deleted_count} generated chart images after sending")
            
        except Exception as e:
            print(f"⚠️ Error during image cleanup: {e}")
        
    async def send_tweets_to_discord(self, webhook_url, tweets):
        """Send generated tweets to Discord as embed messages"""
        import aiohttp
        
        for i, tweet in enumerate(tweets, 1):
            embed = {
                "title": f"🐦 Generated Tweet #{i}",
                "description": tweet['tweet'],
                "color": 0x1DA1F2,  # Twitter blue
                "fields": [
                    {
                        "name": "📊 Tweet Stats",
                        "value": f"Characters: {tweet['character_count']}/280\nVariant: {tweet['variant']}\nTemperature: {tweet['temperature']}",
                        "inline": True
                    }
                ],
                "footer": {
                    "text": f"Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Ready to post!"
                }
            }
            
            payload = {"embeds": [embed]}
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(webhook_url, json=payload) as response:
                        if response.status == 204:
                            print(f"✅ Sent tweet #{i} to Discord")
                        else:
                            response_text = await response.text()
                            print(f"❌ Failed to send tweet #{i}: {response.status} - {response_text}")
            except Exception as e:
                print(f"❌ Error sending tweet #{i}: {e}")
            
            # Small delay between messages to avoid rate limits
            await asyncio.sleep(1)
    
    def match_tweets_with_charts(self, tweets, generated_images, combined_data):
        """Match each tweet with its most relevant chart image based on milestone data"""
        matched_pairs = []
        
        # Get milestone details for matching
        milestone_details = []
        
        # Extract milestone info from combined_data
        single_chain = combined_data.get("single_chain_milestones", {})
        for chain, chain_data in single_chain.items():
            for date, metrics in chain_data.items():
                for metric, milestones_list in metrics.items():
                    for milestone in milestones_list:
                        milestone_details.append({
                            'chain': chain,
                            'metric': metric,
                            'importance': milestone.get('total_importance', 0),
                            'type': 'single_chain'
                        })
        
        cross_chain = combined_data.get("cross_chain_milestones", {})
        for chain, chain_data in cross_chain.items():
            for date, metrics in chain_data.items():
                for metric, milestones_list in metrics.items():
                    for milestone in milestones_list:
                        milestone_details.append({
                            'chain': chain,
                            'metric': metric,
                            'importance': milestone.get('total_importance', 0),
                            'type': 'cross_chain'
                        })
        
        # Sort milestones by importance
        milestone_details.sort(key=lambda x: x['importance'], reverse=True)
        
        # IMPROVED MATCHING: Use tweet metadata for precise pairing
        for i, tweet in enumerate(tweets):
            best_match = None
            
            # Get milestone info from tweet metadata (added during generation)
            tweet_chain = tweet.get('milestone_chain')
            tweet_metric = tweet.get('milestone_metric') 
            tweet_importance = tweet.get('milestone_importance', 0)
            
            # Create milestone object for compatibility
            milestone = {
                'chain': tweet_chain,
                'metric': tweet_metric,
                'importance': tweet_importance,
                'type': 'single_chain'  # Default, could be enhanced later
            } if tweet_chain else None
            
            # Try to find exact chart match (chain + metric)
            if tweet_chain and tweet_metric:
                for img in generated_images:
                    if (img['chain'] == tweet_chain and 
                        img['metric'] == tweet_metric):
                        best_match = img
                        break
                
                # If no exact match, try matching chain only
                if not best_match:
                    for img in generated_images:
                        if img['chain'] == tweet_chain:
                            best_match = img
                            break
            
            # If still no match, use any available chart (reuse is OK)
            if not best_match and generated_images:
                best_match = generated_images[0]
            
            # Create pair - all tweets will be sent
            matched_pairs.append({
                'tweet': tweet,
                'chart': best_match,
                'milestone': milestone
            })
        
        return matched_pairs

    async def send_combined_tweet_and_chart(self, webhook_url, matched_pairs):
        """Send combined tweet and chart messages to Discord"""
        import aiohttp
        
        for i, pair in enumerate(matched_pairs, 1):
            try:
                tweet = pair['tweet']
                chart = pair.get('chart')  # Chart might be None
                milestone = pair.get('milestone')
                
                # Create title based on milestone info
                if milestone:
                    title = f"🚀 {milestone['chain'].title()} {milestone['metric'].upper()} Milestone #{i}"
                    importance_indicator = "🔥" if milestone['importance'] >= 10 else "📈" if milestone['importance'] >= 8 else "📊"
                else:
                    title = f"📊 Generated Tweet #{i}"
                    importance_indicator = "📊"
                
                # Create comprehensive embed
                embed = {
                    "title": f"{importance_indicator} {title}",
                    "description": tweet['tweet'],
                    "color": 0x1DA1F2,  # Twitter blue
                    "fields": [
                        {
                            "name": "🐦 Tweet Stats",
                            "value": f"Characters: {tweet['character_count']}/280\nVariant: {tweet['variant']}\nTemperature: {tweet['temperature']}",
                            "inline": True
                        }
                    ],
                    "footer": {
                        "text": f"Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Ready to post!"
                    }
                }
                
                # Add chart info and image if available
                if chart:
                    embed["fields"].append({
                        "name": "📈 Chart Info",
                        "value": f"Chain: {chart['chain'].title()}\nMetric: {chart['metric']}\nTimespan: 180 days",
                        "inline": True
                    })
                    embed["image"] = {
                        "url": f"attachment://{chart['filename']}"
                    }
                    embed["footer"]["text"] = f"Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Ready to post with chart!"
                
                # Add milestone importance if available
                if milestone:
                    embed["fields"].append({
                        "name": "⭐ Milestone Importance",
                        "value": f"Score: {milestone['importance']}/10\nType: {milestone['type'].replace('_', ' ').title()}",
                        "inline": True
                    })
                
                # Handle file upload differently based on whether we have a chart
                if chart:
                    # Read the image file
                    with open(chart['path'], 'rb') as f:
                        image_bytes = f.read()
                    
                    # Create form data for file upload
                    form_data = aiohttp.FormData()
                    form_data.add_field('file', image_bytes, filename=chart['filename'], content_type='image/png')
                    form_data.add_field('payload_json', json.dumps({"embeds": [embed]}))
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.post(webhook_url, data=form_data) as response:
                            if response.status == 200:
                                print(f"✅ Sent combined tweet #{i} with chart for {chart['chain']} {chart['metric']} to Discord")
                            else:
                                response_text = await response.text()
                                print(f"❌ Failed to send combined message #{i}: {response.status} - {response_text}")
                else:
                    # Send tweet-only (no image attachment)
                    payload = {"embeds": [embed]}
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.post(webhook_url, json=payload) as response:
                            if response.status == 204:
                                print(f"✅ Sent tweet #{i} (no chart) to Discord")
                            else:
                                response_text = await response.text()
                                print(f"❌ Failed to send tweet #{i}: {response.status} - {response_text}")
            
            except Exception as e:
                print(f"❌ Error sending message #{i}: {e}")
            
            # Small delay between uploads
            await asyncio.sleep(2)
    
    def format_milestones_for_tweets(self, combined_data):
        """Convert milestone data into comprehensive context for AI tweet generation"""
        context_parts = []
        
        # Common data structures
        metric_names = {
            'gas_per_second': 'gas usage per second',
            'daa': 'daily active addresses',
            'txcount': 'transaction count',
            'market_cap_usd': 'market capitalization',
            'tvl': 'total value locked'
        }
        
        metric_context = {
            'gas_per_second': 'Higher gas usage indicates increased network activity and transaction throughput',
            'daa': 'More daily active addresses shows growing user adoption',
            'txcount': 'Higher transaction counts demonstrate increased network usage',
            'market_cap_usd': 'Market cap growth reflects investor confidence',
            'tvl': 'Total Value Locked growth shows increased DeFi adoption'
        }
        
        def create_milestone_context(milestone, is_cross_chain=False):
            """Helper function to create consistent milestone context"""
            importance = milestone.get("total_importance", 0)
            
            # Determine significance level
            if is_cross_chain:
                significance = "ECOSYSTEM-WIDE BREAKTHROUGH" if importance >= 10 else "cross-chain milestone"
            else:
                if importance >= 10:
                    significance = "MAJOR BREAKTHROUGH"
                elif importance >= 8:
                    significance = "significant milestone"  
                elif importance >= 6:
                    significance = "notable achievement"
                else:
                    significance = "milestone"
            
            # Extract common data
            chain_name = milestone["origin"].title()
            exact_value = milestone.get("exact_value", "N/A")
            milestone_type = milestone["milestone"]
            metric = milestone.get("metric", "")
            date = milestone.get("date", "")
            
            friendly_metric = metric_names.get(metric, metric)
            context_explanation = metric_context.get(metric, 'This metric shows network growth')
            
            # Create context based on type
            if is_cross_chain:
                achievement_text = f"{significance}: {chain_name} has set a new cross-chain record in {friendly_metric}."
                conclusion = f"This achievement highlights {chain_name}'s leading position in the Layer 2 space and sets a new benchmark for the entire ecosystem."
                value_label = "Record Value"
            else:
                achievement_text = f"{significance}: {chain_name} has achieved a {milestone_type} in {friendly_metric}."
                conclusion = f"This represents {significance.lower()} for the Layer 2 ecosystem and demonstrates strong momentum in network growth and adoption."
                value_label = "Current Value"
            
            return f"""
{achievement_text}

Key Details:
- Chain: {chain_name}
- Metric: {friendly_metric}  
- Achievement: {milestone_type}
- {value_label}: {exact_value}
- Date: {date}
- Impact: {context_explanation}

{conclusion}
""".strip()
        
        # Process single-chain milestones
        single_chain = combined_data.get("single_chain_milestones", {})
        for chain, chain_data in single_chain.items():
            for date, metrics in chain_data.items():
                for metric, milestones_list in metrics.items():
                    for milestone in milestones_list:
                        context_parts.append(create_milestone_context(milestone, is_cross_chain=False))
        
        # Process cross-chain milestones  
        cross_chain = combined_data.get("cross_chain_milestones", {})
        for chain, chain_data in cross_chain.items():
            for date, metrics in chain_data.items():
                for metric, milestones_list in metrics.items():
                    for milestone in milestones_list:
                        context_parts.append(create_milestone_context(milestone, is_cross_chain=True))
        
        if context_parts:
            full_context = "\n\n".join(context_parts)
            return full_context
        
        return None
    
    async def run_automated_social_pipeline(self):
        """Run the same logic as test_gtp_analyst.py but generate tweets from the responses"""
        print("🚀 Starting automated social media content generation...")
        print("=" * 60)
        
        try:
            # Use instance configuration (URLs and filenames are not secrets)
            print("Loading configuration...")
            url = self.data_url
            local_filename = self.local_filename
            master_url = self.master_url
            webhook_url = self.webhook_url
            
            if not webhook_url:
                raise ValueError("Environment variable GTP_ANALYST_WEBHOOK_URL_LOCAL is not set.")
            print("Configuration loaded successfully!")

            # Fetch master.json for social handles
            print(f"Fetching master data from: {master_url}")
            if self.fetch_master_data(master_url, self.master_filename):
                self.chain_social_handles = self.extract_social_handles(self.master_filename)
                print("✅ Social handles loaded successfully!")
            else:
                print("⚠️ Failed to fetch master data, continuing without social handles")

            analytics = self.gtp_analyst

            # Fetch and process data
            print(f"Fetching data from URL: {url} and saving to {local_filename}...")
            analytics.fetch_json_data(url, local_filename)
            print("Data fetched successfully! Data saved to:", local_filename)

            metrics = ["daa", "txcount", "market_cap_usd", "gas_per_second", "tvl", "stables_mcap", "rent_paid_usd", "profit_usd"]
            print(f"Filtering data for metrics: {metrics}...")
            filtered_data = analytics.filter_data(file_path=local_filename, metrics=metrics)
            print(f"Data filtered successfully! Filtered data length: {len(filtered_data)}")

            print("Organizing filtered data...")
            organized_data = analytics.organize_data(filtered_data)
            print(f"Data organized successfully! Organized data length: {len(organized_data)}")

            print("Converting organized data to dataframe...")
            df = analytics.json_to_dataframe(organized_data)
            df['date'] = pd.to_datetime(df['date'])
            df.sort_values(by='date', ascending=False, inplace=True)
            print(f"Dataframe created and sorted by date! Dataframe length: {len(df)}")

            print("Ranking origins by TVL...")
            df = analytics.rank_origins_by_tvl(df)
            print("Origins ranked by TVL! Preview of ranked dataframe:\n", df.head())

            # Detect and analyze milestones
            print("Detecting chain milestones...")
            chain_milestones = analytics.detect_chain_milestones(df, analytics.metric_milestones)
            print(f"Detected {len(chain_milestones)} chain milestones. First milestone preview:\n", chain_milestones[0] if chain_milestones else "No milestones detected")

            print("Getting latest milestones...")
            latest_milestones = analytics.get_latest_milestones(chain_milestones, n=3, day_interval=1)
            print(f"Selected top {len(latest_milestones)} latest milestones.")
            latest_milestones = sorted(latest_milestones, key=lambda x: (x['date'], -x['total_importance']))[:10]
            print(f"First latest milestone preview:\n", latest_milestones[0] if latest_milestones else "No latest milestones selected")

            print("Analyzing cross-chain milestones...")
            cross_chain_milestones = analytics.analyze_cross_chain_milestones(df, analytics.cross_chain_milestones)
            cross_chain_milestones = [milestone for milestone in cross_chain_milestones if pd.to_datetime(milestone['date'], format='%d.%m.%Y') >= pd.Timestamp.now() - pd.Timedelta(days=3)]
            print(f"Analyzed {len(cross_chain_milestones)} cross-chain milestones. First cross-chain milestone preview:\n", cross_chain_milestones[0] if cross_chain_milestones else "No cross-chain milestones detected")

            # Organize single-chain milestones based on metric and include date
            print("Organizing single-chain milestone data...")
            chain_data = {}
            for milestone in latest_milestones:
                origin = milestone['origin']
                date = milestone['date']
                if isinstance(date, pd.Timestamp):
                    date = date.strftime('%d.%m.%Y')
                metric = milestone['metric']
                if origin not in chain_data:
                    chain_data[origin] = {}
                if date not in chain_data[origin]:
                    chain_data[origin][date] = {}
                if metric not in chain_data[origin][date]:
                    chain_data[origin][date][metric] = []
                chain_data[origin][date][metric].append(milestone)
            print(f"Single-chain milestone data organized. Number of origins: {len(chain_data)}")

            # Organize cross-chain milestones based on metric and include date
            print("Organizing cross-chain milestone data...")
            cross_chain_data = {}
            for milestone in cross_chain_milestones:
                origin = milestone['origin']
                date = milestone['date']
                if isinstance(date, pd.Timestamp):
                    date = date.strftime('%d.%m.%Y')
                metric = milestone['metric']
                if origin not in cross_chain_data:
                    cross_chain_data[origin] = {}
                if date not in cross_chain_data[origin]:
                    cross_chain_data[origin][date] = {}
                if metric not in cross_chain_data[origin][date]:
                    cross_chain_data[origin][date][metric] = []
                cross_chain_data[origin][date][metric].append(milestone)
            print(f"Cross-chain milestone data organized. Number of origins: {len(cross_chain_data)}")

            # Organize data for JSON agent (same as test_gtp_analyst.py)
            print("Combining single-chain and cross-chain data...")
            combined_data = {
                "single_chain_milestones": chain_data,
                "cross_chain_milestones": cross_chain_data
            }

            # Convert any remaining Timestamps to strings
            print("Converting timestamps to strings...")
            combined_data = convert_timestamps(combined_data)
            print("Timestamps converted. Combined data preview:", {k: v for k, v in combined_data.items() if v})  # Print only non-empty data

            # Analyze Layer 2 milestones (same as test_gtp_analyst.py)
            print("Generating milestone responses...")
            responses = analytics.generate_milestone_responses(combined_data)
            print(f"Milestone responses generated. Number of responses: {len(responses)}")

            # GENERATE CHART IMAGES (new feature)
            print("\n📊 Generating chart images from milestone data...")
            self.chart_generator.cleanup_old_images(days_old=7)  # Clean up old images first
            generated_images = await self.chart_generator.generate_milestone_charts(combined_data)
            print(f"Chart images generated. Number of images: {len(generated_images)}")
            print("\n🐦 Generating tweets from milestone responses...")
            tweets = []
            
            if responses and len(responses) > 0:
                
                # Process each milestone individually
                milestone_tweets = []
                milestone_count = 0
                
                # Process single-chain milestones
                single_chain = combined_data.get("single_chain_milestones", {})
                for chain, chain_data in single_chain.items():
                    for date, metrics in chain_data.items():
                        for metric, milestones_list in metrics.items():
                            for milestone in milestones_list:
                                milestone_count += 1
                                
                                # Create context for this specific milestone
                                milestone_context = self.format_single_milestone_for_tweets(milestone, chain, metric)
                                
                                if milestone_context:
                                    print(f"Generating tweet for milestone {milestone_count}: {chain} {metric}")
                                    variants = self.twitter_generator.generate_multiple_variants(milestone_context, num_variants=1)
                                    
                                    # Enhance each variant with the specific chain's social handle
                                    if self.chain_social_handles:
                                        for variant in variants:
                                            enhanced_text = self.enhance_tweet_with_social_handles(variant['tweet'], [chain])
                                            variant['tweet'] = enhanced_text
                                            variant['character_count'] = len(enhanced_text)
                                            variant['enhanced_with_social_handles'] = True
                                            variant['milestone_chain'] = chain
                                            variant['milestone_metric'] = metric
                                            variant['milestone_importance'] = milestone.get('total_importance', 0)
                                    
                                    milestone_tweets.extend(variants)
                
                # Process cross-chain milestones
                cross_chain = combined_data.get("cross_chain_milestones", {})
                for chain, chain_data in cross_chain.items():
                    for date, metrics in chain_data.items():
                        for metric, milestones_list in metrics.items():
                            for milestone in milestones_list:
                                milestone_count += 1
                                
                                # Create context for this specific milestone
                                milestone_context = self.format_single_milestone_for_tweets(milestone, chain, metric)
                                
                                if milestone_context:
                                    print(f"Generating tweet for milestone {milestone_count}: {chain} {metric}")
                                    variants = self.twitter_generator.generate_multiple_variants(milestone_context, num_variants=1)
                                    
                                    # Enhance each variant with the specific chain's social handle
                                    if self.chain_social_handles:
                                        for variant in variants:
                                            enhanced_text = self.enhance_tweet_with_social_handles(variant['tweet'], [chain])
                                            variant['tweet'] = enhanced_text
                                            variant['character_count'] = len(enhanced_text)
                                            variant['enhanced_with_social_handles'] = True
                                            variant['milestone_chain'] = chain
                                            variant['milestone_metric'] = metric
                                            variant['milestone_importance'] = milestone.get('total_importance', 0)
                                    
                                    milestone_tweets.extend(variants)
                
                tweets = milestone_tweets
                print(f"✅ Generated {len(tweets)} tweets (1 tweet per milestone, {milestone_count} milestones total)")
                
                if not tweets:
                    print("No milestone context available to generate tweets from.")
                    return None
            else:
                print("No milestone responses available to generate tweets from.")
                return None
            
            # Display results (organized by milestone)
            print("\n" + "="*60)
            print("📱 GENERATED TWEETS FROM MILESTONE RESPONSES")
            print("="*60)
            
            # Display tweets by milestone
            tweet_counter = 0
            
            for tweet in tweets:
                tweet_counter += 1
                chain = tweet.get('milestone_chain', 'Unknown').title()
                metric = tweet.get('milestone_metric', 'Unknown').upper()
                
                print(f"\n🎯 === Tweet #{tweet_counter}: {chain} {metric} Milestone ===")
                print(f"Characters: {tweet['character_count']}/280")
                print(tweet['tweet'])
            
            print(f"\n✅ Generated {len(tweets)} tweets and {len(generated_images)} chart images from milestone responses")
            
            # Send the original Discord embed (same as test_gtp_analyst.py)
            print("\nCrafting and sending Discord embed message...")
            title = "Layer 2 Blockchain Milestone Update"
            footer = f"Analysis as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data compiled from latest metrics"
            analytics.craft_and_send_discord_embeds(webhook_url, responses, title, footer)
            print("Discord embed message sent successfully.")
            
            # Match tweets with their most relevant charts and send combined messages
            if tweets and generated_images:
                print("\n🔗 Matching tweets with chart images...")
                matched_pairs = self.match_tweets_with_charts(tweets, generated_images, combined_data)
                print(f"✅ Matched {len(matched_pairs)} tweet-chart pairs")
                
                print("\n🚀 Sending combined tweet and chart messages to Discord...")
                await self.send_combined_tweet_and_chart(webhook_url, matched_pairs)
                print("Combined tweet and chart messages sent to Discord successfully.")
                
                # Clean up generated images after sending
                self.cleanup_generated_images(generated_images)
            elif tweets:
                print("\n🐦 No charts generated, sending tweets only...")
                await self.send_tweets_to_discord(webhook_url, tweets)
                print("Tweet messages sent to Discord successfully.")
            elif generated_images:
                print("\n📊 No tweets generated, charts were created but won't be sent alone")
            else:
                print("\n⚠️ No tweets or charts were generated")
            
            return {
                "tweets": tweets,
                "generated_images": generated_images,
                "responses": responses
            }
            
        except Exception as e:
            print(f"❌ Error in social media pipeline: {e}")
            return None

def main():
    """Main function to run social media automation"""
    # Check API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ OpenAI API key not found!")
        print("Please add OPENAI_API_KEY to your .env file")
        return
    
    automation = SocialMediaAutomation()
    print("🚀 Running simplified social media pipeline...")
    asyncio.run(automation.run_automated_social_pipeline())

if __name__ == "__main__":
    main() 