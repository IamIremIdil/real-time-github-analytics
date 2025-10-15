# GitHub Archive Live Dashboard - Production Ready
import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import aiohttp
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import redis
from collections import defaultdict, Counter
import threading
import queue
import requests
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RealGitHubDataProcessor:
    """Fetches and processes real GitHub data from GitHub Archive"""

    def __init__(self, redis_url='redis://localhost:6379'):
        self.redis_client = redis.from_url(redis_url)
        self.base_url = "https://data.gharchive.org"

    async def fetch_events_for_hour(self, date: datetime) -> List[Dict]:
        """Fetch events for a specific hour from GitHub Archive"""
        filename = f"{date.strftime('%Y-%m-%d-%-H')}.json.gz"
        url = f"{self.base_url}/{filename}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        import gzip
                        import io
                        content = await response.read()
                        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz_file:
                            data = gz_file.read().decode('utf-8')
                            events = [json.loads(line) for line in data.strip().split('\n') if line.strip()]
                            logger.info(f"Fetched {len(events)} events from {filename}")
                            return events
                    else:
                        logger.warning(f"Failed to fetch {url}: Status {response.status}")
                        return []
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return []

    async def stream_recent_events(self, hours_back: int = 2):
        """Stream events from the last N hours"""
        all_events = []
        current_time = datetime.utcnow()

        for i in range(hours_back):
            target_time = current_time - timedelta(hours=i + 1)
            events = await self.fetch_events_for_hour(target_time)
            all_events.extend(events)

        logger.info(f"Total events collected: {len(all_events)}")
        return all_events

    def cache_events(self, events: List[Dict]):
        """Cache events in Redis for dashboard access"""
        try:
            # Store recent events
            self.redis_client.set('recent_github_events', json.dumps(events[-1000:]))  # Last 1000 events
            self.redis_client.expire('recent_github_events', 3600)  # Expire in 1 hour

            # Update statistics
            self.update_statistics(events)

        except Exception as e:
            logger.error(f"Error caching events: {e}")

    def update_statistics(self, events: List[Dict]):
        """Calculate and cache basic statistics"""
        try:
            language_stats = Counter()
            repo_stats = Counter()
            event_stats = Counter()
            user_stats = Counter()

            for event in events:
                # Process event type
                event_type = event.get('type', 'Unknown')
                event_stats[event_type] += 1

                # Process repo
                repo = event.get('repo', {})
                repo_name = repo.get('name', 'unknown/unknown')
                repo_stats[repo_name] += 1

                # Process user
                actor = event.get('actor', {})
                username = actor.get('login', 'unknown')
                user_stats[username] += 1

                # Estimate language (simplified)
                language = self.estimate_language(repo_name, event.get('payload', {}))
                if language:
                    language_stats[language] += 1

            # Cache statistics
            stats = {
                'languages': dict(language_stats.most_common(10)),
                'repos': dict(repo_stats.most_common(10)),
                'events': dict(event_stats.most_common()),
                'users': dict(user_stats.most_common(10)),
                'total_events': len(events),
                'last_updated': datetime.now().isoformat()
            }

            self.redis_client.set('github_stats', json.dumps(stats))
            self.redis_client.expire('github_stats', 3600)  # 1 hour cache

        except Exception as e:
            logger.error(f"Error updating statistics: {e}")

    def estimate_language(self, repo_name: str, payload: Dict) -> Optional[str]:
        """Estimate programming language based on repo name and event data"""
        # Simple language detection based on repo patterns
        language_patterns = {
            'python': ['python', 'django', 'flask', 'fastapi', 'ml', 'ai', 'data-science'],
            'javascript': ['js', 'javascript', 'react', 'vue', 'node', 'angular'],
            'typescript': ['typescript', 'ts-', '-ts', 'tsconfig'],
            'java': ['java', 'spring', 'maven', 'gradle'],
            'go': ['go', 'golang'],
            'rust': ['rust', 'cargo'],
            'cpp': ['cpp', 'c++'],
            'csharp': ['csharp', 'c#', '.net'],
            'php': ['php', 'laravel', 'symfony'],
            'ruby': ['ruby', 'rails']
        }

        repo_lower = repo_name.lower()
        for lang, keywords in language_patterns.items():
            if any(keyword in repo_lower for keyword in keywords):
                return lang

        return None


class GitHubStreamProcessor:
    """Processes GitHub events and maintains real-time analytics"""

    def __init__(self, redis_url='redis://localhost:6379'):
        self.redis_client = redis.from_url(redis_url)
        self.data_processor = RealGitHubDataProcessor(redis_url)

        # Initialize with cached data if available
        self.load_cached_data()

    def load_cached_data(self):
        """Load cached data from Redis"""
        try:
            cached_stats = self.redis_client.get('github_stats')
            if cached_stats:
                self.cache = json.loads(cached_stats)
            else:
                self.cache = {
                    'last_updated': datetime.now().isoformat(),
                    'trending_repos': [],
                    'language_popularity': {},
                    'event_timeline': [],
                    'top_users': [],
                    'total_events': 0,
                    'event_breakdown': {}
                }
        except Exception as e:
            logger.error(f"Error loading cached data: {e}")
            self.cache = {
                'last_updated': datetime.now().isoformat(),
                'trending_repos': [],
                'language_popularity': {},
                'event_timeline': [],
                'top_users': [],
                'total_events': 0,
                'event_breakdown': {}
            }

    async def refresh_real_data(self):
        """Fetch and process real GitHub data"""
        try:
            st.info("🔄 Fetching real GitHub data... This may take a minute.")

            # Fetch recent events (last 2 hours)
            events = await self.data_processor.stream_recent_events(hours_back=2)

            if events:
                # Cache the events and update statistics
                self.data_processor.cache_events(events)

                # Reload cache
                self.load_cached_data()

                st.success(f"✅ Successfully loaded {len(events):,} real GitHub events!")
                return True
            else:
                st.warning("⚠️ No real data fetched. Using cached data or sample data.")
                return False

        except Exception as e:
            logger.error(f"Error refreshing real data: {e}")
            st.error(f"❌ Error fetching real data: {e}")
            return False

    def generate_sample_events(self, count: int = 100):
        """Generate sample events when real data is unavailable"""
        import random

        event_types = ['PushEvent', 'WatchEvent', 'ForkEvent', 'IssuesEvent',
                       'PullRequestEvent', 'CreateEvent', 'DeleteEvent']

        sample_repos = [
            'microsoft/vscode', 'facebook/react', 'tensorflow/tensorflow',
            'pytorch/pytorch', 'kubernetes/kubernetes', 'golang/go',
            'rust-lang/rust', 'python/cpython', 'nodejs/node',
            'angular/angular', 'vuejs/vue', 'laravel/laravel'
        ]

        sample_users = [f'developer_{i}' for i in range(1, 21)]
        languages = ['python', 'javascript', 'java', 'typescript', 'go', 'rust', 'cpp']

        # Update cache with sample data
        self.cache.update({
            'trending_repos': [[repo, random.randint(10, 100)] for repo in sample_repos[:8]],
            'language_popularity': {lang: random.randint(20, 200) for lang in languages},
            'top_users': [[user, random.randint(5, 50)] for user in sample_users[:10]],
            'total_events': count * 2,
            'event_breakdown': {event: random.randint(10, 100) for event in event_types},
            'last_updated': datetime.now().isoformat()
        })


# Global processor instance
processor = GitHubStreamProcessor()


def create_dashboard():
    """Create the Streamlit dashboard"""

    st.set_page_config(
        page_title="GitHub Archive Live Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Custom CSS for better styling
    st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stMetric { 
        background-color: #16c3f7;
        padding: 1rem;
         color: white;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
    }

    /* Change delta (counter) color to #2260bd */
    [data-testid="stMetricDelta"] svg {
        color: #2260bd !important;
    }

    /* Optional: Also style the delta text */
    [data-testid="stMetricDelta"] div {
        color: #2260bd !important;
        font-weight: bold;
    }

    .top-github-link {
        color: #2260bd;
        font-weight: 600;
        text-decoration: none;
        position: relative;
        transition: all 0.3s ease;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        font-size: 12px;
    }
    .top-github-link:hover {
        color: #ff6b6b;
    }
    </style>
    """, unsafe_allow_html=True)

    # Header
    st.markdown('<h1 class="main-header">🚀 GitHub Archive Live Dashboard</h1>',
                unsafe_allow_html=True)

    st.markdown("---")

    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Dashboard Controls")

        # GitHub link
        st.markdown(
            """
            <div style='text-align: left; margin-bottom: 30px; margin-left: 0px;'>
                <a href="https://github.com/IamIremIdil" target="_blank" class="top-github-link">
                    🐙 My GitHub
                </a>
            </div>
            """,
            unsafe_allow_html=True
        )

        # Data source selection
        data_source = st.radio(
            "📊 Data Source",
            ["Real GitHub Data", "Sample Data"],
            index=0
        )

        # Auto-refresh toggle
        auto_refresh = st.checkbox("🔄 Auto Refresh", value=False)
        refresh_interval = st.slider("Refresh Interval (seconds)", 10, 300, 60)

        # Manual refresh button
        if st.button("🔄 Refresh Data"):
            if data_source == "Real GitHub Data":
                # Use async execution for real data fetching
                import asyncio
                asyncio.run(processor.refresh_real_data())
            else:
                processor.generate_sample_events(100)
                st.success("Sample data refreshed!")

        st.markdown("---")
        st.markdown("### 📈 System Status")

        last_updated = processor.cache.get('last_updated', 'Never')
        if isinstance(last_updated, str):
            try:
                last_updated = datetime.fromisoformat(last_updated)
            except:
                last_updated = datetime.now()

        st.info(f"Last Updated: {last_updated.strftime('%H:%M:%S')}")
        st.info(f"Total Events: {processor.cache.get('total_events', 0):,}")

    # Main dashboard content
    if processor.cache.get('total_events', 0) == 0:
        st.warning("No data available. Click 'Refresh Data' to start!")

        # Auto-load sample data if no data exists
        if st.button("Load Sample Data"):
            processor.generate_sample_events(100)
            st.rerun()
        return

    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_events = processor.cache.get('total_events', 0)
        st.metric(
            label="🎯 Total Events",
            value=f"{total_events:,}",
            delta=None
        )

    with col2:
        languages = processor.cache.get('language_popularity', {})
        top_language = max(languages, key=languages.get) if languages else "N/A"
        top_lang_count = languages.get(top_language, 0) if languages else 0
        st.metric(
            label="🔥 Top Language",
            value=top_language.title(),
            delta=f"{top_lang_count} events"
        )

    with col3:
        trending_repos = processor.cache.get('trending_repos', [])
        trending_repo = trending_repos[0][0] if trending_repos else "N/A"
        trending_repo_count = trending_repos[0][1] if trending_repos else 0
        st.metric(
            label="⭐ Trending Repo",
            value=trending_repo.split('/')[-1] if '/' in trending_repo else trending_repo,
            delta=f"{trending_repo_count} events"
        )

    with col4:
        top_users = processor.cache.get('top_users', [])
        top_user = top_users[0][0] if top_users else "N/A"
        top_user_count = top_users[0][1] if top_users else 0
        st.metric(
            label="👑 Top User",
            value=top_user,
            delta=f"{top_user_count} events"
        )

    st.markdown("---")

    # Charts row 1
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Language Popularity")
        languages = processor.cache.get('language_popularity', {})
        if languages:
            lang_df = pd.DataFrame(
                list(languages.items()),
                columns=['Language', 'Events']
            )
            fig = px.bar(
                lang_df,
                x='Language',
                y='Events',
                title="Most Active Programming Languages",
                color='Events',
                color_continuous_scale='viridis'
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No language data available yet.")

    with col2:
        st.subheader("🏆 Trending Repositories")
        trending_repos = processor.cache.get('trending_repos', [])
        if trending_repos:
            repo_df = pd.DataFrame(
                trending_repos[:8],
                columns=['Repository', 'Events']
            )
            repo_df['Repository'] = repo_df['Repository'].str.split('/').str[-1]

            fig = px.pie(
                repo_df,
                values='Events',
                names='Repository',
                title="Most Active Repositories"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No repository data available yet.")

    # Charts row 2
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 Event Types Breakdown")
        event_breakdown = processor.cache.get('event_breakdown', {})
        if event_breakdown:
            event_df = pd.DataFrame(
                list(event_breakdown.items()),
                columns=['Event Type', 'Count']
            )
            fig = px.bar(
                event_df,
                x='Count',
                y='Event Type',
                title="GitHub Event Types Distribution",
                color='Count',
                color_continuous_scale='plasma',
                orientation='h'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No event data available yet.")

    with col2:
        st.subheader("👥 Top Active Users")
        top_users = processor.cache.get('top_users', [])
        if top_users:
            user_df = pd.DataFrame(
                top_users[:10],
                columns=['Username', 'Activity']
            )
            fig = px.scatter(
                user_df,
                x='Username',
                y='Activity',
                size='Activity',
                title="Most Active GitHub Users",
                color='Activity',
                color_continuous_scale='blues'
            )
            fig.update_layout(height=400)
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No user activity data available yet.")

    # Data tables section
    st.markdown("---")
    st.subheader("📋 Detailed Statistics")

    tab1, tab2, tab3 = st.tabs(["🏆 Top Repositories", "💻 Language Stats", "👤 User Activity"])

    with tab1:
        trending_repos = processor.cache.get('trending_repos', [])
        if trending_repos:
            repo_table_df = pd.DataFrame(
                trending_repos,
                columns=['Repository', 'Events']
            )
            repo_table_df.index = range(1, len(repo_table_df) + 1)
            st.dataframe(repo_table_df, use_container_width=True)
        else:
            st.info("No repository data available.")

    with tab2:
        languages = processor.cache.get('language_popularity', {})
        if languages:
            lang_table_df = pd.DataFrame(
                list(languages.items()),
                columns=['Programming Language', 'Total Events']
            )
            lang_table_df.index = range(1, len(lang_table_df) + 1)
            st.dataframe(lang_table_df, use_container_width=True)
        else:
            st.info("No language data available.")

    with tab3:
        top_users = processor.cache.get('top_users', [])
        if top_users:
            user_table_df = pd.DataFrame(
                top_users,
                columns=['Username', 'Total Activity']
            )
            user_table_df.index = range(1, len(user_table_df) + 1)
            st.dataframe(user_table_df, use_container_width=True)
        else:
            st.info("No user activity data available.")

    # Footer
    footer_html = """
    <style>
    .footer {
        text-align: center;
        color: #666666;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        font-size: 14px;
        margin-top: 3rem;
        padding: 1.5rem;
        border-top: 1px solid #e0e0e0;
    }
    .github-link {
        color: #2260bd;
        font-weight: 600;
        text-decoration: none;
        position: relative;
        transition: all 0.3s ease;
    }
    .github-link:hover {
        color: #ff6b6b;
    }
    </style>

    <div class="footer">
        Crafted with ❤️ by 
        <a href="https://github.com/IamIremIdil" target="_blank" class="github-link">iro</a> 
        © 2025
    </div>
    """

    st.markdown("---")
    st.markdown(footer_html, unsafe_allow_html=True)

    # Auto-refresh functionality
    if auto_refresh:
        time.sleep(refresh_interval)
        if data_source == "Real GitHub Data":
            # Note: Auto-refresh for real data might be too intensive
            st.info("Auto-refresh for real data is limited to prevent rate limiting")
        else:
            processor.generate_sample_events(20)
        st.rerun()


# Production Components
class ProductionComponents:
    """Production-ready components reference"""

    @staticmethod
    def get_fastapi_code():
        return """
# api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis
import json
import os

app = FastAPI(title="GitHub Analytics API")
redis_client = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "GitHub Analytics API"}

@app.get("/api/stats")
async def get_stats():
    stats = redis_client.get('github_stats')
    return json.loads(stats) if stats else {}
"""

    @staticmethod
    def get_docker_compose():
        return """
# docker-compose.yml
version: '3.8'
services:
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  api:
    build: ./api
    ports:
      - "8000:8000"
    environment:
      - REDIS_URL=redis://redis:6379
    depends_on:
      - redis

  dashboard:
    build: ./dashboard
    ports:
      - "8501:8501"
    environment:
      - REDIS_URL=redis://redis:6379
    depends_on:
      - redis

volumes:
  redis_data:
"""


if __name__ == "__main__":
    # Initialize with sample data if no real data exists
    if processor.cache.get('total_events', 0) == 0:
        processor.generate_sample_events(100)

    # Run the dashboard
    create_dashboard()