"""
GitHub Analytics API
MIT License
Copyright (c) 2025 [Your Name]
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import redis
import json
import os
from datetime import datetime
from typing import Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="GitHub Analytics API",
    description="Real-time GitHub activity analytics API with Redis caching",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Redis connection
try:
    redis_client = redis.from_url(
        os.getenv('REDIS_URL', 'redis://localhost:6379'),
        decode_responses=True
    )
    # Test connection
    redis_client.ping()
    logger.info("✅ Redis connected successfully")
except Exception as e:
    logger.error(f"❌ Redis connection failed: {e}")
    redis_client = None

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "GitHub Analytics API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "docs": "/docs",
            "health": "/api/health",
            "stats": "/api/stats",
            "trending_repos": "/api/trending/repos",
            "languages": "/api/languages/popularity",
            "events": "/api/events/breakdown",
            "users": "/api/users/top"
        }
    }


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        redis_status = "connected" if redis_client and redis_client.ping() else "disconnected"
        return {
            "status": "healthy",
            "redis": redis_status,
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0"
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis disconnected: {e}")


@app.get("/api/stats")
async def get_stats():
    """Get all GitHub statistics"""
    try:
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis not available")

        stats_data = redis_client.get('github_stats')
        if stats_data:
            stats = json.loads(stats_data)
            return {
                "success": True,
                "data": stats,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "success": True,
                "data": get_empty_stats(),
                "message": "No data available yet",
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trending/repos")
async def get_trending_repos(limit: int = 10):
    """Get trending repositories"""
    try:
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis not available")

        stats_data = redis_client.get('github_stats')
        if stats_data:
            stats = json.loads(stats_data)
            repos = stats.get('repos', {})
            # Convert to list and sort by activity
            repos_list = [{"name": name, "activity": count} for name, count in repos.items()]
            repos_list.sort(key=lambda x: x['activity'], reverse=True)

            return {
                "success": True,
                "data": repos_list[:limit],
                "total": len(repos_list),
                "timestamp": datetime.now().isoformat()
            }
        return {
            "success": True,
            "data": [],
            "message": "No repository data available",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting trending repos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/languages/popularity")
async def get_language_popularity():
    """Get programming language popularity"""
    try:
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis not available")

        stats_data = redis_client.get('github_stats')
        if stats_data:
            stats = json.loads(stats_data)
            languages = stats.get('languages', {})

            return {
                "success": True,
                "data": languages,
                "total_languages": len(languages),
                "timestamp": datetime.now().isoformat()
            }
        return {
            "success": True,
            "data": {},
            "message": "No language data available",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting language popularity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/events/breakdown")
async def get_event_breakdown():
    """Get event type breakdown"""
    try:
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis not available")

        stats_data = redis_client.get('github_stats')
        if stats_data:
            stats = json.loads(stats_data)
            events = stats.get('events', {})

            return {
                "success": True,
                "data": events,
                "total_event_types": len(events),
                "timestamp": datetime.now().isoformat()
            }
        return {
            "success": True,
            "data": {},
            "message": "No event data available",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting event breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/users/top")
async def get_top_users(limit: int = 10):
    """Get top active users"""
    try:
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis not available")

        stats_data = redis_client.get('github_stats')
        if stats_data:
            stats = json.loads(stats_data)
            users = stats.get('users', {})
            # Convert to list and sort by activity
            users_list = [{"username": name, "activity": count} for name, count in users.items()]
            users_list.sort(key=lambda x: x['activity'], reverse=True)

            return {
                "success": True,
                "data": users_list[:limit],
                "total": len(users_list),
                "timestamp": datetime.now().isoformat()
            }
        return {
            "success": True,
            "data": [],
            "message": "No user data available",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting top users: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/system/info")
async def get_system_info():
    """Get system information and cache status"""
    try:
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis not available")

        # Get cache information
        stats_data = redis_client.get('github_stats')
        cache_exists = stats_data is not None

        if cache_exists:
            stats = json.loads(stats_data)
            last_updated = stats.get('last_updated', 'Unknown')
            total_events = stats.get('total_events', 0)
        else:
            last_updated = 'Never'
            total_events = 0

        return {
            "success": True,
            "data": {
                "cache_status": "active" if cache_exists else "empty",
                "last_updated": last_updated,
                "total_events_cached": total_events,
                "redis_connected": True,
                "timestamp": datetime.now().isoformat()
            }
        }
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def get_empty_stats() -> Dict[str, Any]:
    """Return empty statistics structure"""
    return {
        'languages': {},
        'repos': {},
        'events': {},
        'users': {},
        'hourly_stats': {},
        'total_events': 0,
        'last_updated': datetime.now().isoformat(),
        'events_processed': 0
    }


# Error handlers
@app.exception_handler(500)
async def internal_server_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": "Internal server error",
            "message": str(exc)
        }
    )


@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={
            "success": False,
            "error": "Endpoint not found",
            "message": "The requested endpoint does not exist"
        }
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )