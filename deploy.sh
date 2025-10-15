#!/bin/bash

echo "🚀 Deploying GitHub Analytics Dashboard..."

# Build and start services
docker-compose down
docker-compose up -d --build

echo "✅ Services deployed!"
echo "📊 Dashboard: http://localhost:8501"
echo "🔌 API: http://localhost:8000"
echo "📚 API Docs: http://localhost:8000/docs"