#!/bin/bash

# Data Migration Docker Development Startup Script with Hot Reload
echo "=================================================="
echo "Starting Data Migration Application (DEV MODE)..."
echo "🔥 Hot Reload Enabled - Your changes will be"
echo "   automatically reflected without rebuilding!"
echo "=================================================="

# Stop any running containers first
echo "🧹 Cleaning up any existing containers..."
docker compose -f docker-compose.dev.yml down 2>/dev/null || true

# Check if Docker Desktop or Docker Compose supports watch
if ! docker compose version | grep -q "v2\|version 2"; then
    echo "⚠️  Warning: Docker Compose V2 is required for watch functionality"
    echo "   Using fallback development mode..."
    docker-compose -f docker-compose.dev.yml up --build
else
    echo "🚀 Starting with Docker Watch..."
    echo "📁 Project files will be synchronized automatically"
    echo "🔄 .csproj changes will trigger rebuilds"
    echo ""
    
    # Build and start the container with watch mode
    docker compose -f docker-compose.dev.yml watch
fi

echo ""
echo "=================================================="
