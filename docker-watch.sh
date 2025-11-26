#!/bin/bash

# Docker Watch Management Script
case "$1" in
    start)
        echo "🔥 Starting Docker Watch mode..."
        echo "Changes to your code will trigger automatic rebuilds!"
        echo ""
        docker compose -f docker-compose.dev.yml watch
        ;;
    stop)
        echo "🛑 Stopping Docker Watch mode..."
        docker compose -f docker-compose.dev.yml down
        echo "✅ Watch mode stopped."
        ;;
    restart)
        echo "🔄 Restarting Docker Watch mode..."
        docker compose -f docker-compose.dev.yml down
        sleep 2
        docker compose -f docker-compose.dev.yml watch
        ;;
    logs)
        echo "📋 Showing logs..."
        docker compose -f docker-compose.dev.yml logs -f datamigration
        ;;
    *)
        echo "Docker Watch Management Script"
        echo ""
        echo "Usage: $0 {start|stop|restart|logs}"
        echo ""
        echo "Commands:"
        echo "  start   - Start the application with hot reload"
        echo "  stop    - Stop the watch mode"
        echo "  restart - Restart the watch mode"
        echo "  logs    - View application logs"
        echo ""
        echo "🌐 Application URL: http://localhost:5000/Migration"
        exit 1
        ;;
esac
