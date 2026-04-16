#!/bin/bash

# Start script for Jekyll development server
# Ensures the correct Ruby is in PATH before starting

# Add Homebrew Ruby to PATH
export PATH="/opt/homebrew/opt/ruby/bin:$PATH"
export PATH="/opt/homebrew/lib/ruby/gems/4.0.0/bin:$PATH"

echo "🚀 Starting Jekyll development server..."
echo ""
echo "Site will be available at: http://127.0.0.1:4000/"
echo "Press Ctrl+C to stop the server"
echo ""

jekyll serve --livereload
