#!/bin/bash

# Setup script for Jekyll site
# This script installs all necessary dependencies for running the site locally

set -e  # Exit on error

echo "🚀 Setting up Julian Jurai's personal website..."
echo ""

# Check if Homebrew is installed
if ! command -v brew &> /dev/null; then
    echo "❌ Homebrew is not installed. Please install it first:"
    echo "   /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
    exit 1
fi

echo "✅ Homebrew is installed"

# Check Ruby version
RUBY_VERSION=$(ruby --version | grep -o '[0-9]\+\.[0-9]\+' | head -n 1)
REQUIRED_VERSION="2.7"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$RUBY_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    echo "⚠️  Current Ruby version ($RUBY_VERSION) is too old. Installing Ruby via Homebrew..."
    brew install ruby
    echo ""
    echo "📝 Adding Ruby to PATH..."

    # Add to shell config
    if [ -f "$HOME/.zshrc" ]; then
        if ! grep -q "/opt/homebrew/opt/ruby/bin" "$HOME/.zshrc"; then
            echo 'export PATH="/opt/homebrew/opt/ruby/bin:$PATH"' >> ~/.zshrc
            echo 'export PATH="/opt/homebrew/lib/ruby/gems/4.0.0/bin:$PATH"' >> ~/.zshrc
        fi
        SHELL_CONFIG="~/.zshrc"
    elif [ -f "$HOME/.bash_profile" ]; then
        if ! grep -q "/opt/homebrew/opt/ruby/bin" "$HOME/.bash_profile"; then
            echo 'export PATH="/opt/homebrew/opt/ruby/bin:$PATH"' >> ~/.bash_profile
            echo 'export PATH="/opt/homebrew/lib/ruby/gems/4.0.0/bin:$PATH"' >> ~/.bash_profile
        fi
        SHELL_CONFIG="~/.bash_profile"
    fi

    # Export for current session
    export PATH="/opt/homebrew/opt/ruby/bin:$PATH"
    export PATH="/opt/homebrew/lib/ruby/gems/4.0.0/bin:$PATH"

    echo "✅ Ruby path added to $SHELL_CONFIG"
else
    echo "✅ Ruby version $RUBY_VERSION is sufficient"
fi

# Install Jekyll and Bundler
echo ""
echo "📦 Installing Jekyll and Bundler..."

if ! command -v jekyll &> /dev/null; then
    gem install jekyll bundler
    echo "✅ Jekyll and Bundler installed"
else
    echo "✅ Jekyll is already installed"
fi

# Check if Gemfile exists, if so run bundle install
if [ -f "Gemfile" ]; then
    echo ""
    echo "📦 Installing gems from Gemfile..."
    bundle install
fi

echo ""
echo "✨ Setup complete!"
echo ""
echo "To start the development server, run:"
echo "  ./start.sh"
echo ""
echo "Or manually with:"
echo "  jekyll serve"
echo ""
echo "Then visit: http://127.0.0.1:4000/"
echo ""
