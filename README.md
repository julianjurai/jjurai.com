# Julian Jurai - Personal Website

A modern personal website showcasing my work as a Senior Software Engineer, technical blog posts, and professional experience.

## About

This is my personal website built with Jekyll. It features:

- **Professional Profile**: Updated information about my work at Breezeway, technical skills, and background
- **Knowledge Graph Blog**: A chronological blog system where posts are tagged for easy navigation across various topics and domains
- **Resume**: Direct link to downloadable PDF resume
- **Projects**: Links to GitHub projects and professional portfolio

## Technical Stack

- **Static Site Generator**: Jekyll
- **Hosting**: GitHub Pages
- **Styling**: SCSS with clean, retro design
- **Features**:
  - Chronological blog with tags
  - Automatic dark mode support
  - Responsive design
  - Optimized for readability and accessibility

## Blog Structure

The blog is designed as an interconnected knowledge graph where posts are tagged by topic, allowing readers to explore related content. Posts are displayed chronologically with tags and dates for easy navigation.

## Quick Start

### Prerequisites

- macOS with [Homebrew](https://brew.sh/) installed
- Git

### Automated Setup (Recommended)

1. Clone the repository:
```bash
git clone https://github.com/Julian-Jurai/jjurai.com.git
cd jjurai.com
```

2. Run the setup script (installs Ruby and Jekyll):
```bash
./setup.sh
```

3. Start the development server:
```bash
./start.sh
```

4. Open your browser to `http://127.0.0.1:4000/`

The setup script will:
- Check for Homebrew
- Install Ruby 4.0+ if needed (system Ruby is too old)
- Add Ruby to your PATH in `.zshrc` or `.bash_profile`
- Install Jekyll and Bundler
- Set up all dependencies

### Manual Setup

If you prefer to set up manually or are not on macOS:

1. **Install Ruby 2.7 or higher**
   - On macOS with Homebrew: `brew install ruby`
   - On Linux: Use your package manager or [rbenv](https://github.com/rbenv/rbenv)
   - On Windows: Use [RubyInstaller](https://rubyinstaller.org/)

2. **Add Ruby to PATH** (macOS with Homebrew Ruby):
   ```bash
   echo 'export PATH="/opt/homebrew/opt/ruby/bin:$PATH"' >> ~/.zshrc
   echo 'export PATH="/opt/homebrew/lib/ruby/gems/4.0.0/bin:$PATH"' >> ~/.zshrc
   source ~/.zshrc
   ```

3. **Install Jekyll and Bundler**:
   ```bash
   gem install jekyll bundler
   ```

4. **Start the development server**:
   ```bash
   jekyll serve
   ```

5. Visit `http://127.0.0.1:4000/`

### Development Scripts

**`./setup.sh`** - One-time setup
- Checks for Homebrew
- Installs Ruby 4.0+ if needed
- Installs Jekyll and Bundler
- Configures PATH in shell profile

**`./start.sh`** - Start development server
- Sets up Ruby environment
- Starts Jekyll with live reload
- Available at http://127.0.0.1:4000/

### Development Commands

```bash
# Start server with live reload (recommended)
./start.sh

# Or manually
jekyll serve --livereload

# Build site without serving
jekyll build

# Clean generated files
jekyll clean
```

## Creating New Blog Posts

Create a new markdown file in the `_posts` directory with the format:
```
YYYY-MM-DD-Title-Of-Post.md
```

Include front matter with tags:
```yaml
---
layout: post
title: Your Post Title
tags: [coding, finance, history]
---

Your content here...
```

## Project Structure

```
├── _includes/          # Reusable components (header, footer, etc.)
├── _layouts/           # Page templates (default, page, post)
├── _posts/             # Blog posts in Markdown
├── _sass/              # Sass partials (_variables, _reset, etc.)
├── assets/images/      # Images and static assets
├── style.scss          # Main stylesheet
├── index.html          # Homepage with about section
├── blog.html           # Blog listing page with tag filtering
├── setup.sh            # Automated setup script
├── start.sh            # Start development server script
├── _config.yml         # Jekyll configuration
└── README.md           # This file
```

## Deployment

### GitHub Pages

This site is designed to work with GitHub Pages:

1. Push your changes to the `master` branch
2. GitHub Pages will automatically build and deploy
3. Site will be live at `https://yourusername.github.io/`

### Custom Domain

To use a custom domain:
1. Add a `CNAME` file with your domain name
2. Configure DNS records with your domain provider
3. Update `url` in `_config.yml`

## Troubleshooting

### Jekyll Command Not Found

If `jekyll` command is not found after installation:

1. Ensure Ruby gems bin directory is in your PATH:
   ```bash
   echo 'export PATH="/opt/homebrew/lib/ruby/gems/4.0.0/bin:$PATH"' >> ~/.zshrc
   source ~/.zshrc
   ```

2. Or run the setup script again: `./setup.sh`

### Ruby Version Too Old

If you see errors about Ruby version:

```bash
brew install ruby
# Then add to PATH as shown in setup.sh
```

### Permission Errors

Don't use `sudo` with gem commands. Instead:
- Install Ruby via Homebrew (not system Ruby)
- Or use `gem install --user-install`

### Port Already in Use

If port 4000 is already in use:

```bash
# Find and kill the process
lsof -ti:4000 | xargs kill -9

# Or use a different port
jekyll serve --port 4001
```

## Design Philosophy

The site uses a simple, retro HTML design inspired by classic web aesthetics:
- Clean, bordered sections with minimal styling
- Classic blue hyperlinks (unvisited: #0000ee, visited: #551a8b)
- Simple gray/black color scheme
- No fancy animations or transitions
- Straightforward, readable typography
- Functional, no-nonsense layout

## Contact

- **Email**: drifter.pump.17@icloud.com
- **GitHub**: [@Julian-Jurai](https://github.com/Julian-Jurai)
- **LinkedIn**: [julian-jurai](https://linkedin.com/in/julian-jurai)
- **Location**: Boston, MA

## License

This is a personal website. Feel free to fork and adapt for your own use, but please don't copy content directly.
