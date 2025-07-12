import os
import sys
from pathlib import Path

def create_file(path, content=""):
    """Safely create a file with content."""
    try:
        dir_name = os.path.dirname(path)
        if dir_name:  # Only create directory if path is not root
            os.makedirs(dir_name, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        return 1  # Count created file
    except Exception as e:
        print(f"Error creating {path}: {str(e)}")
        sys.exit(1)

def create_repo_structure():
    """Create the complete production-ready structure in current directory."""
    files_created = 0

    directories = [
        "data/raw/telegrammessages/YYYY-MM-DD",
        "data/processed",
        "docs",
        "logs",
        "tests",
        ".github/workflows",
        "src/scraper",
        "src/dbt_project/models/staging",
        "src/dbt_project/models/marts",
        "src/dbt_project/macros",
        "src/dbt_project/tests",
        "src/yolo",
        "src/api",
        "src/dagster",
        "docker"
    ]

    for dir_path in directories:
        os.makedirs(dir_path, exist_ok=True)
        Path(f"{dir_path}/.gitkeep").touch()

    files = {
        "data/raw/telegrammessages/YYYY-MM-DD/channelname.json": "{}",
        "docs/pipeline_diagram.png": "",
        "docs/star_schema_diagram.png": "",
        "src/scraper/telegram_scraper.py": "# Telegram scraper logic",
        "src/scraper/utils.py": "# Utility functions for scraper",
        "src/dbt_project/models/staging/stg_telegrammessages.sql": "-- Staging SQL",
        "src/dbt_project/models/marts/dim_channels.sql": "-- Dimension channels",
        "src/dbt_project/models/marts/dim_dates.sql": "-- Dimension dates",
        "src/dbt_project/models/marts/fct_messages.sql": "-- Fact messages",
        "src/yolo/yolo_object_detection.py": "# YOLO object detection",
        "src/api/main.py": "# FastAPI main entry",
        "src/api/database.py": "# Database connection setup",
        "src/api/models.py": "# Pydantic models",
        "src/api/schemas.py": "# API schemas",
        "src/api/crud.py": "# CRUD operations",
        "src/dagster/pipeline.py": "# Dagster pipeline definition",
        "src/dagster/schedule.py": "# Dagster schedule configuration",
        ".env.example": "# Environment variables",
        ".gitignore": "*.pyc\n__pycache__/\n.env\n",
        "requirements.txt": "# Python dependencies",
        "README.md": "# Project README",
        "docker/Dockerfile": "# Dockerfile content",
        "docker/docker-compose.yml": "# Docker Compose config",
    }

    for path, content in files.items():
        files_created += create_file(path, content)

    print("\n✅ Setup completed successfully!")
    print(f"→ Created {len(directories)} directories")
    print(f"→ Generated {files_created} files")

if __name__ == "__main__":
    try:
        create_repo_structure()
    except Exception as e:
        print(f"\n❌ Setup failed: {str(e)}")
        sys.exit(1)
