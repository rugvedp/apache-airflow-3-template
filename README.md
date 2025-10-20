# Airflow 3 Reddit Trending Posts Pipeline

A modern Apache Airflow 3.1.0 template for scraping and analyzing Reddit trending posts with PostgreSQL storage, built with Docker Compose for easy deployment.

## 🚀 Features

- **Airflow 3.1.0** with TaskFlow API and modern architecture
- **Reddit API Integration** for scraping trending posts from multiple subreddits
- **PostgreSQL Database** with comprehensive schema for post metadata
- **Docker Compose** setup for easy deployment and scaling
- **Celery Executor** for distributed task processing
- **Redis** for task queue management
- **PgAdmin** for database administration
- **Flower** for Celery monitoring
- **Rate Limiting** and error handling
- **Configurable Subreddits** via environment variables

## 📋 Prerequisites

- Docker and Docker Compose
- At least 4GB RAM
- At least 2 CPU cores
- 10GB free disk space

## 🛠️ Quick Start

### Option A: Use This Template

```bash
git clone https://github.com/rugvedp/apache-airflow-3-template.git
cd apache-airflow-3-template
```

### Option B: Start Fresh with Official Template

If you want to start from the official Airflow 3.1.0 template:

```bash
# Download the official Airflow 3.1.0 docker-compose.yaml
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.0/docker-compose.yaml'

# Create required directories
mkdir -p dags logs plugins config
```

### 2. Initialize Environment

```bash
make env
```

This creates the required directories and `.env` file with your user ID.

### 3. Initialize Airflow

```bash
make init
```

This initializes the Airflow database and creates the default admin user (username: `airflow`, password: `airflow`).

### 4. Start Services

```bash
make upd
```

This starts all services in detached mode.

### 5. Access the Web Interface

- **Airflow UI**: http://localhost:8080 (airflow/airflow)
- **PgAdmin**: http://localhost:5050 (admin@admin.com/admin)
- **Flower**: http://localhost:5555 (optional, for Celery monitoring)

#### Airflow Dashboard
![Airflow Dashboard](public/dashboard.png)

#### DAG Execution View
![DAG Test Execution](public/dag_test.png)

## 🏗️ Architecture

### Services

- **airflow-apiserver**: REST API server
- **airflow-scheduler**: Task scheduler
- **airflow-dag-processor**: DAG parsing and validation
- **airflow-worker**: Celery worker for task execution
- **airflow-triggerer**: Deferred task triggerer
- **postgres**: PostgreSQL database
- **redis**: Message broker for Celery
- **pgadmin**: Database administration interface
- **flower**: Celery monitoring dashboard

### DAG Structure

The `reddit_trending_pipeline` DAG includes:

1. **get_config**: Retrieves configuration and timezone settings
2. **get_subreddits**: Reads subreddit list from environment variables
3. **fetch_subreddit**: Scrapes posts from each subreddit (parallel execution)
4. **combine_results**: Merges results from all subreddits
5. **persist_and_output**: Saves data to PostgreSQL and outputs JSON

## 📊 Database Schema

The pipeline creates a comprehensive `reddit_posts` table with the following fields:

```sql
CREATE TABLE reddit_posts (
    id SERIAL PRIMARY KEY,
    post_id VARCHAR(32) UNIQUE NOT NULL,
    subreddit VARCHAR(100) NOT NULL,
    title TEXT,
    description TEXT,
    upvotes INT,
    downvotes INT,
    score INT,
    url TEXT,
    author VARCHAR(255),
    author_fullname VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE,
    created_utc BIGINT,
    num_comments INT,
    over_18 BOOLEAN,
    pinned BOOLEAN,
    distinguished VARCHAR(64),
    locked BOOLEAN,
    spoiler BOOLEAN,
    stickied BOOLEAN,
    flair_text TEXT,
    flair_css TEXT,
    thumbnail TEXT,
    thumbnail_width INT,
    thumbnail_height INT,
    is_video BOOLEAN,
    media_urls TEXT,
    post_hint VARCHAR(128),
    domain VARCHAR(255),
    subreddit_subscribers INT,
    upvote_ratio REAL,
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file or set these environment variables:

```bash
# Required
AIRFLOW_UID=50000

# Optional Reddit Configuration
REDDIT_SUBREDDITS='["MusicIndia", "IndiaTech", "worldnews", "programming"]'
REDDIT_REQUEST_DELAY_S=2
REDDIT_DB_CONN=postgresql://airflow:airflow@postgres:5432/airflow

# Optional Airflow Configuration
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

### DAG Configuration

The DAG runs daily at 6 AM (Asia/Kolkata timezone) and:
- Scrapes posts from the last 24 hours
- Filters posts with at least 100 upvotes
- Includes rate limiting (2-second delay between requests)
- Handles media URLs (images, videos, galleries)
- Supports conflict resolution for duplicate posts

## 🎯 Usage

### Manual DAG Execution

1. Access the Airflow UI at http://localhost:8080
2. Navigate to the `reddit_trending_pipeline` DAG
3. Click "Trigger DAG" to run manually
4. Monitor task execution in the Graph or Tree view

### Customizing Subreddits

Set the `REDDIT_SUBREDDITS` environment variable:

```bash
export REDDIT_SUBREDDITS='["MusicIndia", "IndiaTech", "worldnews", "programming", "MachineLearning"]'
```

### Database Queries

Connect to PostgreSQL via PgAdmin or directly:

```sql
-- View recent posts
SELECT subreddit, title, upvotes, created_at 
FROM reddit_posts 
ORDER BY created_at DESC 
LIMIT 10;

-- Top posts by subreddit
SELECT subreddit, COUNT(*) as post_count, AVG(upvotes) as avg_upvotes
FROM reddit_posts 
GROUP BY subreddit 
ORDER BY post_count DESC;

-- Trending posts (high upvote ratio)
SELECT title, subreddit, upvotes, upvote_ratio
FROM reddit_posts 
WHERE upvote_ratio > 0.8 
ORDER BY upvotes DESC;
```

## 🛠️ Development

### Makefile Commands

```bash
make help          # Show all available commands
make env           # Create directories and .env file
make init          # Initialize Airflow database
make up            # Start services (foreground)
make upd           # Start services (detached)
make down          # Stop services
make restart       # Restart all services
make clean         # Remove containers, images, and volumes
make shell         # Open bash shell in worker container
make cli ARGS="..." # Run Airflow CLI commands
```

### Adding New DAGs

1. Place your DAG file in the `dags/` directory
2. The DAG will be automatically detected by the DAG processor
3. Use the TaskFlow API for modern Python DAGs:

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="my_custom_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)
def my_dag():
    @task
    def my_task():
        return "Hello World"
    
    my_task()

dag = my_dag()
```

### Custom Dependencies

Add Python packages to the `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yaml`:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-pandas==2.1.3 psycopg2-binary==2.9.9 requests==2.31.0 your-package==1.0.0}
```

## 🔧 Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure `AIRFLOW_UID` is set correctly
2. **Memory Issues**: Increase Docker memory allocation
3. **Database Connection**: Check PostgreSQL service is running
4. **DAG Not Appearing**: Check DAG syntax and restart DAG processor

### Logs

View logs for specific tasks:
- Airflow UI → DAG → Task Instance → Log
- Or via CLI: `make cli ARGS="tasks logs reddit_trending_pipeline get_config 2025-01-01"`

### Health Checks

All services include health checks. Check service status:

```bash
docker compose ps
```

## 📈 Monitoring

### Airflow UI
- **DAGs**: Overview of all DAGs and their status
- **Graph**: Visual representation of task dependencies
- **Tree**: Timeline view of task executions
- **Gantt**: Task duration analysis

![Airflow Dashboard](public/dashboard.png)

![DAG Execution View](public/dag_test.png)

### Flower (Celery Monitoring)
- Access at http://localhost:5555
- Monitor worker status and task queues
- View task execution statistics

### PgAdmin
- Access at http://localhost:5050
- Database administration and query interface
- Monitor database performance

## 🔒 Security

### Production Considerations

1. **Change Default Passwords**: Update Airflow and PgAdmin credentials
2. **Network Security**: Use reverse proxy and SSL certificates
3. **Database Security**: Use strong passwords and connection encryption
4. **Environment Variables**: Store sensitive data in secure vaults
5. **Resource Limits**: Set appropriate CPU and memory limits

### Environment Variables for Production

```bash
# Production settings
AIRFLOW__CORE__FERNET_KEY=your-fernet-key
_AIRFLOW_WWW_USER_PASSWORD=strong-password
PGADMIN_DEFAULT_PASSWORD=strong-password
```

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow 3.0 Migration Guide](https://airflow.apache.org/docs/apache-airflow/stable/migration-guide.html)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Reddit API Documentation](https://www.reddit.com/dev/api/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review Airflow logs
3. Check Docker container status
4. Open an issue in the repository

---

**Happy Data Pipelines! 🚀**
