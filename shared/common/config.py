"""
Centralized configuration loaded from environment variables.
Every microservice imports this to get consistent settings.
"""

import os


class Settings:
    # ── General ──────────────────────────────────────────────
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", "swifttrack")
    PORT: int = int(os.getenv("PORT", "8000"))
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"

    # ── Database (PostgreSQL via asyncpg) ────────────────────
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://swifttrack:swifttrack@localhost:5432/swifttrack",
    )

    # ── RabbitMQ ─────────────────────────────────────────────
    RABBITMQ_URL: str = os.getenv(
        "RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"
    )

    # ── JWT / Security ───────────────────────────────────────
    JWT_SECRET: str = os.getenv("JWT_SECRET", "change_me_in_production")
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_MINUTES: int = int(os.getenv("JWT_EXPIRATION_MINUTES", "1440"))

    # ── Inter-service URLs (used mainly by api-gateway) ──────
    AUTH_SERVICE_URL: str = os.getenv(
        "AUTH_SERVICE_URL", "http://localhost:8002"
    )
    ORDER_SERVICE_URL: str = os.getenv(
        "ORDER_SERVICE_URL", "http://localhost:8001"
    )
    TRACKING_SERVICE_URL: str = os.getenv(
        "TRACKING_SERVICE_URL", "http://localhost:8003"
    )

    # ── Mock external-system URLs ────────────────────────────
    CMS_SERVICE_URL: str = os.getenv(
        "CMS_SERVICE_URL", "http://mock-cms:8004"
    )
    ROS_SERVICE_URL: str = os.getenv(
        "ROS_SERVICE_URL", "http://mock-ros:8005"
    )
    WMS_SERVICE_HOST: str = os.getenv("WMS_SERVICE_HOST", "mock-wms")
    WMS_SERVICE_PORT: int = int(os.getenv("WMS_SERVICE_PORT", "9000"))

    # ── RabbitMQ Exchange / Queue names ──────────────────────
    ORDER_EXCHANGE: str = "swifttrack.orders"
    TRACKING_EXCHANGE: str = "swifttrack.tracking"
    NOTIFICATION_EXCHANGE: str = "swifttrack.notifications"

    # Routing keys
    ORDER_CREATED_KEY: str = "order.created"
    ORDER_CONFIRMED_KEY: str = "order.confirmed"
    ORDER_PROCESSING_KEY: str = "order.processing"
    ORDER_FAILED_KEY: str = "order.failed"
    TRACKING_UPDATE_KEY: str = "tracking.update"
    WMS_PACKAGE_KEY: str = "wms.package"
    ROS_ROUTE_KEY: str = "ros.route"
    CMS_BILLING_KEY: str = "cms.billing"


settings = Settings()
