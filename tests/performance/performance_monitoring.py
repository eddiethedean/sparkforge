"""
Performance monitoring for SparkForge.

This module provides comprehensive performance monitoring capabilities including:
- Real-time performance metrics collection
- Resource utilization monitoring
- Performance alerting and thresholds
- Performance trend analysis
- Performance reporting and visualization
"""

import json
import logging
import statistics
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import psutil


@dataclass
class PerformanceMetric:
    """Performance metric data structure."""

    name: str
    value: float
    unit: str
    timestamp: datetime
    tags: Dict[str, str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = {}


@dataclass
class ResourceUsage:
    """Resource usage data structure."""

    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    memory_available_mb: float
    disk_usage_percent: float
    network_bytes_sent: int
    network_bytes_received: int
    timestamp: datetime


@dataclass
class PerformanceAlert:
    """Performance alert data structure."""

    alert_id: str
    metric_name: str
    current_value: float
    threshold_value: float
    severity: str
    message: str
    timestamp: datetime
    acknowledged: bool = False


@dataclass
class PerformanceReport:
    """Performance report data structure."""

    report_period: timedelta
    start_time: datetime
    end_time: datetime
    metrics_summary: Dict[str, Dict[str, float]]
    resource_usage_summary: Dict[str, float]
    alerts: List[PerformanceAlert]
    recommendations: List[str]


class PerformanceMonitor:
    """Real-time performance monitoring system."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._default_config()
        self.metrics: Dict[str, deque] = {}
        self.resource_history: deque = deque(maxlen=1000)
        self.alerts: List[PerformanceAlert] = []
        self.alert_callbacks: List[Callable[[PerformanceAlert], None]] = []

        # Monitoring state
        self.monitoring_active = False
        self.monitor_thread = None
        self.last_network_stats = None

        # Performance thresholds
        self.thresholds = {
            "cpu_percent": 80.0,
            "memory_percent": 85.0,
            "memory_used_mb": 2048.0,
            "disk_usage_percent": 90.0,
            "response_time_ms": 5000.0,
            "throughput_rps": 10.0,
        }

        # Setup logging
        self.logger = logging.getLogger("performance_monitor")
        self.logger.setLevel(logging.INFO)

    def _default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "monitoring_interval": 10,  # seconds
            "metrics_retention": 1000,  # number of metrics to keep
            "resource_retention": 1000,  # number of resource snapshots to keep
            "alert_retention_days": 7,
            "enable_alerts": True,
            "enable_resource_monitoring": True,
            "enable_metrics_collection": True,
        }

    def start_monitoring(self) -> None:
        """Start performance monitoring."""
        if self.monitoring_active:
            self.logger.warning("Performance monitoring is already active")
            return

        self.monitoring_active = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop, daemon=True
        )
        self.monitor_thread.start()

        self.logger.info("Performance monitoring started")

    def stop_monitoring(self) -> None:
        """Stop performance monitoring."""
        if not self.monitoring_active:
            self.logger.warning("Performance monitoring is not active")
            return

        self.monitoring_active = False

        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)

        self.logger.info("Performance monitoring stopped")

    def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self.monitoring_active:
            try:
                # Collect resource usage
                if self.config.get("enable_resource_monitoring", True):
                    resource_usage = self._collect_resource_usage()
                    self.resource_history.append(resource_usage)

                    # Check for resource alerts
                    if self.config.get("enable_alerts", True):
                        self._check_resource_alerts(resource_usage)

                # Clean up old data
                self._cleanup_old_data()

                # Sleep until next monitoring cycle
                time.sleep(self.config.get("monitoring_interval", 10))

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(5)  # Brief pause before retrying

    def _collect_resource_usage(self) -> ResourceUsage:
        """Collect current resource usage."""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)

            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_used_mb = memory.used / 1024 / 1024
            memory_available_mb = memory.available / 1024 / 1024

            # Disk usage
            disk = psutil.disk_usage("/")
            disk_usage_percent = (disk.used / disk.total) * 100

            # Network usage
            network_stats = psutil.net_io_counters()
            if self.last_network_stats:
                network_bytes_sent = (
                    network_stats.bytes_sent - self.last_network_stats.bytes_sent
                )
                network_bytes_received = (
                    network_stats.bytes_recv - self.last_network_stats.bytes_recv
                )
            else:
                network_bytes_sent = 0
                network_bytes_received = 0

            self.last_network_stats = network_stats

            return ResourceUsage(
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                memory_used_mb=memory_used_mb,
                memory_available_mb=memory_available_mb,
                disk_usage_percent=disk_usage_percent,
                network_bytes_sent=network_bytes_sent,
                network_bytes_received=network_bytes_received,
                timestamp=datetime.now(),
            )

        except Exception as e:
            self.logger.error(f"Error collecting resource usage: {e}")
            return ResourceUsage(
                cpu_percent=0.0,
                memory_percent=0.0,
                memory_used_mb=0.0,
                memory_available_mb=0.0,
                disk_usage_percent=0.0,
                network_bytes_sent=0,
                network_bytes_received=0,
                timestamp=datetime.now(),
            )

    def _check_resource_alerts(self, resource_usage: ResourceUsage) -> None:
        """Check for resource-based alerts."""
        alerts_to_create = []

        # CPU alert
        if resource_usage.cpu_percent > self.thresholds["cpu_percent"]:
            alerts_to_create.append(
                PerformanceAlert(
                    alert_id=f"cpu_high_{int(time.time())}",
                    metric_name="cpu_percent",
                    current_value=resource_usage.cpu_percent,
                    threshold_value=self.thresholds["cpu_percent"],
                    severity="warning",
                    message=f"High CPU usage: {resource_usage.cpu_percent:.1f}%",
                    timestamp=datetime.now(),
                )
            )

        # Memory alert
        if resource_usage.memory_percent > self.thresholds["memory_percent"]:
            alerts_to_create.append(
                PerformanceAlert(
                    alert_id=f"memory_high_{int(time.time())}",
                    metric_name="memory_percent",
                    current_value=resource_usage.memory_percent,
                    threshold_value=self.thresholds["memory_percent"],
                    severity="warning",
                    message=f"High memory usage: {resource_usage.memory_percent:.1f}%",
                    timestamp=datetime.now(),
                )
            )

        # Disk alert
        if resource_usage.disk_usage_percent > self.thresholds["disk_usage_percent"]:
            alerts_to_create.append(
                PerformanceAlert(
                    alert_id=f"disk_high_{int(time.time())}",
                    metric_name="disk_usage_percent",
                    current_value=resource_usage.disk_usage_percent,
                    threshold_value=self.thresholds["disk_usage_percent"],
                    severity="critical",
                    message=f"High disk usage: {resource_usage.disk_usage_percent:.1f}%",
                    timestamp=datetime.now(),
                )
            )

        # Create alerts
        for alert in alerts_to_create:
            self._create_alert(alert)

    def _create_alert(self, alert: PerformanceAlert) -> None:
        """Create a performance alert."""
        # Check if similar alert already exists
        existing_alerts = [
            a
            for a in self.alerts
            if a.metric_name == alert.metric_name
            and not a.acknowledged
            and (datetime.now() - a.timestamp).total_seconds() < 300
        ]  # 5 minutes

        if not existing_alerts:
            self.alerts.append(alert)
            self.logger.warning(f"Performance Alert: {alert.message}")

            # Call alert callbacks
            for callback in self.alert_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    self.logger.error(f"Error in alert callback: {e}")

    def record_metric(
        self,
        name: str,
        value: float,
        unit: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a performance metric."""
        if not self.config.get("enable_metrics_collection", True):
            return

        # Initialize metric queue if not exists
        if name not in self.metrics:
            maxlen = self.config.get("metrics_retention", 1000)
            self.metrics[name] = deque(maxlen=maxlen)

        # Create metric
        metric = PerformanceMetric(
            name=name, value=value, unit=unit, timestamp=datetime.now(), tags=tags or {}
        )

        # Add to queue
        self.metrics[name].append(metric)

        # Check for metric-based alerts
        self._check_metric_alerts(metric)

    def _check_metric_alerts(self, metric: PerformanceMetric) -> None:
        """Check for metric-based alerts."""
        if not self.config.get("enable_alerts", True):
            return

        # Check response time
        if (
            metric.name == "response_time"
            and metric.value > self.thresholds["response_time_ms"]
        ):
            alert = PerformanceAlert(
                alert_id=f"response_time_{int(time.time())}",
                metric_name="response_time",
                current_value=metric.value,
                threshold_value=self.thresholds["response_time_ms"],
                severity="warning",
                message=f"Slow response time: {metric.value:.1f}ms",
                timestamp=datetime.now(),
            )
            self._create_alert(alert)

        # Check throughput
        if (
            metric.name == "throughput"
            and metric.value < self.thresholds["throughput_rps"]
        ):
            alert = PerformanceAlert(
                alert_id=f"throughput_{int(time.time())}",
                metric_name="throughput",
                current_value=metric.value,
                threshold_value=self.thresholds["throughput_rps"],
                severity="warning",
                message=f"Low throughput: {metric.value:.1f} requests/second",
                timestamp=datetime.now(),
            )
            self._create_alert(alert)

    def get_metric_stats(self, name: str, window_minutes: int = 60) -> Dict[str, float]:
        """Get statistics for a specific metric."""
        if name not in self.metrics:
            return {}

        # Filter metrics by time window
        cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
        recent_metrics = [m for m in self.metrics[name] if m.timestamp > cutoff_time]

        if not recent_metrics:
            return {}

        values = [m.value for m in recent_metrics]

        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
            "p95": self._percentile(values, 95),
            "p99": self._percentile(values, 99),
        }

    def _percentile(self, values: List[float], percentile: int) -> float:
        """Calculate percentile of values."""
        if not values:
            return 0.0

        sorted_values = sorted(values)
        index = (percentile / 100.0) * (len(sorted_values) - 1)

        if index.is_integer():
            return sorted_values[int(index)]
        else:
            lower = sorted_values[int(index)]
            upper = sorted_values[int(index) + 1]
            return lower + (upper - lower) * (index - int(index))

    def get_resource_summary(self, window_minutes: int = 60) -> Dict[str, float]:
        """Get resource usage summary."""
        if not self.resource_history:
            return {}

        # Filter by time window
        cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
        recent_resources = [
            r for r in self.resource_history if r.timestamp > cutoff_time
        ]

        if not recent_resources:
            return {}

        return {
            "avg_cpu_percent": statistics.mean(
                [r.cpu_percent for r in recent_resources]
            ),
            "max_cpu_percent": max([r.cpu_percent for r in recent_resources]),
            "avg_memory_percent": statistics.mean(
                [r.memory_percent for r in recent_resources]
            ),
            "max_memory_percent": max([r.memory_percent for r in recent_resources]),
            "avg_memory_used_mb": statistics.mean(
                [r.memory_used_mb for r in recent_resources]
            ),
            "max_memory_used_mb": max([r.memory_used_mb for r in recent_resources]),
            "avg_disk_usage_percent": statistics.mean(
                [r.disk_usage_percent for r in recent_resources]
            ),
            "max_disk_usage_percent": max(
                [r.disk_usage_percent for r in recent_resources]
            ),
        }

    def get_active_alerts(self) -> List[PerformanceAlert]:
        """Get active (unacknowledged) alerts."""
        return [alert for alert in self.alerts if not alert.acknowledged]

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                alert.acknowledged = True
                self.logger.info(f"Alert acknowledged: {alert_id}")
                return True
        return False

    def add_alert_callback(self, callback: Callable[[PerformanceAlert], None]) -> None:
        """Add alert callback."""
        self.alert_callbacks.append(callback)

    def _cleanup_old_data(self) -> None:
        """Clean up old data based on retention policies."""
        try:
            # Clean up old alerts
            retention_days = self.config.get("alert_retention_days", 7)
            cutoff_time = datetime.now() - timedelta(days=retention_days)

            self.alerts = [
                alert for alert in self.alerts if alert.timestamp > cutoff_time
            ]

        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")

    def generate_performance_report(
        self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> PerformanceReport:
        """Generate comprehensive performance report."""
        if not start_time:
            start_time = datetime.now() - timedelta(hours=1)
        if not end_time:
            end_time = datetime.now()

        report_period = end_time - start_time

        # Calculate metrics summary
        metrics_summary = {}
        for metric_name in self.metrics:
            stats = self.get_metric_stats(
                metric_name, window_minutes=int(report_period.total_seconds() / 60)
            )
            if stats:
                metrics_summary[metric_name] = stats

        # Calculate resource usage summary
        resource_summary = self.get_resource_summary(
            window_minutes=int(report_period.total_seconds() / 60)
        )

        # Get alerts in time period
        period_alerts = [
            alert for alert in self.alerts if start_time <= alert.timestamp <= end_time
        ]

        # Generate recommendations
        recommendations = self._generate_recommendations(
            metrics_summary, resource_summary, period_alerts
        )

        return PerformanceReport(
            report_period=report_period,
            start_time=start_time,
            end_time=end_time,
            metrics_summary=metrics_summary,
            resource_usage_summary=resource_summary,
            alerts=period_alerts,
            recommendations=recommendations,
        )

    def _generate_recommendations(
        self,
        metrics_summary: Dict[str, Dict[str, float]],
        resource_summary: Dict[str, float],
        alerts: List[PerformanceAlert],
    ) -> List[str]:
        """Generate performance recommendations."""
        recommendations = []

        # Resource-based recommendations
        if resource_summary:
            if resource_summary.get("avg_cpu_percent", 0) > 70:
                recommendations.append(
                    "Consider optimizing CPU-intensive operations or scaling horizontally"
                )

            if resource_summary.get("avg_memory_percent", 0) > 80:
                recommendations.append(
                    "Consider optimizing memory usage or increasing available memory"
                )

            if resource_summary.get("max_memory_used_mb", 0) > 2048:
                recommendations.append(
                    "Memory usage is high - review data structures and caching strategies"
                )

        # Metrics-based recommendations
        if "response_time" in metrics_summary:
            avg_response = metrics_summary["response_time"].get("mean", 0)
            if avg_response > 1000:
                recommendations.append(
                    "Response times are high - consider optimizing slow operations"
                )

        if "throughput" in metrics_summary:
            avg_throughput = metrics_summary["throughput"].get("mean", 0)
            if avg_throughput < 50:
                recommendations.append(
                    "Throughput is low - consider parallelization or caching"
                )

        # Alert-based recommendations
        critical_alerts = [a for a in alerts if a.severity == "critical"]
        if critical_alerts:
            recommendations.append(
                f"Address {len(critical_alerts)} critical performance alerts immediately"
            )

        warning_alerts = [a for a in alerts if a.severity == "warning"]
        if warning_alerts:
            recommendations.append(
                f"Review {len(warning_alerts)} warning-level performance alerts"
            )

        # General recommendations
        recommendations.extend(
            [
                "Regular performance monitoring helps identify issues early",
                "Consider implementing performance baselines and SLAs",
                "Monitor performance trends over time for capacity planning",
                "Implement automated alerting for critical performance metrics",
            ]
        )

        return recommendations

    def export_report(
        self, report: PerformanceReport, output_file: Optional[Path] = None
    ) -> Path:
        """Export performance report to file."""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = Path(f"performance_report_{timestamp}.json")

        # Convert to serializable format
        report_data = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "report_period_seconds": report.report_period.total_seconds(),
                "start_time": report.start_time.isoformat(),
                "end_time": report.end_time.isoformat(),
            },
            "metrics_summary": report.metrics_summary,
            "resource_usage_summary": report.resource_usage_summary,
            "alerts": [asdict(alert) for alert in report.alerts],
            "recommendations": report.recommendations,
        }

        with open(output_file, "w") as f:
            json.dump(report_data, f, indent=2, default=str)

        return output_file

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data for performance dashboard."""
        try:
            # Get recent metrics (last hour)
            metrics_data = {}
            for metric_name in self.metrics:
                stats = self.get_metric_stats(metric_name, window_minutes=60)
                if stats:
                    metrics_data[metric_name] = {
                        "current": (
                            self.metrics[metric_name][-1].value
                            if self.metrics[metric_name]
                            else 0
                        ),
                        "stats": stats,
                    }

            # Get resource summary
            resource_summary = self.get_resource_summary(window_minutes=60)

            # Get active alerts
            active_alerts = self.get_active_alerts()

            # Get current resource usage
            current_resources = (
                self.resource_history[-1] if self.resource_history else None
            )

            return {
                "current_time": datetime.now().isoformat(),
                "monitoring_active": self.monitoring_active,
                "metrics": metrics_data,
                "resources": {
                    "current": asdict(current_resources) if current_resources else {},
                    "summary": resource_summary,
                },
                "alerts": {
                    "active_count": len(active_alerts),
                    "recent": [asdict(alert) for alert in active_alerts[-10:]],
                },
                "thresholds": self.thresholds,
            }

        except Exception as e:
            self.logger.error(f"Error getting dashboard data: {e}")
            return {}


# Global performance monitor instance
_performance_monitor = PerformanceMonitor()


def get_performance_monitor() -> PerformanceMonitor:
    """Get global performance monitor instance."""
    return _performance_monitor


def record_metric(
    name: str, value: float, unit: str = "", tags: Optional[Dict[str, str]] = None
):
    """Record a performance metric."""
    _performance_monitor.record_metric(name, value, unit, tags)


def start_performance_monitoring():
    """Start global performance monitoring."""
    _performance_monitor.start_monitoring()


def stop_performance_monitoring():
    """Stop global performance monitoring."""
    _performance_monitor.stop_monitoring()


# CLI interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SparkForge Performance Monitor")
    parser.add_argument(
        "--start", action="store_true", help="Start performance monitoring"
    )
    parser.add_argument(
        "--stop", action="store_true", help="Stop performance monitoring"
    )
    parser.add_argument(
        "--report", action="store_true", help="Generate performance report"
    )
    parser.add_argument("--dashboard", action="store_true", help="Show dashboard data")
    parser.add_argument(
        "--duration", type=int, default=60, help="Monitoring duration in seconds"
    )

    args = parser.parse_args()

    monitor = PerformanceMonitor()

    if args.start:
        monitor.start_monitoring()
        print("Performance monitoring started")

        try:
            time.sleep(args.duration)
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
        finally:
            monitor.stop_monitoring()

    if args.stop:
        monitor.stop_monitoring()
        print("Performance monitoring stopped")

    if args.report:
        report = monitor.generate_performance_report()
        report_file = monitor.export_report(report)
        print(f"Performance report saved to: {report_file}")

        print("\nPerformance Summary:")
        print(f"Report Period: {report.report_period}")
        print(f"Metrics Tracked: {len(report.metrics_summary)}")
        print(f"Active Alerts: {len(report.alerts)}")
        print(f"Recommendations: {len(report.recommendations)}")

    if args.dashboard:
        dashboard_data = monitor.get_dashboard_data()
        print(json.dumps(dashboard_data, indent=2, default=str))
