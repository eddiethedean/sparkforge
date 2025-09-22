"""
Security monitoring for SparkForge.

This module provides runtime security monitoring capabilities including:
- Real-time security event monitoring
- Anomaly detection
- Security alerting
- Threat intelligence integration
"""

import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import psutil


class SecurityEventType(Enum):
    """Types of security events."""

    AUTHENTICATION_FAILURE = "authentication_failure"
    AUTHORIZATION_FAILURE = "authorization_failure"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    DATA_BREACH = "data_breach"
    MALWARE_DETECTION = "malware_detection"
    INTRUSION_ATTEMPT = "intrusion_attempt"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    DATA_EXFILTRATION = "data_exfiltration"
    SYSTEM_COMPROMISE = "system_compromise"
    CONFIGURATION_CHANGE = "configuration_change"


class SecuritySeverity(Enum):
    """Security event severity levels."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class SecurityEvent:
    """Security event data structure."""

    event_id: str
    event_type: SecurityEventType
    severity: SecuritySeverity
    timestamp: datetime
    source: str
    description: str
    details: Dict[str, Any]
    user_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class SecurityAlert:
    """Security alert data structure."""

    alert_id: str
    event_id: str
    severity: SecuritySeverity
    timestamp: datetime
    title: str
    description: str
    recommendation: str
    acknowledged: bool = False
    resolved: bool = False
    escalated: bool = False


@dataclass
class SecurityMetrics:
    """Security metrics data structure."""

    timestamp: datetime
    total_events: int
    events_by_type: Dict[str, int]
    events_by_severity: Dict[str, int]
    active_alerts: int
    resolved_alerts: int
    false_positives: int
    mean_time_to_resolution: float
    security_score: float


class SecurityMonitor:
    """Real-time security monitoring system."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._default_config()
        self.events: List[SecurityEvent] = []
        self.alerts: List[SecurityAlert] = []
        self.metrics: List[SecurityMetrics] = []
        self.monitoring_active = False
        self.monitor_thread = None
        self.alert_callbacks: List[Callable[[SecurityAlert], None]] = []
        self.event_callbacks: List[Callable[[SecurityEvent], None]] = []

        # Setup logging
        self.logger = logging.getLogger("security_monitor")
        self.logger.setLevel(logging.INFO)

        # Security thresholds
        self.thresholds = {
            "max_failed_auth_attempts": 5,
            "max_suspicious_requests": 10,
            "max_data_access_violations": 3,
            "max_privilege_escalation_attempts": 2,
        }

        # Rate limiting
        self.rate_limits = {"events_per_minute": 100, "alerts_per_hour": 50}

        # Event counters for rate limiting
        self.event_counters = {
            "minute": 0,
            "hour": 0,
            "last_minute_reset": datetime.now(),
            "last_hour_reset": datetime.now(),
        }

    def _default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "monitoring_interval": 60,  # seconds
            "retention_days": 30,
            "alert_threshold": 10,
            "enable_real_time_monitoring": True,
            "enable_anomaly_detection": True,
            "enable_threat_intelligence": True,
            "log_file": "security_monitor.log",
            "metrics_file": "security_metrics.json",
        }

    def start_monitoring(self) -> None:
        """Start security monitoring."""
        if self.monitoring_active:
            self.logger.warning("Security monitoring is already active")
            return

        self.monitoring_active = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop, daemon=True
        )
        self.monitor_thread.start()

        self.logger.info("Security monitoring started")

        # Log startup event
        self._log_event(
            SecurityEvent(
                event_id=f"startup_{int(time.time())}",
                event_type=SecurityEventType.CONFIGURATION_CHANGE,
                severity=SecuritySeverity.INFO,
                timestamp=datetime.now(),
                source="security_monitor",
                description="Security monitoring started",
                details={"config": self.config},
            )
        )

    def stop_monitoring(self) -> None:
        """Stop security monitoring."""
        if not self.monitoring_active:
            self.logger.warning("Security monitoring is not active")
            return

        self.monitoring_active = False

        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)

        self.logger.info("Security monitoring stopped")

        # Log shutdown event
        self._log_event(
            SecurityEvent(
                event_id=f"shutdown_{int(time.time())}",
                event_type=SecurityEventType.CONFIGURATION_CHANGE,
                severity=SecuritySeverity.INFO,
                timestamp=datetime.now(),
                source="security_monitor",
                description="Security monitoring stopped",
                details={},
            )
        )

    def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self.monitoring_active:
            try:
                # Monitor system resources
                self._monitor_system_resources()

                # Monitor network activity
                self._monitor_network_activity()

                # Monitor file system changes
                self._monitor_file_system()

                # Monitor process activity
                self._monitor_process_activity()

                # Check for anomalies
                if self.config.get("enable_anomaly_detection", True):
                    self._detect_anomalies()

                # Update metrics
                self._update_metrics()

                # Cleanup old data
                self._cleanup_old_data()

                time.sleep(self.config.get("monitoring_interval", 60))

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(5)  # Brief pause before retrying

    def _monitor_system_resources(self) -> None:
        """Monitor system resource usage."""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > 90:
                self._log_event(
                    SecurityEvent(
                        event_id=f"high_cpu_{int(time.time())}",
                        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                        severity=SecuritySeverity.MEDIUM,
                        timestamp=datetime.now(),
                        source="system_monitor",
                        description=f"High CPU usage detected: {cpu_percent}%",
                        details={"cpu_percent": cpu_percent},
                    )
                )

            # Memory usage
            memory = psutil.virtual_memory()
            if memory.percent > 90:
                self._log_event(
                    SecurityEvent(
                        event_id=f"high_memory_{int(time.time())}",
                        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                        severity=SecuritySeverity.MEDIUM,
                        timestamp=datetime.now(),
                        source="system_monitor",
                        description=f"High memory usage detected: {memory.percent}%",
                        details={"memory_percent": memory.percent},
                    )
                )

            # Disk usage
            disk = psutil.disk_usage("/")
            if disk.percent > 90:
                self._log_event(
                    SecurityEvent(
                        event_id=f"high_disk_{int(time.time())}",
                        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                        severity=SecuritySeverity.LOW,
                        timestamp=datetime.now(),
                        source="system_monitor",
                        description=f"High disk usage detected: {disk.percent}%",
                        details={"disk_percent": disk.percent},
                    )
                )

        except Exception as e:
            self.logger.error(f"Error monitoring system resources: {e}")

    def _monitor_network_activity(self) -> None:
        """Monitor network activity."""
        try:
            # Network connections
            connections = psutil.net_connections(kind="inet")

            # Check for suspicious connections
            suspicious_connections = []
            for conn in connections:
                if conn.raddr and conn.raddr.port in [22, 3389, 5900]:  # SSH, RDP, VNC
                    suspicious_connections.append(
                        {
                            "local_address": f"{conn.laddr.ip}:{conn.laddr.port}",
                            "remote_address": f"{conn.raddr.ip}:{conn.raddr.port}",
                            "status": conn.status,
                        }
                    )

            if suspicious_connections:
                self._log_event(
                    SecurityEvent(
                        event_id=f"suspicious_connections_{int(time.time())}",
                        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                        severity=SecuritySeverity.MEDIUM,
                        timestamp=datetime.now(),
                        source="network_monitor",
                        description=f"Suspicious network connections detected: {len(suspicious_connections)}",
                        details={"connections": suspicious_connections},
                    )
                )

        except Exception as e:
            self.logger.error(f"Error monitoring network activity: {e}")

    def _monitor_file_system(self) -> None:
        """Monitor file system changes."""
        try:
            # Check for critical file modifications
            critical_files = [
                "/etc/passwd",
                "/etc/shadow",
                "/etc/hosts",
                "/etc/crontab",
            ]

            for file_path in critical_files:
                if os.path.exists(file_path):
                    stat = os.stat(file_path)
                    modified_time = datetime.fromtimestamp(stat.st_mtime)

                    # Check if file was modified recently (within last hour)
                    if datetime.now() - modified_time < timedelta(hours=1):
                        self._log_event(
                            SecurityEvent(
                                event_id=f"critical_file_modified_{int(time.time())}",
                                event_type=SecurityEventType.SYSTEM_COMPROMISE,
                                severity=SecuritySeverity.HIGH,
                                timestamp=datetime.now(),
                                source="file_monitor",
                                description=f"Critical file modified: {file_path}",
                                details={
                                    "file_path": file_path,
                                    "modified_time": modified_time.isoformat(),
                                },
                            )
                        )

        except Exception as e:
            self.logger.error(f"Error monitoring file system: {e}")

    def _monitor_process_activity(self) -> None:
        """Monitor process activity."""
        try:
            # Check for suspicious processes
            processes = psutil.process_iter(
                ["pid", "name", "cpu_percent", "memory_percent"]
            )

            suspicious_processes = []
            for proc in processes:
                try:
                    if (
                        proc.info["cpu_percent"] > 50
                        or proc.info["memory_percent"] > 20
                    ):
                        # Check for known malicious process names
                        malicious_names = [
                            "nc",
                            "netcat",
                            "ncat",
                            "socat",
                            "reverse_shell",
                        ]
                        if any(
                            name in proc.info["name"].lower()
                            for name in malicious_names
                        ):
                            suspicious_processes.append(
                                {
                                    "pid": proc.info["pid"],
                                    "name": proc.info["name"],
                                    "cpu_percent": proc.info["cpu_percent"],
                                    "memory_percent": proc.info["memory_percent"],
                                }
                            )
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            if suspicious_processes:
                self._log_event(
                    SecurityEvent(
                        event_id=f"suspicious_processes_{int(time.time())}",
                        event_type=SecurityEventType.MALWARE_DETECTION,
                        severity=SecuritySeverity.HIGH,
                        timestamp=datetime.now(),
                        source="process_monitor",
                        description=f"Suspicious processes detected: {len(suspicious_processes)}",
                        details={"processes": suspicious_processes},
                    )
                )

        except Exception as e:
            self.logger.error(f"Error monitoring process activity: {e}")

    def _detect_anomalies(self) -> None:
        """Detect security anomalies."""
        try:
            # Check for rate limiting violations
            if self._check_rate_limits():
                self._log_event(
                    SecurityEvent(
                        event_id=f"rate_limit_exceeded_{int(time.time())}",
                        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                        severity=SecuritySeverity.MEDIUM,
                        timestamp=datetime.now(),
                        source="anomaly_detector",
                        description="Rate limit exceeded",
                        details={"rate_limits": self.rate_limits},
                    )
                )

            # Check for failed authentication attempts
            recent_events = self._get_recent_events(minutes=5)
            auth_failures = [
                e
                for e in recent_events
                if e.event_type == SecurityEventType.AUTHENTICATION_FAILURE
            ]

            if len(auth_failures) > self.thresholds["max_failed_auth_attempts"]:
                self._create_alert(
                    event_id=f"auth_brute_force_{int(time.time())}",
                    severity=SecuritySeverity.HIGH,
                    title="Brute Force Attack Detected",
                    description=f"Multiple failed authentication attempts: {len(auth_failures)}",
                    recommendation="Implement account lockout and rate limiting",
                )

            # Check for privilege escalation attempts
            privilege_events = [
                e
                for e in recent_events
                if e.event_type == SecurityEventType.PRIVILEGE_ESCALATION
            ]

            if (
                len(privilege_events)
                > self.thresholds["max_privilege_escalation_attempts"]
            ):
                self._create_alert(
                    event_id=f"privilege_escalation_{int(time.time())}",
                    severity=SecuritySeverity.CRITICAL,
                    title="Privilege Escalation Attempts Detected",
                    description=f"Multiple privilege escalation attempts: {len(privilege_events)}",
                    recommendation="Review user permissions and implement least privilege principle",
                )

        except Exception as e:
            self.logger.error(f"Error detecting anomalies: {e}")

    def _check_rate_limits(self) -> bool:
        """Check if rate limits are exceeded."""
        now = datetime.now()

        # Reset counters if needed
        if now - self.event_counters["last_minute_reset"] > timedelta(minutes=1):
            self.event_counters["minute"] = 0
            self.event_counters["last_minute_reset"] = now

        if now - self.event_counters["last_hour_reset"] > timedelta(hours=1):
            self.event_counters["hour"] = 0
            self.event_counters["last_hour_reset"] = now

        # Check rate limits
        if (
            self.event_counters["minute"] > self.rate_limits["events_per_minute"]
            or self.event_counters["hour"] > self.rate_limits["alerts_per_hour"]
        ):
            return True

        return False

    def _get_recent_events(self, minutes: int = 5) -> List[SecurityEvent]:
        """Get recent events within specified time window."""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        return [e for e in self.events if e.timestamp > cutoff_time]

    def _log_event(self, event: SecurityEvent) -> None:
        """Log a security event."""
        try:
            # Check rate limits
            if not self._check_rate_limits():
                self.events.append(event)
                self.event_counters["minute"] += 1
                self.event_counters["hour"] += 1

            # Log to file
            self.logger.info(
                f"Security Event: {event.event_type.value} - {event.description}"
            )

            # Call event callbacks
            for callback in self.event_callbacks:
                try:
                    callback(event)
                except Exception as e:
                    self.logger.error(f"Error in event callback: {e}")

            # Create alert if severity is high enough
            if event.severity in [SecuritySeverity.CRITICAL, SecuritySeverity.HIGH]:
                self._create_alert(
                    event_id=event.event_id,
                    severity=event.severity,
                    title=f"Security Event: {event.event_type.value}",
                    description=event.description,
                    recommendation=self._get_recommendation(event),
                )

        except Exception as e:
            self.logger.error(f"Error logging security event: {e}")

    def _create_alert(
        self,
        event_id: str,
        severity: SecuritySeverity,
        title: str,
        description: str,
        recommendation: str,
    ) -> None:
        """Create a security alert."""
        try:
            alert = SecurityAlert(
                alert_id=f"alert_{event_id}_{int(time.time())}",
                event_id=event_id,
                severity=severity,
                timestamp=datetime.now(),
                title=title,
                description=description,
                recommendation=recommendation,
            )

            self.alerts.append(alert)

            # Call alert callbacks
            for callback in self.alert_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    self.logger.error(f"Error in alert callback: {e}")

            # Log alert
            self.logger.warning(f"Security Alert: {alert.title} - {alert.description}")

        except Exception as e:
            self.logger.error(f"Error creating security alert: {e}")

    def _get_recommendation(self, event: SecurityEvent) -> str:
        """Get recommendation for security event."""
        recommendations = {
            SecurityEventType.AUTHENTICATION_FAILURE: "Review authentication logs and implement account lockout",
            SecurityEventType.AUTHORIZATION_FAILURE: "Review user permissions and access controls",
            SecurityEventType.SUSPICIOUS_ACTIVITY: "Investigate suspicious activity and review system logs",
            SecurityEventType.DATA_BREACH: "Immediately investigate potential data breach and notify stakeholders",
            SecurityEventType.MALWARE_DETECTION: "Quarantine affected systems and run malware scan",
            SecurityEventType.INTRUSION_ATTEMPT: "Block suspicious IP addresses and review firewall rules",
            SecurityEventType.PRIVILEGE_ESCALATION: "Review user permissions and implement least privilege principle",
            SecurityEventType.DATA_EXFILTRATION: "Investigate data access patterns and implement data loss prevention",
            SecurityEventType.SYSTEM_COMPROMISE: "Isolate compromised systems and perform forensic analysis",
            SecurityEventType.CONFIGURATION_CHANGE: "Review configuration changes and implement change management",
        }

        return recommendations.get(
            event.event_type, "Review security logs and investigate incident"
        )

    def _update_metrics(self) -> None:
        """Update security metrics."""
        try:
            now = datetime.now()

            # Calculate metrics
            total_events = len(self.events)
            events_by_type = {}
            events_by_severity = {}

            for event in self.events:
                event_type = event.event_type.value
                severity = event.severity.value

                events_by_type[event_type] = events_by_type.get(event_type, 0) + 1
                events_by_severity[severity] = events_by_severity.get(severity, 0) + 1

            active_alerts = len([a for a in self.alerts if not a.resolved])
            resolved_alerts = len([a for a in self.alerts if a.resolved])

            # Calculate mean time to resolution
            resolved_alert_times = []
            for alert in self.alerts:
                if alert.resolved:
                    # This would need to track resolution time in real implementation
                    resolved_alert_times.append(0)  # Placeholder

            mean_time_to_resolution = (
                sum(resolved_alert_times) / len(resolved_alert_times)
                if resolved_alert_times
                else 0
            )

            # Calculate security score (0-100, higher is better)
            security_score = self._calculate_security_score()

            metrics = SecurityMetrics(
                timestamp=now,
                total_events=total_events,
                events_by_type=events_by_type,
                events_by_severity=events_by_severity,
                active_alerts=active_alerts,
                resolved_alerts=resolved_alerts,
                false_positives=0,  # Would need to track in real implementation
                mean_time_to_resolution=mean_time_to_resolution,
                security_score=security_score,
            )

            self.metrics.append(metrics)

        except Exception as e:
            self.logger.error(f"Error updating metrics: {e}")

    def _calculate_security_score(self) -> float:
        """Calculate overall security score."""
        try:
            # Base score
            score = 100.0

            # Deduct points for active alerts
            active_alerts = len([a for a in self.alerts if not a.resolved])
            score -= active_alerts * 5  # 5 points per active alert

            # Deduct points for critical/high severity events in last 24 hours
            recent_events = self._get_recent_events(minutes=1440)  # 24 hours
            critical_events = len(
                [
                    e
                    for e in recent_events
                    if e.severity in [SecuritySeverity.CRITICAL, SecuritySeverity.HIGH]
                ]
            )
            score -= critical_events * 10  # 10 points per critical/high event

            # Ensure score doesn't go below 0
            return max(0.0, score)

        except Exception as e:
            self.logger.error(f"Error calculating security score: {e}")
            return 0.0

    def _cleanup_old_data(self) -> None:
        """Clean up old data based on retention policy."""
        try:
            retention_days = self.config.get("retention_days", 30)
            cutoff_time = datetime.now() - timedelta(days=retention_days)

            # Clean up old events
            self.events = [e for e in self.events if e.timestamp > cutoff_time]

            # Clean up old alerts (keep resolved alerts for longer)
            resolved_cutoff = datetime.now() - timedelta(days=retention_days * 2)
            self.alerts = [
                a
                for a in self.alerts
                if a.timestamp > cutoff_time
                or (a.resolved and a.timestamp > resolved_cutoff)
            ]

            # Clean up old metrics (keep more metrics)
            metrics_cutoff = datetime.now() - timedelta(days=retention_days // 2)
            self.metrics = [m for m in self.metrics if m.timestamp > metrics_cutoff]

        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")

    def add_event_callback(self, callback: Callable[[SecurityEvent], None]) -> None:
        """Add event callback."""
        self.event_callbacks.append(callback)

    def add_alert_callback(self, callback: Callable[[SecurityAlert], None]) -> None:
        """Add alert callback."""
        self.alert_callbacks.append(callback)

    def get_security_dashboard_data(self) -> Dict[str, Any]:
        """Get data for security dashboard."""
        try:
            recent_events = self._get_recent_events(minutes=60)  # Last hour
            [
                a
                for a in self.alerts
                if a.timestamp > datetime.now() - timedelta(hours=24)
            ]

            # Get latest metrics
            latest_metrics = self.metrics[-1] if self.metrics else None

            return {
                "current_security_score": (
                    latest_metrics.security_score if latest_metrics else 0
                ),
                "active_alerts": len([a for a in self.alerts if not a.resolved]),
                "recent_events": len(recent_events),
                "events_by_severity": (
                    latest_metrics.events_by_severity if latest_metrics else {}
                ),
                "top_threats": self._get_top_threats(),
                "security_trends": self._get_security_trends(),
                "monitoring_status": {
                    "active": self.monitoring_active,
                    "uptime": self._get_uptime(),
                    "last_scan": datetime.now().isoformat(),
                },
            }

        except Exception as e:
            self.logger.error(f"Error getting dashboard data: {e}")
            return {}

    def _get_top_threats(self) -> List[Dict[str, Any]]:
        """Get top security threats."""
        try:
            # Analyze recent events to identify top threats
            recent_events = self._get_recent_events(minutes=1440)  # Last 24 hours

            threat_counts = {}
            for event in recent_events:
                threat_type = event.event_type.value
                threat_counts[threat_type] = threat_counts.get(threat_type, 0) + 1

            # Sort by count and return top 5
            sorted_threats = sorted(
                threat_counts.items(), key=lambda x: x[1], reverse=True
            )

            return [
                {"threat_type": threat, "count": count}
                for threat, count in sorted_threats[:5]
            ]

        except Exception as e:
            self.logger.error(f"Error getting top threats: {e}")
            return []

    def _get_security_trends(self) -> List[Dict[str, Any]]:
        """Get security trends over time."""
        try:
            # Get metrics from last 7 days
            week_ago = datetime.now() - timedelta(days=7)
            recent_metrics = [m for m in self.metrics if m.timestamp > week_ago]

            trends = []
            for metrics in recent_metrics:
                trends.append(
                    {
                        "timestamp": metrics.timestamp.isoformat(),
                        "security_score": metrics.security_score,
                        "total_events": metrics.total_events,
                        "active_alerts": metrics.active_alerts,
                    }
                )

            return trends

        except Exception as e:
            self.logger.error(f"Error getting security trends: {e}")
            return []

    def _get_uptime(self) -> str:
        """Get monitoring uptime."""
        if not self.monitoring_active:
            return "Stopped"

        # This would track actual start time in real implementation
        return "Running"

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge a security alert."""
        try:
            for alert in self.alerts:
                if alert.alert_id == alert_id:
                    alert.acknowledged = True
                    self.logger.info(f"Alert acknowledged: {alert_id}")
                    return True
            return False
        except Exception as e:
            self.logger.error(f"Error acknowledging alert: {e}")
            return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve a security alert."""
        try:
            for alert in self.alerts:
                if alert.alert_id == alert_id:
                    alert.resolved = True
                    alert.acknowledged = True
                    self.logger.info(f"Alert resolved: {alert_id}")
                    return True
            return False
        except Exception as e:
            self.logger.error(f"Error resolving alert: {e}")
            return False

    def escalate_alert(self, alert_id: str) -> bool:
        """Escalate a security alert."""
        try:
            for alert in self.alerts:
                if alert.alert_id == alert_id:
                    alert.escalated = True
                    self.logger.warning(f"Alert escalated: {alert_id}")
                    return True
            return False
        except Exception as e:
            self.logger.error(f"Error escalating alert: {e}")
            return False

    def export_security_report(self, output_file: Optional[Path] = None) -> Path:
        """Export comprehensive security report."""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = Path(f"security_report_{timestamp}.json")

        report_data = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "monitoring_uptime": self._get_uptime(),
                "total_events": len(self.events),
                "total_alerts": len(self.alerts),
                "total_metrics": len(self.metrics),
            },
            "events": [
                asdict(event) for event in self.events[-1000:]
            ],  # Last 1000 events
            "alerts": [
                asdict(alert) for alert in self.alerts[-500:]
            ],  # Last 500 alerts
            "metrics": [
                asdict(metric) for metric in self.metrics[-100:]
            ],  # Last 100 metrics
            "dashboard_data": self.get_security_dashboard_data(),
            "configuration": self.config,
        }

        with open(output_file, "w") as f:
            json.dump(report_data, f, indent=2, default=str)

        return output_file


# Example usage and testing
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SparkForge Security Monitor")
    parser.add_argument("--config", type=Path, help="Configuration file")
    parser.add_argument("--output", type=Path, help="Output file for report")
    parser.add_argument(
        "--duration", type=int, default=300, help="Monitoring duration in seconds"
    )

    args = parser.parse_args()

    # Load configuration if provided
    config = {}
    if args.config and args.config.exists():
        with open(args.config) as f:
            config = json.load(f)

    # Create security monitor
    monitor = SecurityMonitor(config)

    # Add example callbacks
    def event_callback(event: SecurityEvent):
        print(f"Security Event: {event.event_type.value} - {event.description}")

    def alert_callback(alert: SecurityAlert):
        print(f"Security Alert: {alert.title} - {alert.description}")

    monitor.add_event_callback(event_callback)
    monitor.add_alert_callback(alert_callback)

    # Start monitoring
    monitor.start_monitoring()

    try:
        # Run for specified duration
        time.sleep(args.duration)
    except KeyboardInterrupt:
        print("Monitoring stopped by user")
    finally:
        # Stop monitoring
        monitor.stop_monitoring()

        # Generate report
        report_file = monitor.export_security_report(args.output)
        print(f"Security report saved to: {report_file}")

        # Print summary
        dashboard_data = monitor.get_security_dashboard_data()
        print("\nSecurity Summary:")
        print(
            f"Security Score: {dashboard_data.get('current_security_score', 0):.1f}/100"
        )
        print(f"Active Alerts: {dashboard_data.get('active_alerts', 0)}")
        print(f"Recent Events: {dashboard_data.get('recent_events', 0)}")
