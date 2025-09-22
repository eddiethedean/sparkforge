"""
Integration tests for SparkForge security components.

This module tests the integration of all security components including:
- Security test suite
- Vulnerability scanner
- Compliance checker
- Security monitoring
"""

import json
import tempfile
import time
from datetime import datetime
from pathlib import Path

import pytest

from tests.security.compliance_checker import ComplianceChecker, ComplianceStandard
from tests.security.security_monitoring import (
    SecurityEvent,
    SecurityEventType,
    SecurityMonitor,
    SecuritySeverity,
)

# Import security components
from tests.security.security_tests import SecurityTestSuite
from tests.security.vulnerability_scanner import VulnerabilityScanner


class TestSecurityIntegration:
    """Integration tests for security components."""

    @pytest.fixture
    def temp_project_dir(self):
        """Create temporary project directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def security_test_suite(self):
        """Create security test suite instance."""
        return SecurityTestSuite()

    @pytest.fixture
    def vulnerability_scanner(self, temp_project_dir):
        """Create vulnerability scanner instance."""
        return VulnerabilityScanner(temp_project_dir)

    @pytest.fixture
    def compliance_checker(self, temp_project_dir):
        """Create compliance checker instance."""
        return ComplianceChecker(temp_project_dir)

    @pytest.fixture
    def security_monitor(self):
        """Create security monitor instance."""
        config = {
            "monitoring_interval": 1,
            "retention_days": 1,
            "enable_real_time_monitoring": True,
            "enable_anomaly_detection": True,
        }
        return SecurityMonitor(config)

    def test_security_test_suite_integration(self, security_test_suite):
        """Test security test suite integration."""
        # Run comprehensive security scan
        results = security_test_suite.run_security_scan()

        # Verify all scan components are present
        assert "vulnerability_scan" in results
        assert "dependency_check" in results
        assert "code_security" in results
        assert "configuration_security" in results
        assert "data_security" in results
        assert "compliance_check" in results

        # Verify scan results structure
        for _component, result in results.items():
            assert isinstance(result, dict)
            assert "success" in result or "compliant" in result

    def test_vulnerability_scanner_integration(
        self, vulnerability_scanner, temp_project_dir
    ):
        """Test vulnerability scanner integration."""
        # Create test files
        test_file = temp_project_dir / "test_security.py"
        test_file.write_text(
            """
import os
password = "hardcoded_password"  # This should trigger a security issue
"""
        )

        # Run vulnerability scan
        scan_results = vulnerability_scanner.scan_all()

        # Verify scan results
        assert "scan_results" in scan_results
        assert "security_metrics" in scan_results
        assert "summary" in scan_results

        # Verify security metrics
        metrics = scan_results["security_metrics"]
        assert metrics is not None
        assert hasattr(metrics, "total_vulnerabilities")
        assert hasattr(metrics, "scan_timestamp")

    def test_compliance_checker_integration(self, compliance_checker):
        """Test compliance checker integration."""
        # Check all compliance standards
        compliance_reports = compliance_checker.check_all_standards()

        # Verify all standards are checked
        expected_standards = [
            ComplianceStandard.OWASP_TOP_10,
            ComplianceStandard.CVE_COMPLIANCE,
            ComplianceStandard.LICENSE_COMPLIANCE,
            ComplianceStandard.SECURITY_BEST_PRACTICES,
        ]

        for standard in expected_standards:
            assert standard.value in compliance_reports

            report = compliance_reports[standard.value]
            assert hasattr(report, "overall_compliant")
            assert hasattr(report, "compliance_score")
            assert hasattr(report, "checks")
            assert hasattr(report, "recommendations")

    def test_security_monitor_integration(self, security_monitor):
        """Test security monitor integration."""
        # Start monitoring
        security_monitor.start_monitoring()

        # Wait for monitoring to start
        time.sleep(2)

        # Verify monitoring is active
        assert security_monitor.monitoring_active

        # Create test security event
        test_event = SecurityEvent(
            event_id="test_event_001",
            event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
            severity=SecuritySeverity.MEDIUM,
            timestamp=datetime.now(),
            source="test_integration",
            description="Test security event for integration testing",
            details={"test": True},
        )

        # Log the event
        security_monitor._log_event(test_event)

        # Verify event was logged
        assert len(security_monitor.events) > 0

        # Get dashboard data
        dashboard_data = security_monitor.get_security_dashboard_data()
        assert "current_security_score" in dashboard_data
        assert "active_alerts" in dashboard_data
        assert "recent_events" in dashboard_data

        # Stop monitoring
        security_monitor.stop_monitoring()
        assert not security_monitor.monitoring_active

    def test_security_components_workflow(
        self,
        security_test_suite,
        vulnerability_scanner,
        compliance_checker,
        security_monitor,
    ):
        """Test complete security workflow integration."""
        # Step 1: Run security tests
        security_results = security_test_suite.run_security_scan()

        # Step 2: Run vulnerability scan
        vulnerability_results = vulnerability_scanner.scan_all()

        # Step 3: Run compliance check
        compliance_results = compliance_checker.check_all_standards()

        # Step 4: Start security monitoring
        security_monitor.start_monitoring()

        # Step 5: Generate security event
        security_event = SecurityEvent(
            event_id="workflow_test_001",
            event_type=SecurityEventType.AUTHENTICATION_FAILURE,
            severity=SecuritySeverity.HIGH,
            timestamp=datetime.now(),
            source="workflow_test",
            description="Authentication failure detected",
            details={"user": "test_user", "attempts": 3},
        )

        security_monitor._log_event(security_event)

        # Step 6: Verify workflow results
        assert security_results["vulnerability_scan"]["success"]
        assert vulnerability_results["security_metrics"] is not None
        assert len(compliance_results) >= 4
        assert len(security_monitor.events) > 0

        # Step 7: Stop monitoring
        security_monitor.stop_monitoring()

    def test_security_reporting_integration(
        self,
        vulnerability_scanner,
        compliance_checker,
        security_monitor,
        temp_project_dir,
    ):
        """Test security reporting integration."""
        # Generate vulnerability report
        vuln_report_file = vulnerability_scanner.generate_report(
            temp_project_dir / "vulnerability_report.json"
        )
        assert vuln_report_file.exists()

        # Generate compliance report
        compliance_report_file = compliance_checker.generate_compliance_report(
            temp_project_dir / "compliance_report.json"
        )
        assert compliance_report_file.exists()

        # Start monitoring and generate security report
        security_monitor.start_monitoring()
        time.sleep(1)

        security_report_file = security_monitor.export_security_report(
            temp_project_dir / "security_report.json"
        )
        assert security_report_file.exists()

        # Verify report contents
        with open(vuln_report_file) as f:
            vuln_data = json.load(f)
            assert "scan_results" in vuln_data
            assert "security_metrics" in vuln_data

        with open(compliance_report_file) as f:
            compliance_data = json.load(f)
            assert len(compliance_data) >= 4

        with open(security_report_file) as f:
            security_data = json.load(f)
            assert "report_metadata" in security_data
            assert "events" in security_data
            assert "alerts" in security_data

        security_monitor.stop_monitoring()

    def test_security_alerting_integration(self, security_monitor):
        """Test security alerting integration."""
        alert_callback_called = False
        alert_received = None

        def alert_callback(alert):
            nonlocal alert_callback_called, alert_received
            alert_callback_called = True
            alert_received = alert

        # Add alert callback
        security_monitor.add_alert_callback(alert_callback)

        # Start monitoring
        security_monitor.start_monitoring()

        # Create high severity event that should trigger alert
        critical_event = SecurityEvent(
            event_id="critical_test_001",
            event_type=SecurityEventType.SYSTEM_COMPROMISE,
            severity=SecuritySeverity.CRITICAL,
            timestamp=datetime.now(),
            source="alert_test",
            description="Critical security event for alerting test",
            details={"critical": True},
        )

        security_monitor._log_event(critical_event)

        # Wait for alert processing
        time.sleep(2)

        # Verify alert was triggered
        assert alert_callback_called
        assert alert_received is not None
        assert alert_received.severity == SecuritySeverity.CRITICAL

        # Verify alert is in alerts list
        assert len(security_monitor.alerts) > 0

        # Test alert acknowledgment
        alert_id = security_monitor.alerts[0].alert_id
        assert security_monitor.acknowledge_alert(alert_id)
        assert security_monitor.alerts[0].acknowledged

        # Test alert resolution
        assert security_monitor.resolve_alert(alert_id)
        assert security_monitor.alerts[0].resolved

        security_monitor.stop_monitoring()

    def test_security_metrics_integration(self, security_monitor):
        """Test security metrics integration."""
        # Start monitoring
        security_monitor.start_monitoring()

        # Create various security events
        events = [
            SecurityEvent(
                event_id=f"metrics_test_{i}",
                event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
                severity=SecuritySeverity.MEDIUM,
                timestamp=datetime.now(),
                source="metrics_test",
                description=f"Test event {i} for metrics",
                details={"event_number": i},
            )
            for i in range(5)
        ]

        for event in events:
            security_monitor._log_event(event)

        # Wait for metrics update
        time.sleep(3)

        # Verify metrics were generated
        assert len(security_monitor.metrics) > 0

        latest_metrics = security_monitor.metrics[-1]
        assert latest_metrics.total_events >= 5
        assert latest_metrics.events_by_type is not None
        assert latest_metrics.events_by_severity is not None
        assert latest_metrics.security_score >= 0

        # Test dashboard data
        dashboard_data = security_monitor.get_security_dashboard_data()
        assert "current_security_score" in dashboard_data
        assert "active_alerts" in dashboard_data
        assert "recent_events" in dashboard_data
        assert "security_trends" in dashboard_data

        security_monitor.stop_monitoring()

    def test_security_thresholds_integration(self, security_monitor):
        """Test security thresholds integration."""
        # Start monitoring
        security_monitor.start_monitoring()

        # Create multiple authentication failure events to trigger threshold
        for i in range(6):  # More than max_failed_auth_attempts (5)
            auth_failure = SecurityEvent(
                event_id=f"auth_failure_{i}",
                event_type=SecurityEventType.AUTHENTICATION_FAILURE,
                severity=SecuritySeverity.MEDIUM,
                timestamp=datetime.now(),
                source="threshold_test",
                description=f"Authentication failure {i}",
                details={"attempt": i},
            )
            security_monitor._log_event(auth_failure)

        # Wait for anomaly detection
        time.sleep(3)

        # Verify alert was created for threshold violation
        auth_alerts = [
            alert for alert in security_monitor.alerts if "Brute Force" in alert.title
        ]
        assert len(auth_alerts) > 0

        security_monitor.stop_monitoring()

    def test_security_configuration_integration(self, temp_project_dir):
        """Test security configuration integration."""
        # Create security configuration file
        config_file = temp_project_dir / "security_config.yaml"
        config_content = """
security_scanning:
  bandit:
    enabled: true
    severity_threshold: "medium"

security_monitoring:
  enabled: true
  monitoring_interval: 60
"""
        config_file.write_text(config_content)

        # Test that configuration can be loaded
        import yaml

        with open(config_file) as f:
            config = yaml.safe_load(f)

        assert config["security_scanning"]["bandit"]["enabled"]
        assert config["security_monitoring"]["enabled"]

    @pytest.mark.performance
    def test_security_performance_integration(
        self, security_test_suite, vulnerability_scanner
    ):
        """Test security components performance."""
        import time

        # Test security test suite performance
        start_time = time.time()
        security_results = security_test_suite.run_security_scan()
        security_time = time.time() - start_time

        # Test vulnerability scanner performance
        start_time = time.time()
        vuln_results = vulnerability_scanner.scan_all()
        vuln_time = time.time() - start_time

        # Verify performance is within acceptable limits
        assert (
            security_time < 60
        ), f"Security test suite took too long: {security_time}s"
        assert vuln_time < 120, f"Vulnerability scanner took too long: {vuln_time}s"

        # Verify results are still valid
        assert security_results["vulnerability_scan"]["success"]
        assert vuln_results["security_metrics"] is not None


# Pytest fixtures for security testing
@pytest.fixture(scope="session")
def security_test_environment():
    """Set up security test environment."""
    # Set up test environment
    import os

    os.environ["SECURITY_TEST_MODE"] = "true"
    yield
    # Cleanup
    if "SECURITY_TEST_MODE" in os.environ:
        del os.environ["SECURITY_TEST_MODE"]


@pytest.mark.security
class TestSecurityMarkers:
    """Test security-specific pytest markers."""

    def test_security_marker_works(self):
        """Test that security marker works."""
        assert True

    @pytest.mark.slow
    def test_slow_security_test(self):
        """Test slow security test marker."""
        time.sleep(0.1)  # Simulate slow test
        assert True


# Integration test for CI/CD security pipeline
def test_security_cicd_integration():
    """Test security integration with CI/CD pipeline."""
    # This test simulates what would happen in CI/CD
    security_suite = SecurityTestSuite()

    # Run security scan
    results = security_suite.run_security_scan()

    # Verify all security checks pass
    assert results["vulnerability_scan"][
        "success"
    ], "Vulnerability scan failed in CI/CD"
    assert results["dependency_check"]["success"], "Dependency check failed in CI/CD"
    assert results["code_security"][
        "overall_success"
    ], "Code security checks failed in CI/CD"
    assert results["configuration_security"][
        "success"
    ], "Configuration security failed in CI/CD"
    assert results["data_security"]["success"], "Data security checks failed in CI/CD"

    # Verify compliance
    compliance_results = results["compliance_check"]
    assert compliance_results["owasp_top_10"][
        "compliant"
    ], "OWASP compliance failed in CI/CD"
    assert compliance_results["cve_compliance"][
        "compliant"
    ], "CVE compliance failed in CI/CD"
    assert compliance_results["license_compliance"][
        "compliant"
    ], "License compliance failed in CI/CD"


if __name__ == "__main__":
    # Run integration tests
    pytest.main([__file__, "-v", "--tb=short"])
